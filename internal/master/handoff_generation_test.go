/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package master

import (
	"fmt"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"testing"
	"time"
)

type r3Logger struct {
	heldProgressLogger
	target string
	hold   sync.Once
}

func (l *r3Logger) maybe(f string) {
	if f == l.target {
		l.hold.Do(func() { close(l.entered); <-l.release })
	}
}
func (l *r3Logger) Infof(f string, _ ...interface{})  { l.maybe(f) }
func (l *r3Logger) Errorf(f string, _ ...interface{}) { l.maybe(f) }
func r3Hold(target string) *r3Logger {
	l := &r3Logger{heldProgressLogger: heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}, target: target}
	logger.SetLogger(l)
	return l
}

type r3TimeoutContext struct{ retirementActorContext }

func (c r3TimeoutContext) RequestFuture(_ *actor.PID, _ interface{}, _ time.Duration) *actor.Future {
	return actor.NewFuture(c.system, time.Millisecond)
}

type r3UpdateError struct{ persistence.TaskPersistence }

func (p *r3UpdateError) UpdateTaskStatus(int64, []int64, taskstatus.TaskStatus, string, string) (int64, error) {
	return 0, fmt.Errorf("forced update failure")
}

func TestDelayedTimeoutFailureCannotFailNextCycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers([]string{"worker@127.0.0.1:1234", "other@127.0.0.1:1235"})
	m.actorCtx = r3TimeoutContext{retirementActorContext{system: actor.NewActorSystem()}}
	m.taskPersistence = &r3UpdateError{m.taskPersistence}
	sink := &reviewSerialRecorder{serial: make(chan int64, 2)}
	m.statusHandler = sink
	held := r3Hold("jobInstanceId=%d, timeout return init error")
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(7), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.BatchDispatchTasks([]*schedulerx.MasterStartContainerRequest{req}, "worker@127.0.0.1:1234")
	}()
	waitRetirementTest(t, held.entered)
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.Clear(m)
	m.AcquireSerialNum()
	m.startBatchHandler()
	defer m.Stop()
	close(held.release)
	waitRetirementTest(t, done)
	select {
	case serial := <-sink.serial:
		t.Fatalf("old timeout failure reached new-cycle status handler: serial=%d", serial)
	default:
	}
}

func TestDelayedFinalizerCannotDestroyReplacement(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	defer pool.Remove(1001)
	held := r3Hold("report jobInstance=%d, status=%d to AtLeastDeliveryRoutingActor")
	done := make(chan struct{})
	go func() { defer close(done); old.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, "") }()
	waitRetirementTest(t, held.entered)
	current := newRetirementGrid(t, 0)
	pool.Put(1001, current)
	// Drain only messages handed off before replacement. The finding concerns new enqueue after retirement.
drain:
	for {
		select {
		case <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
		default:
			break drain
		}
	}
	close(held.release)
	waitRetirementTest(t, done)
	for {
		select {
		case msg := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
			if w, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
				if req, ok := w.Msg.(*schedulerx.MasterDestroyContainerPoolRequest); ok {
					t.Fatalf("retired finalizer newly enqueued Destroy: instance=%d attempt=%d serial=%d", req.GetJobInstanceId(), req.GetAttempt(), req.GetSerialNum())
				}
			}
		default:
			return
		}
	}
}

func TestDelayedWrapperMapCannotChangeNextCycleQuota(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	held := r3Hold("map taskList, jobInstanceId=%v, taskName:%v, taskList size:%v")
	ctx := new(jobcontext.JobContext)
	ctx.SetAttempt(0)
	ctx.SetSerialNum(0)
	done := make(chan struct{})
	go func() { defer close(done); m.Map(ctx, [][]byte{[]byte("old")}, "old") }()
	waitRetirementTest(t, held.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	m.startBatchHandler()
	defer m.Stop()
	close(held.release)
	waitRetirementTest(t, done)
	if n := m.taskCounter.Load(); n != 0 {
		t.Fatalf("old cycle Map changed new cycle admission counter: %d", n)
	}
}

func isolateHandoffProbe(t *testing.T) bool {
	t.Helper()
	if os.Getenv("SCHEDULERX_HANDOFF_PROBE") == t.Name() {
		return false
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "SCHEDULERX_HANDOFF_PROBE="+t.Name())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated handoff probe: %v\n%s", err, out)
	}
	return true
}
