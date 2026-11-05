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
	"runtime"
	"sync"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/asynkron/protoactor-go/actor"
)

func drainR4(c chan interface{}) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}

func newR4ProductionSecondGrid(t *testing.T) *GridTaskMaster {
	// Reuse the explicit config/pool fixture, then invoke the production
	// constructor with TimeTypeSecondDelay so the real handler is installed.
	fixture := newRetirementGrid(t, 0)
	info := fixture.GetJobInstanceInfo()
	info.SetTimeType(int32(common.TimeTypeSecondDelay))
	info.SetTimeExpression("3600")
	return NewGridTaskMaster(info, retirementActorContext{system: actor.NewActorSystem()})
}

type heldR4SendContext struct {
	retirementActorContext
	entered, release chan struct{}
	once             sync.Once
	sent             chan *schedulerx.MasterKillContainerRequest
}

func (c *heldR4SendContext) Send(_ *actor.PID, msg interface{}) {
	c.once.Do(func() { close(c.entered); <-c.release })
	c.sent <- msg.(*schedulerx.MasterKillContainerRequest)
}

// A Send already admitted before replacement is excluded. The second worker
// request is constructed and first submitted only AFTER replacement is visible.
func TestLifecycleKillDoesNotSendAfterReplacement(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	ctx := &heldR4SendContext{retirementActorContext: retirementActorContext{system: actor.NewActorSystem()}, entered: make(chan struct{}), release: make(chan struct{}), sent: make(chan *schedulerx.MasterKillContainerRequest, 4)}
	old.actorCtx = ctx
	old.jobInstanceInfo.SetAllWorkers([]string{"first@127.0.0.1:1234", "second@127.0.0.1:1235"})
	done := make(chan struct{})
	go func() { defer close(done); old.KillInstance("killed from server") }()
	waitRetirementTest(t, ctx.entered)
	current := newRetirementGrid(t, 0)
	replaced := make(chan struct{})
	go func() { defer close(replaced); pool.Put(1001, current) }()
	// The first non-waiting handoff is joined; retirement intent rejects later sends.
	deadline := time.After(time.Second)
	for !old.retirementStarted() {
		select {
		case <-deadline:
			t.Fatal("retirement not started")
		default:
			runtime.Gosched()
		}
	}
	close(ctx.release)
	waitRetirementTest(t, replaced)
	defer pool.Remove(1001)
	waitRetirementTest(t, done)
	<-ctx.sent // first request was already admitted at the held boundary.
	select {
	case req := <-ctx.sent:
		t.Fatalf("old owner submitted NEW second-worker kill after replacement: attempt=%d serial=%d", req.GetAttempt(), req.GetSerialNum())
	default:
	}
}

func TestLifecycleSecondTerminalDoesNotEnqueueAfterReplacement(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	channel := actorcomm.AtLeastOnceDeliveryMsgReceiver()
	drainR4(channel)
	defer drainR4(channel)
	for i := 0; i < cap(channel); i++ {
		channel <- struct{}{}
	}
	old := newR4ProductionSecondGrid(t)
	old.jobInstanceInfo.SetAllWorkers(nil)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	done := make(chan struct{})
	go func() { defer close(done); old.KillInstance("killed, because of worker missed active server.") }()
	r4WaitStack(t, "(*MapTaskMaster).sendCycleMessage")
	current := newRetirementGrid(t, 0)
	pool.Put(1001, current)
	defer pool.Remove(1001)
	<-channel
	waitRetirementTest(t, done)
	for len(channel) > 0 {
		msg := <-channel
		if wrapped, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
			if req, ok := wrapped.Msg.(*schedulerx.WorkerReportJobInstanceStatusRequest); ok {
				t.Fatalf("old terminal report enqueued after replacement was published: status=%d instance=%d", req.GetStatus(), req.GetJobInstanceId())
			}
		}
	}
}

func TestLifecycleOrdinaryCurrentTerminalStillReports(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	defer pool.Remove(1001)
	channel := actorcomm.AtLeastOnceDeliveryMsgReceiver()
	drainR4(channel)
	defer drainR4(channel)
	m.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, "")
	select {
	case msg := <-channel:
		if _, ok := msg.(*actorcomm.SchedulerWrappedMsg).Msg.(*schedulerx.WorkerReportJobInstanceStatusRequest); !ok {
			t.Fatal("not terminal")
		}
	default:
		t.Fatal("normal terminal lost")
	}
	if pool.Get(1001) != nil {
		t.Fatal("normal terminal did not remove owner")
	}
}

func TestLifecycleSecondProgressReportsBetweenCycles(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	utils.GetHealthTimeHolder().ResetServerHeartbeatTime()
	m := newR4ProductionSecondGrid(t)
	// Preserve a healthy known worker list while waiting for the next cycle.
	m.jobInstanceInfo.SetAllWorkers([]string{"worker@127.0.0.1:1234"})
	h := m.statusHandler.(*SecondJobUpdateInstanceStatusHandler)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	defer pool.Remove(1001)
	h.triggerNextCycle("1_1001_0", 0, processor.InstanceStatusSucceed)
	defer GetTimeScheduler().remove(1001)
	channel := actorcomm.TaskMasterMsgReceiver()
	drainR4(channel)
	defer drainR4(channel)
	defer h.stop()
	select {
	case msg := <-channel:
		if _, ok := msg.(*actorcomm.SchedulerWrappedMsg).Msg.(*schedulerx.WorkerReportJobInstanceProgressRequest); !ok {
			t.Fatal("wrong report")
		}
	case <-time.After(12 * time.Second):
		t.Fatal("current second-job owner suppressed aggregate progress for entire inter-cycle delay")
	}
}
