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
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type r3PauseLogger struct {
	heldProgressLogger
	prefix string
	once   sync.Once
}

func (l *r3PauseLogger) Infof(f string, _ ...interface{}) {
	if strings.HasPrefix(f, l.prefix) {
		l.once.Do(func() { close(l.entered); <-l.release })
	}
}
func TestDelayedGridMapCannotConsumeNextCycleQuota(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	l := &r3PauseLogger{heldProgressLogger: heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}, prefix: "map taskList,"}
	logger.SetLogger(l)
	oldCtx := new(jobcontext.JobContext)
	oldCtx.SetSerialNum(0)
	oldCtx.SetAttempt(0)
	done := make(chan struct{})
	go func() { defer close(done); m.Map(oldCtx, [][]byte{[]byte("old")}, "sub") }()
	waitRetirementTest(t, l.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	close(l.release)
	waitRetirementTest(t, done)
	if got := m.taskCounter.Load(); got != 0 {
		t.Fatalf("old serial-0 Map consumed current serial-1 quota: %d", got)
	}
}
func TestRetiredProgressCannotEnqueueAfterReplacement(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	old.jobInstanceInfo.SetAllWorkers(nil)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	l := &r3PauseLogger{heldProgressLogger: heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}, prefix: "MapTaskMaster reportJobInstanceProgress,"}
	logger.SetLogger(l)
	done := make(chan struct{})
	go func() { defer close(done); old.reportJobInstanceProgress(old.cycleCtx) }()
	waitRetirementTest(t, l.entered)
	current := newRetirementGrid(t, 0)
	pool.Put(1001, current)
	defer pool.Remove(1001)
	defer current.Stop()
	// RetireStatusWrites does not cancel loops, but Stop also cancels; either must suppress stale send.
	old.Stop()
	close(l.release)

	waitRetirementTest(t, done)
	for {
		select {
		case msg := <-actorcomm.TaskMasterMsgReceiver():
			if w, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
				if report, ok := w.Msg.(*schedulerx.WorkerReportJobInstanceProgressRequest); ok && report.GetJobInstanceId() == 1001 {
					t.Fatal("retired owner enqueued stale progress")
				}
			}
		default:
			return
		}
	}
}

func TestRetryProgressCannotCrossCycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	grid := newRetirementGrid(t, 0)
	grid.jobInstanceInfo.SetAllWorkers(nil)
	m := &ParallelTaskMaster{MapTaskMaster: grid.MapTaskMaster}
	first := common.NewTaskProgressCounter("first")
	first.IncrementOneSuccess()
	m.taskProgressMap.Store("first", first)
	const n = 2000000
	entities := make([]*schedulerx.RetryTaskEntity, n)
	middle := &schedulerx.RetryTaskEntity{TaskId: proto.Int64(1), TaskName: proto.String("unused"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed))}
	for i := range entities {
		entities[i] = middle
	}
	entities[0] = &schedulerx.RetryTaskEntity{TaskId: proto.Int64(2), TaskName: proto.String("first"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed))}
	entities[n-1] = &schedulerx.RetryTaskEntity{TaskId: proto.Int64(3), TaskName: proto.String("last"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed))}
	done := make(chan struct{})
	go func() { defer close(done); m.RetryTasks(entities) }()
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()
	for {
		select {
		case msg := <-actorcomm.TaskMasterMsgReceiver():
			if w, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
				if r, ok := w.Msg.(*schedulerx.WorkerReportTaskListStatusRequest); ok && r.GetJobInstanceId() == 1001 {
					goto sent
				}
			}
		case <-timer.C:
			t.Fatal("no retry request")
		}
	}
sent:
	actorcomm.WorkerReportTaskListStatusRespMsgSender() <- &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(true)}
	for first.GetSuccess() != 0 {
		select {
		case <-timer.C:
			t.Fatal("no first progress update")
		default:
			runtime.Gosched()
		}
	}
	m.Clear(m)
	m.AcquireSerialNum()
	m.startBatchHandler()
	defer m.Stop()
	current := common.NewTaskProgressCounter("last")
	current.IncrementOneSuccess()
	m.taskProgressMap.Store("last", current)
	waitRetirementTest(t, done)
	if got := current.GetSuccess(); got != 1 {
		t.Fatalf("old retry changed new serial-1 success counter to %d", got)
	}
}
