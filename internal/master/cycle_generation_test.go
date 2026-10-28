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
	"context"
	"github.com/alibaba/schedulerx-worker-go/config"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
)

func TestDispatchFailureCannotFinishReplacementCycle(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	db := m.taskPersistence
	defer db.ClearTasks(1001)
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(0), TaskName: proto.String("sub"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	m.batchUpdateTaskStatus("w", "127.0.0.1:1234", []*schedulerx.MasterStartContainerRequest{req})
	// Real status handler dequeues now, but is descheduled before Process.
	queued := m.taskStatusReqQueue.RetrieveRequests(8)
	if len(queued) != 1 {
		t.Fatalf("queued=%d", len(queued))
	}
	old := queued[0].(*schedulerx.ContainerReportTaskStatusRequest)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	if err := db.CreateTask(1, 1001, 0, "replacement cycle", nil); err != nil {
		t.Fatal(err)
	}
	m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{old})
	if got := db.CheckInstanceStatus(1001); got != processor.InstanceStatusRunning {
		t.Fatalf("old dispatch failure deleted new-cycle row; synthesized serial pointer=%v status=%v", old.SerialNum, got)
	}
}

type reviewBlockedCheck struct {
	persistence.TaskPersistence
	entered, release chan struct{}
}

func (p *reviewBlockedCheck) CheckInstanceStatus(id int64) processor.InstanceStatus {
	close(p.entered)
	<-p.release
	return processor.InstanceStatusSucceed
}

type reviewSerialRecorder struct{ serial chan int64 }

func (r *reviewSerialRecorder) Handle(serial int64, _ processor.InstanceStatus, _ string) error {
	r.serial <- serial
	return nil
}
func TestDelayedStatusQueryCannotFinishReplacementCycle(t *testing.T) {
	config.InitWorkerConfig(config.NewWorkerConfig(config.WithMapMasterStatusCheckInterval(time.Millisecond)))
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	p := &reviewBlockedCheck{TaskPersistence: m.taskPersistence, entered: make(chan struct{}), release: make(chan struct{})}
	m.taskPersistence = p
	r := &reviewSerialRecorder{serial: make(chan int64, 1)}
	m.statusHandler = r
	ctx, cancel := context.WithCancel(context.Background())
	m.cycleCancel = cancel
	done := make(chan struct{})
	go func() { defer close(done); m.checkInstanceStatus(ctx) }()
	waitRetirementTest(t, p.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	// Cancellation is already true; resume only the old persistence-query completion.
	close(p.release)
	waitRetirementTest(t, done)
	select {
	case s := <-r.serial:
		t.Fatalf("old-cycle query called status handler using serial %d after restart to serial %d", s, m.GetSerialNum())
	default:
	}
}

func TestLocalMapContextCannotCrossSerial(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	oldCtx := new(jobcontext.JobContext)
	oldCtx.SetJobId(1)
	oldCtx.SetJobInstanceId(1001)
	oldCtx.SetAttempt(0)
	oldCtx.SetSerialNum(0)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	if _, err := m.Map(oldCtx, [][]byte{[]byte("old callback")}, "old subtask"); err != nil {
		t.Fatal(err)
	}
	pending := m.taskBlockingQueue.RetrieveRequests(1)
	if len(pending) == 0 {
		return
	}
	req := pending[0].(*schedulerx.MasterStartContainerRequest)
	m.BatchPullTasks([]*schedulerx.MasterStartContainerRequest{req}, "w@127.0.0.1:1234")
	n, err := m.taskPersistence.UpdateTaskStatus(1001, []int64{req.GetTaskId()}, taskstatus.TaskStatusRunning, "w", "127.0.0.1:1234")
	defer m.taskPersistence.ClearTasks(1001)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("old local job context serial=%d rebound to current serial=%d and inserted %d task rows", oldCtx.SerialNum(), req.GetSerialNum(), n)
	}
}

func TestWrappedBatchStatusesRespectRetirement(t *testing.T) {
	for _, mode := range []string{"sharding", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			base, r := attemptTestMaster(0)
			m := &MapTaskMaster{TaskMaster: base, taskStatusReqQueue: batch.NewReqQueue(4)}
			var wrapper taskmaster.TaskMaster
			if mode == "sharding" {
				wrapper = &ShardingTaskMaster{GridTaskMaster: &GridTaskMaster{MapTaskMaster: m}}
			} else {
				wrapper = &ParallelTaskMaster{MapTaskMaster: m}
			}
			m.RetireStatusWrites()
			req := &schedulerx.ContainerBatchReportTaskStatuesRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), Attempt: proto.Int32(0), SerialNum: proto.Int64(0), TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(int32(taskstatus.TaskStatusSucceed))}}}
			if err := wrapper.BatchUpdateTaskStatus(wrapper, req); err != nil {
				t.Fatal(err)
			}
			if r.calls != 0 {
				t.Fatalf("retired %s accepted actor batch and called status handler %d times", mode, r.calls)
			}
		})
	}
}
