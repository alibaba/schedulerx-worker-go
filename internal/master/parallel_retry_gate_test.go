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
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
)

// A delayed Retry response must not pin the lifecycle writer. Once that writer
// retires the owner or advances the cycle, the response must not change counters.
func TestParallelRetryResponseWaitDoesNotBlockLifecycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	for _, transition := range []string{"retire owner", "advance cycle"} {
		t.Run(transition, func(t *testing.T) {
			base, _ := attemptTestMaster(0)
			base.inited = true
			m := &ParallelTaskMaster{MapTaskMaster: &MapTaskMaster{
				TaskMaster: base,
				retireCh:   make(chan struct{}),
			}}
			counter := common.NewTaskProgressCounter("sub")
			counter.IncrementOneSuccess()
			m.taskProgressMap.Store("sub", counter)
			worker := common.NewWorkerProgressCounter("worker")
			worker.IncrementSuccess()
			m.workerProgressMap.Store("worker", worker)

			done := make(chan struct{})
			go func() {
				defer close(done)
				m.RetryTasks([]*schedulerx.RetryTaskEntity{{
					TaskId: proto.Int64(7), TaskName: proto.String("sub"),
					WorkerAddr: proto.String("worker"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed)),
				}})
			}()
			// Release the real response channel even if the old implementation
			// blocks the lifecycle assertion, so failure doesn't leak goroutines.
			replied := false
			defer func() {
				if !replied {
					actorcomm.WorkerReportTaskListStatusRespMsgSender() <- &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(true)}
				}
				waitRetirementTest(t, done)
			}()
			select {
			case msg := <-actorcomm.TaskMasterMsgReceiver():
				req := msg.(*actorcomm.SchedulerWrappedMsg).Msg.(*schedulerx.WorkerReportTaskListStatusRequest)
				if req.GetJobInstanceId() != 1001 || len(req.TaskId) != 1 || req.TaskId[0] != 7 || req.GetStatus() != int32(taskstatus.TaskStatusInit) {
					t.Fatalf("unexpected retry reset: %v", req)
				}
			case <-time.After(time.Second):
				t.Fatal("retry reset was not sent")
			}

			transitioned := make(chan struct{})
			go func() {
				defer close(transitioned)
				if transition == "retire owner" {
					m.RetireStatusWrites()
				} else {
					m.AcquireSerialNum()
				}
			}()
			defer waitRetirementTest(t, transitioned)
			select {
			case <-transitioned:
			case <-time.After(time.Second):
				// The cleanup must send the response before joining this writer.
				actorcomm.WorkerReportTaskListStatusRespMsgSender() <- &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(true)}
				replied = true
				t.Fatal("lifecycle transition blocked on a remote Retry response")
			}
			actorcomm.WorkerReportTaskListStatusRespMsgSender() <- &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(true)}
			replied = true
			waitRetirementTest(t, done)
			if counter.GetSuccess() != 1 || worker.GetSuccess() != 1 {
				t.Fatalf("late retry changed inactive cycle counters: task=%d worker=%d", counter.GetSuccess(), worker.GetSuccess())
			}
		})
	}
}
