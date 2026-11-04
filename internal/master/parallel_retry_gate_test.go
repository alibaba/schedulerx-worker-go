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
			var sent *actorcomm.SchedulerWrappedMsg
			// Release the request's response even if the old implementation
			// blocks the lifecycle assertion, so failure doesn't leak goroutines.
			replied := false
			defer func() {
				if !replied {
					replyRetryForTest(sent, true)
				}
				waitRetirementTest(t, done)
			}()
			select {
			case msg := <-actorcomm.TaskMasterMsgReceiver():
				sent = msg.(*actorcomm.SchedulerWrappedMsg)
				req := sent.Msg.(*schedulerx.WorkerReportTaskListStatusRequest)
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
				replyRetryForTest(sent, true)
				replied = true
				t.Fatal("lifecycle transition blocked on a remote Retry response")
			}
			replyRetryForTest(sent, true)
			replied = true
			waitRetirementTest(t, done)
			if counter.GetSuccess() != 1 || worker.GetSuccess() != 1 {
				t.Fatalf("late retry changed inactive cycle counters: task=%d worker=%d", counter.GetSuccess(), worker.GetSuccess())
			}
		})
	}
}

// Replying to an old successful reset must never authorize a new failed reset.
// Cover both response orders and reuse of a master across serial changes.
func TestParallelRetryRepliesStayWithTheirRequest(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	for _, transition := range []string{"replacement owner", "next serial", "same cycle"} {
		for _, order := range []string{"old first", "current first"} {
			t.Run(transition+"/"+order, func(t *testing.T) {
				newMaster := func() (*ParallelTaskMaster, *common.TaskProgressCounter, *common.WorkerProgressCounter) {
					base, _ := attemptTestMaster(0)
					base.inited = true
					m := &ParallelTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: base, retireCh: make(chan struct{})}}
					counter := common.NewTaskProgressCounter("sub")
					counter.IncrementOneSuccess()
					m.taskProgressMap.Store("sub", counter)
					worker := common.NewWorkerProgressCounter("worker")
					worker.IncrementSuccess()
					m.workerProgressMap.Store("worker", worker)
					return m, counter, worker
				}
				start := func(m *ParallelTaskMaster, taskID int64) (*actorcomm.SchedulerWrappedMsg, <-chan struct{}) {
					done := make(chan struct{})
					go func() {
						defer close(done)
						m.RetryTasks([]*schedulerx.RetryTaskEntity{{TaskId: proto.Int64(taskID), TaskName: proto.String("sub"), WorkerAddr: proto.String("worker"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed))}})
					}()
					select {
					case msg := <-actorcomm.TaskMasterMsgReceiver():
						return msg.(*actorcomm.SchedulerWrappedMsg), done
					case <-time.After(time.Second):
						t.Fatal("retry request was not sent")
						return nil, done
					}
				}
				old, oldTask, oldWorker := newMaster()
				oldReq, oldDone := start(old, 7)
				oldReplied := false
				defer func() {
					if !oldReplied {
						replyRetryForTest(oldReq, true)
					}
					waitRetirementTest(t, oldDone)
				}()
				current, currentTask, currentWorker := old, oldTask, oldWorker
				switch transition {
				case "replacement owner":
					old.RetireStatusWrites()
					current, currentTask, currentWorker = newMaster()
				case "next serial":
					old.AcquireSerialNum()
				}
				currentReq, currentDone := start(current, 42)
				currentReplied := false
				defer func() {
					if !currentReplied {
						replyRetryForTest(currentReq, false)
					}
					if !oldReplied {
						replyRetryForTest(oldReq, true)
						oldReplied = true
					}
					waitRetirementTest(t, currentDone)
				}()
				if oldReq.Reply == nil || currentReq.Reply == nil || oldReq.Reply == currentReq.Reply {
					t.Fatal("overlapping retry requests do not have independent replies")
				}
				if order == "old first" {
					replyRetryForTest(oldReq, true)
					oldReplied = true
					waitRetirementTest(t, oldDone)
					select {
					case <-currentDone:
						t.Fatal("current retry consumed an old reply")
					default:
					}
					replyRetryForTest(currentReq, false)
					currentReplied = true
				} else {
					replyRetryForTest(currentReq, false)
					currentReplied = true
					waitRetirementTest(t, currentDone)
					select {
					case <-oldDone:
						t.Fatal("old retry consumed a current reply")
					default:
					}
					replyRetryForTest(oldReq, true)
					oldReplied = true
				}
				waitRetirementTest(t, oldDone)
				waitRetirementTest(t, currentDone)
				want := int64(1)
				if transition == "same cycle" {
					want = 0
				}
				if currentTask.GetSuccess() != want || currentWorker.GetSuccess() != want {
					t.Fatalf("retry response changed wrong counters: task=%d worker=%d want=%d", currentTask.GetSuccess(), currentWorker.GetSuccess(), want)
				}
				if transition == "replacement owner" && (oldTask.GetSuccess() != 1 || oldWorker.GetSuccess() != 1) {
					t.Fatal("retired retry applied progress")
				}
			})
		}
	}
}

// A legacy response already in the shared channel belongs to another request.
func TestParallelRetryIgnoresUnrelatedLegacyReply(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	base, _ := attemptTestMaster(0)
	base.inited = true
	m := &ParallelTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: base}}
	counter := common.NewTaskProgressCounter("sub")
	counter.IncrementOneSuccess()
	m.taskProgressMap.Store("sub", counter)
	legacy := actorcomm.WorkerReportTaskListStatusRespMsgSender()
	legacyResponse := &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(true)}
	legacy <- legacyResponse
	defer func() {
		select {
		case <-legacy:
		default:
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RetryTasks([]*schedulerx.RetryTaskEntity{{TaskId: proto.Int64(42), TaskName: proto.String("sub"), OldStatus: proto.Int32(int32(taskstatus.TaskStatusSucceed))}})
	}()
	var req *actorcomm.SchedulerWrappedMsg
	select {
	case msg := <-actorcomm.TaskMasterMsgReceiver():
		req = msg.(*actorcomm.SchedulerWrappedMsg)
	case <-time.After(time.Second):
		t.Fatal("retry request was not sent")
	}
	replyRetryForTest(req, false)
	waitRetirementTest(t, done)
	if counter.GetSuccess() != 1 {
		t.Fatal("unrelated legacy success authorized a failed retry")
	}
	select {
	case got := <-legacy:
		if got != legacyResponse {
			t.Fatal("legacy reply was replaced")
		}
	default:
		t.Fatal("scoped retry consumed a legacy reply")
	}
}

func replyRetryForTest(req *actorcomm.SchedulerWrappedMsg, success bool) {
	response := &schedulerx.WorkerReportTaskListStatusResponse{Success: proto.Bool(success)}
	if req != nil && req.Reply != nil {
		req.Reply <- response
	} else {
		actorcomm.WorkerReportTaskListStatusRespMsgSender() <- response
	}
}
