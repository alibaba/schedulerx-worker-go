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
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
)

func remoteGateMaster(t *testing.T) *ParallelTaskMaster {
	t.Helper()
	base, _ := attemptTestMaster(0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &ParallelTaskMaster{MapTaskMaster: &MapTaskMaster{
		TaskMaster: base, taskPersistence: &persistence.ServerTaskPersistence{},
		retireCh: make(chan struct{}), cycleCtx: ctx, cycleCancel: cancel,
		pageSize: 1, taskBlockingQueue: batch.NewReqQueue(4),
	}}
}

func TestRemotePullWaitAllowsReplacementAndRejectsLateTasks(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	for _, transition := range []string{"replace owner", "advance serial"} {
		t.Run(transition, func(t *testing.T) {
			m := remoteGateMaster(t)
			counter := common.NewTaskProgressCounter("sub")
			counter.IncrementRunning()
			m.taskProgressMap.Store("sub", counter)
			pool := masterpool.NewTaskMasterPool(tasks.GetTaskMap())
			pool.Put(1001, m)
			replacement := remoteGateMaster(t)
			done := make(chan struct{})
			go func() { defer close(done); m.pullTask(m.cycleCtx, "1_1001") }()
			var sent *actorcomm.SchedulerWrappedMsg
			select {
			case msg := <-actorcomm.TaskMasterMsgReceiver():
				sent = msg.(*actorcomm.SchedulerWrappedMsg)
				if _, ok := sent.Msg.(*schedulerx.WorkerPullTasksRequest); !ok {
					t.Fatalf("unexpected request: %T", msg)
				}
			case <-time.After(time.Second):
				t.Fatal("remote Pull request was not enqueued")
			}
			replied := false
			reply := func() {
				if !replied {
					response := &schedulerx.WorkerPullTasksResponse{
						Success: proto.Bool(true), TaskMessage: []*schedulerx.TaskMessage{{TaskId: proto.Int64(7), TaskName: proto.String("sub"), TaskBody: []byte(`{}`)}},
					}
					if sent.Reply != nil {
						sent.Reply <- response
					} else {
						actorcomm.WorkerPullTasksRespMsgSender() <- response
					}
					replied = true
				}
			}
			defer func() { reply(); waitRetirementTest(t, done) }()
			changed := make(chan struct{})
			go func() {
				defer close(changed)
				if transition == "replace owner" {
					pool.Put(1001, replacement)
				} else {
					m.AcquireSerialNum()
				}
			}()
			defer waitRetirementTest(t, changed)
			select {
			case <-changed:
			case <-time.After(time.Second):
				reply()
				t.Fatal("lifecycle transition waited for remote Pull response")
			}
			if transition == "replace owner" && pool.Get(1001) != replacement {
				t.Fatal("replacement owner was not published")
			}
			reply()
			waitRetirementTest(t, done)
			if counter.GetRunning() != 1 || len(m.taskBlockingQueue.RetrieveRequests(4)) != 0 {
				t.Fatal("late Pull response changed progress or dispatched an old task")
			}
		})
	}
}

func TestRemotePersistenceBackpressureDoesNotPinLifecycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	for _, operation := range []string{"update", "failover", "pull", "batch update"} {
		for _, transition := range []string{"retire", "advance"} {
			t.Run(operation+"/"+transition, func(t *testing.T) {
				m := remoteGateMaster(t)
				target := actorcomm.AtLeastOnceDeliveryMsgReceiver()
				method := "(*ServerTaskPersistence).UpdateTaskStatus"
				if operation == "pull" || operation == "batch update" {
					target = actorcomm.TaskMasterMsgReceiver()
					method = "(*ServerTaskPersistence).Pull"
					if operation == "batch update" {
						method = "(*ServerTaskPersistence).BatchUpdateTaskStatus"
					}
				}
				for len(target) < cap(target) {
					target <- "full"
				}
				defer func() {
					for len(target) > 0 {
						<-target
					}
				}()
				done := make(chan struct{})
				var admitted bool
				go func() {
					defer close(done)
					switch operation {
					case "update":
						_, admitted, _ = m.updatePersistedTasks(0, 1001, []int64{7}, taskstatus.TaskStatusInit, "worker", "addr")
					case "failover":
						admitted = m.batchHandlePersistence("worker", "addr", []*schedulerx.MasterStartContainerRequest{{JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(7), SerialNum: proto.Int64(0)}}, true)
					case "pull":
						_, admitted, _ = m.pullPersistedTasks(0, 1001, 1)
					case "batch update":
						_, admitted = m.batchUpdatePersistedTasks(0, 1001, taskstatus.TaskStatusInit, "worker", "addr")
					}
				}()
				r4WaitStack(t, method)
				changed := make(chan struct{})
				go func() {
					defer close(changed)
					if transition == "retire" {
						m.RetireStatusWrites()
					} else {
						m.AcquireSerialNum()
					}
				}()
				select {
				case <-changed:
				case <-time.After(time.Second):
					// Release the old implementation for deterministic cleanup.
					<-target
					if operation == "pull" {
						actorcomm.WorkerPullTasksRespMsgSender() <- &schedulerx.WorkerPullTasksResponse{Success: proto.Bool(true)}
					} else if operation == "batch update" {
						actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender() <- &schedulerx.WorkerBatchUpdateTaskStatusResponse{Success: proto.Bool(true)}
					}
					waitRetirementTest(t, done)
					waitRetirementTest(t, changed)
					t.Fatal("full remote queue pinned lifecycle transition")
				}
				waitRetirementTest(t, done)
				if admitted {
					t.Fatal("canceled remote operation was admitted")
				}
				for len(target) > 0 {
					if msg := <-target; msg != "full" {
						t.Fatal("inactive cycle enqueued a remote request after backpressure")
					}
				}
			})
		}
	}
}

func TestRemotePullRepliesStayWithTheirOwner(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	for _, mode := range []string{"replacement owner", "next serial on same owner"} {
		t.Run(mode, func(t *testing.T) {
			type result struct {
				tasks    []*common.TaskInfo
				admitted bool
				err      error
			}
			start := func(m *ParallelTaskMaster) (<-chan result, *actorcomm.SchedulerWrappedMsg) {
				out := make(chan result, 1)
				serial := m.GetSerialNum()
				go func() {
					tasks, admitted, err := m.pullPersistedTasks(serial, 1001, 1)
					out <- result{tasks, admitted, err}
				}()
				select {
				case msg := <-actorcomm.TaskMasterMsgReceiver():
					return out, msg.(*actorcomm.SchedulerWrappedMsg)
				case <-time.After(time.Second):
					t.Fatal("remote Pull was not enqueued")
					return nil, nil
				}
			}
			old := remoteGateMaster(t)
			oldDone, oldReq := start(old)
			if oldReq.Reply == nil {
				actorcomm.WorkerPullTasksRespMsgSender() <- &schedulerx.WorkerPullTasksResponse{Success: proto.Bool(true)}
				<-oldDone
				t.Fatal("remote Pull has no request-scoped response route")
			}
			current := old
			if mode == "replacement owner" {
				old.RetireStatusWrites()
				current = remoteGateMaster(t)
			} else {
				old.AcquireSerialNum()
			}
			currentDone, currentReq := start(current)
			if currentReq.Reply == nil || oldReq.Reply == currentReq.Reply {
				t.Fatal("replacement Pull reused the old owner's response route")
			}
			oldReq.Reply <- &schedulerx.WorkerPullTasksResponse{Success: proto.Bool(true), TaskMessage: []*schedulerx.TaskMessage{{TaskId: proto.Int64(7)}}}
			select {
			case got := <-oldDone:
				if got.admitted || len(got.tasks) != 0 {
					t.Fatal("old owner accepted a late task list")
				}
			case <-time.After(time.Second):
				t.Fatal("old owner did not consume its own response")
			}
			select {
			case <-currentDone:
				t.Fatal("replacement completed before receiving its own response")
			case <-time.After(30 * time.Millisecond):
			}
			currentReq.Reply <- &schedulerx.WorkerPullTasksResponse{Success: proto.Bool(true), TaskMessage: []*schedulerx.TaskMessage{{TaskId: proto.Int64(42)}}}
			select {
			case got := <-currentDone:
				if !got.admitted || got.err != nil || len(got.tasks) != 1 || got.tasks[0].TaskId() != 42 {
					t.Fatalf("replacement did not receive its own task list: %+v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("replacement response was lost")
			}
		})
	}
}

func TestRemoteCanceledHandoffIsNotAdmittedBeforeCycleCloses(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	m := remoteGateMaster(t)
	target := actorcomm.AtLeastOnceDeliveryMsgReceiver()
	for len(target) < cap(target) {
		target <- "full"
	}
	defer func() {
		for len(target) > 0 {
			<-target
		}
	}()
	// Clear cancels before it can join the state writer. The owner can still
	// pass the state check here, but a blocked handoff must report rejection.
	m.cycleCancel()
	if !m.cycleActive(0) {
		t.Fatal("test did not reach the canceled-before-close window")
	}
	_, admitted, _ := m.updatePersistedTasks(0, 1001, []int64{7}, taskstatus.TaskStatusInit, "worker", "addr")
	if admitted {
		t.Fatal("canceled remote handoff was reported as admitted")
	}
	if len(target) != cap(target) {
		t.Fatal("canceled handoff changed the full queue")
	}
}
