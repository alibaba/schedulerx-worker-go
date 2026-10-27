/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package master

import (
	"testing"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
)

type attemptStatusRecorder struct{ calls int }

func (r *attemptStatusRecorder) Handle(_ int64, _ processor.InstanceStatus, _ string) error {
	r.calls++
	return nil
}

// Only the persistence boundary is replaced; production status aggregation and
// progress mutations are exercised by the tests below.
type attemptPersistenceRecorder struct {
	persistence.TaskPersistence
	received []*schedulerx.ContainerReportTaskStatusRequest
}

func (r *attemptPersistenceRecorder) UpdateTaskStatues(reqs []*schedulerx.ContainerReportTaskStatusRequest) error {
	r.received = append(r.received, reqs...)
	return nil
}

func attemptTestMaster(attempt int32) (*TaskMaster, *attemptStatusRecorder) {
	info := new(common.JobInstanceInfo)
	info.SetJobId(1)
	info.SetJobInstanceId(1001)
	info.SetAttempt(attempt)
	recorder := new(attemptStatusRecorder)
	return &TaskMaster{jobInstanceInfo: info, statusHandler: recorder, serialNum: atomic.NewInt64(0)}, recorder
}
func attemptTestRequest(attempt *int32) *schedulerx.ContainerReportTaskStatusRequest {
	return &schedulerx.ContainerReportTaskStatusRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(0), Status: proto.Int32(int32(taskstatus.TaskStatusSucceed)), SerialNum: proto.Int64(0), Attempt: attempt, Result: proto.String("terminal")}
}

func TestTaskStatusAttemptIsolation(t *testing.T) {
	for _, mode := range []string{"standalone", "broadcast", "parallel", "grid", "batch", "sharding"} {
		t.Run(mode, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				current  int32
				reported *int32
				accept   bool
			}{
				{"old", 1, proto.Int32(0), false}, {"future", 1, proto.Int32(2), false}, {"current", 1, proto.Int32(1), true}, {"explicit-zero", 0, proto.Int32(0), true}, {"future-from-zero", 0, proto.Int32(1), false}, {"legacy", 1, nil, true},
			} {
				t.Run(test.name, func(t *testing.T) {
					base, recorder := attemptTestMaster(test.current)
					base.taskStatusMap.Store(utils.GetUniqueId(1, 1001, 0), taskstatus.TaskStatusRunning)
					queue := batch.NewReqQueue(8)
					var target taskmaster.TaskMaster
					switch mode {
					case "standalone":
						target = &StandaloneTaskMaster{TaskMaster: base}
					case "broadcast":
						m := &BroadcastTaskMaster{TaskMaster: base}
						m.taskStatusMap.Store(utils.GetUniqueId(1, 1001, 0), taskstatus.TaskStatusRunning)
						target = m
					default:
						m := &MapTaskMaster{TaskMaster: base, taskStatusReqQueue: queue}
						switch mode {
						case "parallel":
							target = &ParallelTaskMaster{MapTaskMaster: m}
						case "grid":
							target = &GridTaskMaster{MapTaskMaster: m}
						case "batch":
							target = &BatchTaskMaster{GridTaskMaster: &GridTaskMaster{MapTaskMaster: m}}
						case "sharding":
							target = &ShardingTaskMaster{GridTaskMaster: &GridTaskMaster{MapTaskMaster: m}}
						}
					}
					_ = target.UpdateTaskStatus(attemptTestRequest(test.reported))
					if mode == "standalone" || mode == "broadcast" {
						if (recorder.calls > 0) != test.accept {
							t.Fatalf("accept=%t, status handler calls=%d", test.accept, recorder.calls)
						}
						if !test.accept && base.IsJobInstanceFinished() {
							t.Fatal("rejected status finished current task map")
						}
					} else if (queue.Size() > 0) != test.accept {
						t.Fatalf("accept=%t, queued=%d", test.accept, queue.Size())
					}
				})
			}
		})
	}
}

func TestBatchTaskStatusAttemptValidationAndPropagation(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt *int32
		accept  bool
	}{
		{"old-zero", proto.Int32(0), false}, {"future", proto.Int32(2), false}, {"current", proto.Int32(1), true}, {"legacy", nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, _ := attemptTestMaster(1)
			queue := batch.NewReqQueue(8)
			target := &MapTaskMaster{TaskMaster: base, taskStatusReqQueue: queue}
			packet := &schedulerx.ContainerBatchReportTaskStatuesRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), Attempt: test.attempt, TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(int32(taskstatus.TaskStatusSucceed))}}}
			_ = target.BatchUpdateTaskStatus(target, packet)
			got := queue.RetrieveRequests(8)
			if (len(got) > 0) != test.accept {
				t.Fatalf("accept=%t, queued=%d", test.accept, len(got))
			}
			if len(got) > 0 {
				req := got[0].(*schedulerx.ContainerReportTaskStatusRequest)
				if (req.Attempt == nil) != (test.attempt == nil) || req.GetAttempt() != packet.GetAttempt() {
					t.Fatalf("batch lost attempt presence/value: %v", req)
				}
			}
		})
	}
	// Explicit zero is a real generation and must survive the batch split.
	base, _ := attemptTestMaster(0)
	queue := batch.NewReqQueue(8)
	target := &MapTaskMaster{TaskMaster: base, taskStatusReqQueue: queue}
	_ = target.BatchUpdateTaskStatus(target, &schedulerx.ContainerBatchReportTaskStatuesRequest{Attempt: proto.Int32(0), TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(4)}}})
	if reqs := queue.RetrieveRequests(8); len(reqs) != 1 || reqs[0].(*schedulerx.ContainerReportTaskStatusRequest).Attempt == nil {
		t.Fatal("explicit zero became legacy missing attempt")
	}
}

func TestDelayedMapStatusFlushCannotMutateReplacementAttempt(t *testing.T) {
	for _, sharding := range []bool{false, true} {
		t.Run(map[bool]string{false: "map", true: "sharding"}[sharding], func(t *testing.T) {
			masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
			base, _ := attemptTestMaster(1)
			sink := new(attemptPersistenceRecorder)
			m := &MapTaskMaster{TaskMaster: base, taskPersistence: sink}
			var target taskmaster.TaskMaster = m
			var shard *ShardingTaskMaster
			if sharding {
				shard = &ShardingTaskMaster{GridTaskMaster: &GridTaskMaster{MapTaskMaster: m}}
				target = shard
			}
			masterpool.GetTaskMasterPool().Put(1001, target)
			defer masterpool.GetTaskMasterPool().Remove(1001)
			queue := batch.NewReqQueue(8)
			handler := batch.NewTMStatusReqHandler(1001, 1, 1, 8, queue)
			queue.SubmitRequest(attemptTestRequest(proto.Int32(0)))
			handler.AsyncHandleReqs(handler) // The old handler resolves the replacement Master at flush time.
			if len(sink.received) != 0 {
				t.Fatal("old queued status reached replacement persistence")
			}
			if _, ok := m.taskStatusMap.Load(int64(0)); ok {
				t.Fatal("old queued status changed replacement task map")
			}
			if _, ok := m.taskProgressMap.Load(""); ok {
				t.Fatal("old queued status changed progress counters")
			}
			if shard != nil {
				if _, ok := shard.shardingTaskStatusMap.Load(int64(0)); ok {
					t.Fatal("old queued status changed sharding status")
				}
			}
			queue.SubmitRequest(attemptTestRequest(proto.Int32(1)))
			handler.AsyncHandleReqs(handler)
			if len(sink.received) != 1 || sink.received[0].GetAttempt() != 1 {
				t.Fatal("current status was not persisted")
			}
		})
	}
}

func TestMapBatchIgnoresOldRootFailure(t *testing.T) {
	base, _ := attemptTestMaster(1)
	sink := new(attemptPersistenceRecorder)
	m := &MapTaskMaster{TaskMaster: base, taskPersistence: sink}
	current := attemptTestRequest(proto.Int32(1))
	current.TaskId = proto.Int64(1)
	old := attemptTestRequest(proto.Int32(0))
	old.TaskName = proto.String(constants.MapTaskRootName)
	old.Status = proto.Int32(int32(taskstatus.TaskStatusFailed))
	old.Result = proto.String("old root failure")
	m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{current, old})
	if m.rootTaskResult != "" {
		t.Fatal("old root failure replaced current root result")
	}
	if len(sink.received) != 1 || sink.received[0].GetTaskId() != 1 {
		t.Fatal("mixed batch persisted old status")
	}
}
