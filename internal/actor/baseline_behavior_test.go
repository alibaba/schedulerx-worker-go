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

package actor

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

type baselineProcessorFunc func(*jobcontext.JobContext) (*processor.ProcessResult, error)

func (f baselineProcessorFunc) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	return f(ctx)
}

// Only old SDK interfaces are used here: this file is also executed unchanged
// against the original source archive, to distinguish regressions from defects
// already present before the concurrency changes.
func waitBaselineTerminal(t *testing.T, h *pushHarness, task int64, want int32) *schedulerx.ContainerReportTaskStatusRequest {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, ok := batch.GetContainerStatusReqHandlerPool().GetHandlers().Load(int64(1001))
		if ok {
			req, ok := raw.(*batch.ContainerStatusReqHandler).GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
			if ok && req.GetTaskId() == task && req.GetStatus() == want && !h.worker.containerPool.Contain("1_1001_"+strconv.FormatInt(task, 10)) {
				return req
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("task %d did not finish with terminal status %d", task, want)
	return nil
}

func TestBaselineProcessorBehaviorAcrossModes(t *testing.T) {
	for _, mode := range []string{"standalone", "broadcast", "sharding", "parallel", "grid", "batch"} {
		for _, outcome := range []string{"success", "error", "nil", "panic", "missing", "java", "retry", "root-no-retry"} {
			t.Run(mode+"/"+outcome, func(t *testing.T) {
				h := newPushHarness(t)
				var calls atomic.Int32
				tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
					calls.Add(1)
					switch outcome {
					case "error":
						return nil, errors.New(strings.Repeat("x", constants.InstanceResultSizeMax+10))
					case "nil":
						return nil, nil
					case "panic":
						panic("baseline-process-panic")
					case "retry", "root-no-retry":
						if ctx.TaskAttempt() < 2 {
							return processor.NewProcessResult(processor.WithFailed(), processor.WithResult("retry")), nil
						}
					}
					return processor.NewProcessResult(processor.WithSucceed(), processor.WithResult("done")), nil
				}))
				req := h.request(1, mode, proto.Int32(1))
				req.SerialNum = proto.Int64(7)
				req.TaskName = proto.String("application-task")
				req.Attempt = proto.Int32(2)
				want := int32(processor.InstanceStatusSucceed)
				switch outcome {
				case "error", "nil", "panic":
					want = int32(processor.InstanceStatusFailed)
				case "missing":
					req.Content = proto.String(`{"jobName":"no-such-baseline-processor"}`)
					want = int32(processor.InstanceStatusFailed)
				case "java":
					req.JobType = proto.String("java")
					req.Content = proto.String(`{"className":"` + t.Name() + `"}`)
				case "retry":
					req.MaxAttempt = proto.Int32(2)
				case "root-no-retry":
					req.TaskId = proto.Int64(0)
					req.MaxAttempt = proto.Int32(2)
					want = int32(processor.InstanceStatusFailed)
				}
				if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
					t.Fatal("valid baseline request rejected")
				}
				result := waitBaselineTerminal(t, h, req.GetTaskId(), want)
				if result.GetJobId() != 1 || result.GetJobInstanceId() != 1001 || result.GetSerialNum() != 7 || result.GetTaskName() != "application-task" || result.GetInstanceMasterActorPath() != req.GetInstanceMasterAkkaPath() {
					t.Fatalf("status metadata changed: %v", result)
				}
				wantCalls := int32(1)
				if outcome == "missing" {
					wantCalls = 0
				}
				if outcome == "retry" {
					wantCalls = 3
				}
				if calls.Load() != wantCalls {
					t.Fatalf("want %d Process calls, got %d", wantCalls, calls.Load())
				}
				switch outcome {
				case "success", "java", "retry":
					if result.GetResult() != "done" {
						t.Fatal(result)
					}
				case "nil":
					if result.GetResult() != "result can't be null" {
						t.Fatal(result)
					}
				case "error":
					if len(result.GetResult()) != constants.InstanceResultSizeMax {
						t.Fatal("error truncation changed")
					}
				case "panic":
					if !strings.Contains(result.GetResult(), "baseline-process-panic") {
						t.Fatal(result)
					}
				case "missing":
					if !strings.Contains(result.GetResult(), "not found") {
						t.Fatal(result)
					}
				}
			})
		}
	}
}

func TestBaselineRequestContextRoundtrip(t *testing.T) {
	h := newPushHarness(t)
	got := make(chan *jobcontext.JobContext, 1)
	tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
		got <- ctx
		return processor.NewProcessResult(processor.WithSucceed()), nil
	}))
	req := h.request(1, "sharding", nil)
	req.SerialNum = proto.Int64(9)
	req.Attempt = proto.Int32(3)
	req.Parameters = proto.String("job-params")
	req.InstanceParameters = proto.String("instance-params")
	req.User = proto.String("user")
	req.GroupId = proto.String("group")
	req.TaskName = proto.String("task")
	req.Task = []byte(`{"id":4,"parameter":"value"}`)
	req.WfInstanceId = proto.Int64(99)
	req.ShardingNum = proto.Int32(7)
	req.ScheduleTime = proto.Int64(123456)
	req.DataTime = proto.Int64(123000)
	req.TriggerType = proto.Int32(2)
	req.TimeType = proto.Int32(3)
	req.TimeExpression = proto.String("cron")
	req.UpstreamData = []*schedulerx.UpstreamData{{JobName: proto.String("upstream"), Data: proto.String("data")}}
	if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("rejected")
	}
	waitBaselineTerminal(t, h, 1, int32(processor.InstanceStatusSucceed))
	select {
	case ctx := <-got:
		if ctx.JobId() != 1 || ctx.JobInstanceId() != 1001 || ctx.TaskId() != 1 || ctx.Attempt() != 3 || ctx.SerialNum() != 9 || ctx.ExecuteMode() != "sharding" || ctx.JobParameters() != "job-params" || ctx.InstanceParameters() != "instance-params" || ctx.User() != "user" || ctx.GroupId() != "group" || ctx.WfInstanceId() != 99 || ctx.ShardingNum() != 7 || ctx.ShardingId() != 4 || ctx.ShardingParameter() != "value" || ctx.ScheduleTime().UnixMilli() != 123456 || ctx.DataTime().UnixMilli() != 123000 || ctx.TriggerType() != 2 || ctx.TimeType() != 3 || ctx.TimeExpression() != "cron" || len(ctx.UpstreamData()) != 1 || string(ctx.Task()) != string(req.Task) {
			t.Fatalf("request context changed: %+v", ctx)
		}
	case <-time.After(time.Second):
		t.Fatal("no real processor context")
	}
}

type baselineStatusObservation struct {
	serial int64
	status processor.InstanceStatus
	result string
}
type baselineStatusHandler struct {
	observations chan baselineStatusObservation
}

func (h *baselineStatusHandler) Handle(serial int64, status processor.InstanceStatus, result string) error {
	h.observations <- baselineStatusObservation{serial, status, result}
	return nil
}

// Bridge the SDK's ordinary at-least-once output into the real task Actor and
// common Master status updater. No external SchedulerX server is emulated.
func TestBaselineBatchedStatusReachesTaskMaster(t *testing.T) {
	for _, mode := range []string{"standalone", "broadcast", "sharding", "parallel", "grid", "batch"} {
		t.Run(mode, func(t *testing.T) {
			h := newPushHarness(t)
			tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(*jobcontext.JobContext) (*processor.ProcessResult, error) {
				return processor.NewProcessResult(processor.WithSucceed(), processor.WithResult(mode)), nil
			}))
			observations := make(chan baselineStatusObservation, 8)
			receiver := &taskActor{taskMasterPool: masterpool.GetTaskMasterPool()}
			pid := h.system.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
				if _, ok := ctx.Message().(string); ok {
					info := new(common.JobInstanceInfo)
					info.SetJobId(1)
					info.SetJobInstanceId(1001)
					tm := master.NewTaskMaster(ctx, info, &baselineStatusHandler{observations})
					masterpool.GetTaskMasterPool().Put(1001, tm)
					ctx.Respond(true)
					return
				}
				receiver.Receive(ctx)
			}))
			defer func() { masterpool.GetTaskMasterPool().Remove(1001); _ = h.system.Root.StopFuture(pid).Wait() }()
			if _, err := h.system.Root.RequestFuture(pid, "init", time.Second).Result(); err != nil {
				t.Fatal(err)
			}
			req := h.request(1, mode, proto.Int32(1))
			req.SerialNum = proto.Int64(123456789)
			req.TaskName = proto.String(mode)
			if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
				t.Fatal("rejected")
			}
			timeout := time.NewTimer(3 * time.Second)
			defer timeout.Stop()
			for {
				select {
				case raw := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
					wrapped, ok := raw.(*actorcomm.SchedulerWrappedMsg)
					if !ok {
						continue
					}
					wire, ok := wrapped.Msg.(*schedulerx.ContainerBatchReportTaskStatuesRequest)
					if !ok || wire.GetSerialNum() != 123456789 {
						continue
					}
					final := false
					for _, s := range wire.GetTaskStatues() {
						if s.GetTaskName() == mode && s.GetResult() == mode && s.GetStatus() == int32(processor.InstanceStatusSucceed) {
							final = true
						}
					}
					if !final {
						continue
					}
					if wire.GetJobId() != 1 || wire.GetJobInstanceId() != 1001 || wire.GetWorkerAddr() != "127.0.0.1:12345" {
						t.Fatalf("batch metadata changed: %v", wire)
					}
					ack, err := h.system.Root.RequestFuture(pid, wire, time.Second).Result()
					if err != nil {
						t.Fatal(err)
					}
					if !ack.(*schedulerx.ContainerBatchReportTaskStatuesResponse).GetSuccess() {
						t.Fatal("Master rejected status batch")
					}
					for {
						select {
						case got := <-observations:
							if got.status == processor.InstanceStatusSucceed {
								if got.serial != 123456789 || got.result != mode {
									t.Fatalf("Master status changed: %+v", got)
								}
								return
							}
						case <-timeout.C:
							t.Fatal("terminal status not applied by real Master")
						}
					}
				case <-timeout.C:
					t.Fatal("no terminal status emitted to ordinary transport")
				}
			}
		})
	}
}
