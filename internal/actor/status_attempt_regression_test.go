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
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

// Replay a real old worker wire batch into the real standalone Master updater.
// Transport delay is controlled here; no external SchedulerX server is simulated.
func TestDelayedOldAttemptTerminalCannotFinishReplacement(t *testing.T) {
	h := newPushHarness(t)
	tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
		if ctx.Attempt() == 0 {
			return processor.NewProcessResult(processor.WithSucceed(), processor.WithResult("old-attempt-terminal")), nil
		}
		h.processor.Process(ctx)
		return processor.NewProcessResult(processor.WithSucceed(), processor.WithResult("current-attempt-terminal")), nil
	}))
	old := h.request(0, "standalone", nil)
	old.Attempt = proto.Int32(0)
	old.SerialNum = proto.Int64(0)
	if !h.send(old).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("old attempt rejected")
	}
	var held []byte
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
capture:
	for {
		select {
		case raw := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
			wrap, ok := raw.(*actorcomm.SchedulerWrappedMsg)
			if !ok {
				continue
			}
			packet, ok := wrap.Msg.(*schedulerx.ContainerBatchReportTaskStatuesRequest)
			if !ok || packet.GetJobInstanceId() != 1001 {
				continue
			}
			for _, s := range packet.GetTaskStatues() {
				if s.GetTaskId() == 0 && s.GetResult() == "old-attempt-terminal" {
					var err error
					held, err = proto.Marshal(packet)
					if err != nil {
						t.Fatal(err)
					}
					break capture
				}
			}
		case <-deadline.C:
			t.Fatal("old terminal did not reach wire handoff")
		}
	}
	current := h.request(0, "standalone", nil)
	current.Attempt = proto.Int32(1)
	current.SerialNum = proto.Int64(0)
	if !h.send(current).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("new attempt rejected")
	}
	h.started() // Attempt 1 Process is now blocked and has not completed.
	destroyRequests := make(chan *schedulerx.MasterDestroyContainerPoolRequest, 1)
	// Observe and acknowledge the actual cleanup request without modifying user work.
	// The default status handler and its outbound instance-status report are real.
	router, err := h.system.Root.SpawnNamed(actor.PropsFromFunc(func(ctx actor.Context) {
		if req, ok := ctx.Message().(*schedulerx.MasterDestroyContainerPoolRequest); ok {
			destroyRequests <- req
			ctx.Respond(&schedulerx.MasterDestroyContainerPoolResponse{Success: proto.Bool(true)})
		}
	}), actorcomm.ContainerRouterPidId)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.system.Root.StopFuture(router).Wait() }()
	receiver := &taskActor{taskMasterPool: masterpool.GetTaskMasterPool()}
	pid := h.system.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {
		if _, ok := ctx.Message().(string); ok {
			info := new(common.JobInstanceInfo)
			info.SetJobId(1)
			info.SetJobInstanceId(1001)
			info.SetAttempt(1)
			tm := master.NewStandaloneTaskMaster(info, ctx).(*master.StandaloneTaskMaster)
			masterpool.GetTaskMasterPool().Put(1001, tm)
			ctx.Respond(tm)
			return
		}
		receiver.Receive(ctx)
	}))
	defer func() { masterpool.GetTaskMasterPool().Remove(1001); _ = h.system.Root.StopFuture(pid).Wait() }()
	installed, err := h.system.Root.RequestFuture(pid, "install attempt 1 master", time.Second).Result()
	if err != nil {
		t.Fatal(err)
	}
	tm := installed.(*master.StandaloneTaskMaster)
	running := &schedulerx.ContainerBatchReportTaskStatuesRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(0), Attempt: proto.Int32(1), TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(int32(processor.InstanceStatusRunning))}}}
	if _, err := h.system.Root.RequestFuture(pid, running, time.Second).Result(); err != nil {
		t.Fatal(err)
	}
	if tm.GetInstanceStatus() != processor.InstanceStatusRunning {
		t.Fatal("new task not running")
	}
	delayed := new(schedulerx.ContainerBatchReportTaskStatuesRequest)
	if err := proto.Unmarshal(held, delayed); err != nil {
		t.Fatal(err)
	}
	if delayed.Attempt == nil || delayed.GetAttempt() != 0 {
		t.Fatal("old worker batch lost explicit Attempt zero")
	}
	if _, err := h.system.Root.RequestFuture(pid, delayed, time.Second).Result(); err != nil {
		t.Fatal(err)
	}
	if tm.GetInstanceStatus() != processor.InstanceStatusRunning {
		t.Fatal("old terminal changed current Attempt instance status")
	}
	if masterpool.GetTaskMasterPool().Get(1001) != tm {
		t.Fatal("old terminal removed current Attempt Master")
	}
	select {
	case <-destroyRequests:
		t.Fatal("old terminal requested current Attempt cleanup")
	default:
	}
	select {
	case <-h.processor.release:
		t.Fatal("current user Process unexpectedly released")
	default:
	}
	// The new Attempt must still complete normally through the same real handler.
	h.release()
	deadlineReport := time.NewTimer(3 * time.Second)
	defer deadlineReport.Stop()
	for {
		select {
		case raw := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
			wrap, ok := raw.(*actorcomm.SchedulerWrappedMsg)
			if !ok {
				continue
			}
			if packet, ok := wrap.Msg.(*schedulerx.ContainerBatchReportTaskStatuesRequest); ok && packet.GetJobInstanceId() == 1001 {
				if _, err := h.system.Root.RequestFuture(pid, packet, time.Second).Result(); err != nil {
					t.Fatal(err)
				}
			}
			report, ok := wrap.Msg.(*schedulerx.WorkerReportJobInstanceStatusRequest)
			if !ok || report.GetJobInstanceId() != 1001 {
				continue
			}
			if report.GetResult() == "old-attempt-terminal" {
				t.Fatal("old result escaped as instance terminal status")
			}
			if report.GetStatus() == int32(processor.InstanceStatusSucceed) && report.GetResult() == "current-attempt-terminal" {
				if tm.GetInstanceStatus() != processor.InstanceStatusSucceed {
					t.Fatal("current completion was not applied")
				}
				return
			}
		case <-deadlineReport.C:
			t.Fatal("current Attempt completion did not reach instance reporting")
		}
	}
}
