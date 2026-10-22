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

package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

type blockingProcessor struct {
	started chan int64
	release chan struct{}
}

func (p *blockingProcessor) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	p.started <- ctx.TaskId()
	<-p.release
	return processor.NewProcessResult(processor.WithSucceed()), nil
}

type pushHarness struct {
	t         *testing.T
	processor *blockingProcessor
	system    *actor.ActorSystem
	worker    *containerActor
	pid       *actor.PID
	once      sync.Once
}

func newPushHarness(t *testing.T) *pushHarness {
	config.InitWorkerConfig(config.NewWorkerConfig())
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	h := &pushHarness{t: t, processor: &blockingProcessor{started: make(chan int64, 64), release: make(chan struct{})}}
	tasks.GetTaskMap().Register(t.Name(), h.processor)
	h.system = actor.NewActorSystem()
	h.system.ProcessRegistry.Address = "127.0.0.1:12345"
	h.worker = newContainerActor()
	h.pid = h.system.Root.Spawn(actor.PropsFromProducer(func() actor.Actor { return h.worker }))
	t.Cleanup(func() {
		h.release()
		deadline := time.Now().Add(3 * time.Second)
		for {
			pending := false
			h.worker.containerPool.GetContainerMap().Range(func(_, _ any) bool { pending = true; return false })
			if !pending {
				break
			}
			if time.Now().After(deadline) {
				t.Error("containers did not finish during cleanup")
				break
			}
			time.Sleep(time.Millisecond)
		}
		h.worker.containerPool.DestroyByInstance(1001)
		batch.GetContainerStatusReqHandlerPool().Stop(1001)
		_ = h.system.Root.StopFuture(h.pid).Wait()
		h.system.Shutdown()
	})
	return h
}

func (h *pushHarness) release() { h.once.Do(func() { close(h.processor.release) }) }

func (h *pushHarness) request(id int64, mode string, consumers *int32) *schedulerx.MasterStartContainerRequest {
	return &schedulerx.MasterStartContainerRequest{
		JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(id),
		ExecuteMode: proto.String(mode), JobType: proto.String("golang"),
		Content:     proto.String(`{"jobName":"` + h.t.Name() + `"}`),
		ConsumerNum: consumers, InstanceMasterAkkaPath: proto.String("worker@127.0.0.1:12345"),
	}
}

func (h *pushHarness) send(request any) any {
	h.t.Helper()
	response, err := h.system.Root.RequestFuture(h.pid, request, time.Second).Result()
	if err != nil {
		h.t.Fatalf("actor must respond while consumers are occupied: %v", err)
	}
	return response
}

func (h *pushHarness) push(id int64, mode string, consumers *int32) {
	h.t.Helper()
	response := h.send(h.request(id, mode, consumers)).(*schedulerx.MasterStartContainerResponse)
	if !response.GetSuccess() {
		h.t.Fatalf("push task %d rejected: %v", id, response)
	}
}

func (h *pushHarness) started() int64 {
	h.t.Helper()
	select {
	case id := <-h.processor.started:
		return id
	case <-time.After(3 * time.Second):
		h.t.Fatal("configured consumer did not execute a task")
		return 0
	}
}

func (h *pushHarness) assertNoStart() {
	h.t.Helper()
	select {
	case id := <-h.processor.started:
		h.t.Fatalf("task %d unexpectedly entered Process", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPushConsumerSizeLimitsExecutionWithoutBlockingActor(t *testing.T) {
	h := newPushHarness(t)
	for id := int64(1); id <= 8; id++ {
		h.push(id, "parallel", proto.Int32(2))
	}
	h.started()
	h.started()
	h.assertNoStart()
	h.release()
	for i := 0; i < 6; i++ {
		h.started()
	}
}

func TestBatchPushUsesConsumerSize(t *testing.T) {
	h := newPushHarness(t)
	request := &schedulerx.MasterBatchStartContainersRequest{}
	for id := int64(1); id <= 8; id++ {
		request.StartReqs = append(request.StartReqs, h.request(id, "batch", proto.Int32(2)))
	}
	if response := h.send(request).(*schedulerx.MasterBatchStartContainersResponse); !response.GetSuccess() {
		t.Fatalf("batch rejected: %v", response)
	}
	h.started()
	h.started()
	h.assertNoStart()
	h.release()
	for i := 0; i < 6; i++ {
		h.started()
	}
}

func TestMissingOrInvalidConsumerSizeUsesDefault(t *testing.T) {
	for _, tc := range []struct {
		mode      string
		consumers *int32
	}{
		{"parallel", nil}, {"grid", proto.Int32(0)}, {"batch", proto.Int32(-1)},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			h := newPushHarness(t)
			for id := int64(1); id <= 6; id++ {
				h.push(id, tc.mode, tc.consumers)
			}
			for i := 0; i < 5; i++ {
				h.started()
			}
			h.assertNoStart()
			h.release()
			h.started()
		})
	}
}

func TestNonMapModesKeepTheirExecutionBehavior(t *testing.T) {
	for _, mode := range []string{"standalone", "broadcast", "sharding"} {
		t.Run(mode, func(t *testing.T) {
			h := newPushHarness(t)
			for id := int64(1); id <= 3; id++ {
				h.push(id, mode, proto.Int32(1))
			}
			for i := 0; i < 3; i++ {
				h.started()
			}
		})
	}
}

func TestKillQueuedPushTaskDoesNotExecuteIt(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.push(3, "parallel", proto.Int32(1))
	response := h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(2)}).(*schedulerx.MasterKillContainerResponse)
	if !response.GetSuccess() {
		t.Fatalf("kill rejected: %v", response)
	}
	h.release()
	if id := h.started(); id != 3 {
		t.Fatalf("killed queued task ran: %d", id)
	}
	h.assertNoStart()
}

func TestDestroyQueuedPushTasksDoesNotExecuteThem(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.push(3, "parallel", proto.Int32(1))
	response := h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001)}).(*schedulerx.MasterDestroyContainerPoolResponse)
	if !response.GetSuccess() {
		t.Fatalf("destroy rejected: %v", response)
	}
	h.release()
	h.assertNoStart()
}

func TestKillInstanceDiscardsQueuedPushTasks(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.push(3, "parallel", proto.Int32(1))
	response := h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(0)}).(*schedulerx.MasterKillContainerResponse)
	if !response.GetSuccess() {
		t.Fatalf("kill rejected: %v", response)
	}
	h.release()
	h.assertNoStart()
}
