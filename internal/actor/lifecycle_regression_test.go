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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

func TestLifecycleLatePushAfterDestroyMustNotReopenOldCycle(t *testing.T) {
	h := newPushHarness(t)
	first := h.request(1, "parallel", proto.Int32(1))
	first.SerialNum = proto.Int64(7)
	if !h.send(first).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("first push failed")
	}
	h.started()
	response := h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7)}).(*schedulerx.MasterDestroyContainerPoolResponse)
	if !response.GetSuccess() {
		t.Fatal("destroy failed")
	}
	late := h.request(2, "parallel", proto.Int32(1))
	late.SerialNum = proto.Int64(7)
	h.send(late)
	h.assertNoStart()
}
func TestLifecycleDuplicatePushAndKill(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.push(2, "parallel", proto.Int32(1))
	h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(2)})
	h.release()
	h.assertNoStart()
}

type lifecycleKillProcessor struct {
	*blockingProcessor
	killed chan int64
}

func (p *lifecycleKillProcessor) Kill(ctx *jobcontext.JobContext) error {
	p.killed <- ctx.JobInstanceId()
	return nil
}
func TestLifecycleKillInstanceDoesNotMatchLongerInstanceID(t *testing.T) {
	h := newPushHarness(t)
	p := &lifecycleKillProcessor{blockingProcessor: h.processor, killed: make(chan int64, 4)}
	tasks.GetTaskMap().Register(t.Name(), p)
	t.Cleanup(func() {
		h.release()
		h.worker.containerPool.DestroyByInstance(10010)
		batch.GetContainerStatusReqHandlerPool().Stop(10010)
	})
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	other := h.request(1, "parallel", proto.Int32(1))
	other.JobInstanceId = proto.Int64(10010)
	if !h.send(other).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("second instance rejected")
	}
	h.started()
	h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(0)})
	for {
		select {
		case instance := <-p.killed:
			if instance != 1001 {
				t.Fatalf("kill of 1001 called Kill() for unrelated instance %d", instance)
			}
		default:
			return
		}
	}
}

func TestLifecycleNewCycleIgnoresOldDestroyAndPush(t *testing.T) {
	h := newPushHarness(t)
	first := h.request(1, "parallel", proto.Int32(2))
	first.SerialNum = proto.Int64(7)
	h.send(first)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7)})
	next := h.request(1, "parallel", proto.Int32(2))
	next.SerialNum = proto.Int64(8)
	if !h.send(next).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("new cycle rejected")
	}
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7)})
	late := h.request(99, "parallel", proto.Int32(2))
	late.SerialNum = proto.Int64(7)
	h.send(late)
	second := h.request(2, "parallel", proto.Int32(2))
	second.SerialNum = proto.Int64(8)
	h.send(second)
	h.assertNoStart()
	h.release()
	if id := h.started(); id != 2 {
		t.Fatalf("old cycle affected current cycle: task %d", id)
	}
	h.assertNoStart()
}

func TestLifecycleFutureDestroyFencesDelayedBatch(t *testing.T) {
	h := newPushHarness(t)
	first := h.request(1, "parallel", proto.Int32(1))
	first.SerialNum = proto.Int64(7)
	h.send(first)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(8)})
	late := h.request(2, "parallel", proto.Int32(1))
	late.SerialNum = proto.Int64(8)
	h.send(late)
	h.assertNoStart()
}

func TestLifecycleDuplicateBatchCanBeCanceled(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	batchRequest := &schedulerx.MasterBatchStartContainersRequest{}
	for i := 0; i < 100; i++ {
		batchRequest.StartReqs = append(batchRequest.StartReqs, h.request(2, "parallel", proto.Int32(1)))
	}
	batchRequest.StartReqs = append(batchRequest.StartReqs, h.request(3, "parallel", proto.Int32(1)))
	if !h.send(batchRequest).(*schedulerx.MasterBatchStartContainersResponse).GetSuccess() {
		t.Fatal("batch rejected")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !h.worker.containerPool.Contain("1_1001_3") {
		if time.Now().After(deadline) {
			t.Fatal("batch not fully submitted")
		}
		time.Sleep(time.Millisecond)
	}
	h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(2)})
	h.release()
	if id := h.started(); id != 3 {
		t.Fatalf("canceled batch copy entered Process: %d", id)
	}
	h.assertNoStart()
}

func TestLifecycleWrongJobCannotDestroyInstance(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	response := h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(2), JobInstanceId: proto.Int64(1001)}).(*schedulerx.MasterDestroyContainerPoolResponse)
	if response.GetSuccess() {
		t.Fatal("another job was allowed to destroy this instance")
	}
	h.push(2, "parallel", proto.Int32(1))
	h.assertNoStart()
	h.release()
	if id := h.started(); id != 2 {
		t.Fatalf("original instance lost task %d", id)
	}
}

func TestLifecycleAdmissionHistoryIsBoundedAndExpires(t *testing.T) {
	h := newPushHarness(t)
	h.worker.admissionMu.Lock()
	for id := int64(1); id <= maxInstanceAdmissions; id++ {
		if err := h.worker.closeInstance(id, 1, 7); err != nil {
			h.worker.admissionMu.Unlock()
			t.Fatal(err)
		}
	}
	h.worker.admissionMu.Unlock()
	overflow := h.request(1, "parallel", proto.Int32(1))
	overflow.JobInstanceId = proto.Int64(maxInstanceAdmissions + 1)
	if h.send(overflow).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("history overflow silently evicted live close fences")
	}
	h.worker.admissionMu.Lock()
	h.worker.pruneInstanceAdmissions(time.Now().Add(2 * closedInstanceRetention))
	remaining, closed := len(h.worker.instances), h.worker.admissionExpirations.Len()
	h.worker.admissionMu.Unlock()
	if remaining != 0 || closed != 0 {
		t.Fatalf("expired history retained %d instances / %d closed entries", remaining, closed)
	}
	if !h.send(overflow).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("admission did not recover after history expiry")
	}
	h.started()
	h.release()
	h.worker.containerPool.DestroyByInstance(maxInstanceAdmissions + 1)
	batch.GetContainerStatusReqHandlerPool().Stop(maxInstanceAdmissions + 1)
}

func TestLifecycleHigherAttemptCanReopenClosedInstance(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001)})
	retry := h.request(1, "parallel", proto.Int32(1))
	retry.Attempt = proto.Int32(1)
	if !h.send(retry).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("explicit instance retry rejected")
	}
	h.assertNoStart()
	h.release()
	h.started()
}

type goexitProcessor struct{ *blockingProcessor }

func (p *goexitProcessor) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	if ctx.TaskId() == 1 {
		p.started <- 1
		runtime.Goexit()
	}
	return p.blockingProcessor.Process(ctx)
}
func TestLifecycleRealProcessorGoexitReportsFailureAndContinues(t *testing.T) {
	h := newPushHarness(t)
	tasks.GetTaskMap().Register(t.Name(), &goexitProcessor{h.processor})
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	deadline := time.Now().Add(time.Second)
	for {
		handler, ok := h.worker.statusReqBatchHandlerPool.GetHandlers().Load(int64(1001))
		if ok {
			status, ok := handler.(*batch.ContainerStatusReqHandler).GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
			if ok && status.GetTaskId() == 1 && status.GetStatus() == int32(processor.InstanceStatusFailed) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("Goexit left the real task in RUNNING instead of reporting failure")
		}
		time.Sleep(time.Millisecond)
	}
	h.push(2, "parallel", proto.Int32(1))
	if id := h.started(); id != 2 {
		t.Fatalf("no replacement consumer: task %d", id)
	}
}

func TestLifecycleLegacyStandaloneDestroyWithoutJobID(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "standalone", proto.Int32(1))
	h.started()
	response := h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(0)}).(*schedulerx.MasterDestroyContainerPoolResponse)
	if !response.GetSuccess() {
		t.Fatal("legacy standalone destroy omitted JobId and was rejected")
	}
	if h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("standalone status handler survived destroy")
	}
}

func TestLifecycleOldAttemptCannotDestroyOrKillRetry(t *testing.T) {
	h := newPushHarness(t)
	first := h.request(1, "parallel", proto.Int32(2))
	first.SerialNum = proto.Int64(7)
	h.send(first)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7)})
	retry := h.request(1, "parallel", proto.Int32(2))
	retry.Attempt = proto.Int32(1)
	h.send(retry)
	h.started()
	destroy := &schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7)}
	body, _ := proto.Marshal(destroy)
	body = protowire.AppendVarint(protowire.AppendTag(body, 7, protowire.VarintType), 0)
	if err := proto.Unmarshal(body, destroy); err != nil {
		t.Fatal(err)
	}
	h.send(destroy)
	kill := &schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), MayInterruptIfRunning: proto.Bool(false)}
	body, _ = proto.Marshal(kill)
	body = protowire.AppendVarint(protowire.AppendTag(body, 7, protowire.VarintType), 7)
	body = protowire.AppendVarint(protowire.AppendTag(body, 8, protowire.VarintType), 0)
	if err := proto.Unmarshal(body, kill); err != nil {
		t.Fatal(err)
	}
	h.send(kill)
	next := h.request(2, "parallel", proto.Int32(2))
	next.Attempt = proto.Int32(1)
	h.send(next)
	h.assertNoStart()
	h.release()
	if id := h.started(); id != 2 {
		t.Fatalf("old attempt affected retry: task %d", id)
	}
}

func TestLifecycleRoutingActorsShareClosedEpoch(t *testing.T) {
	h := newPushHarness(t)
	remoteWorker := newContainerActorWithState(h.worker.containerAdmissionState)
	remotePID := h.system.Root.Spawn(actor.PropsFromProducer(func() actor.Actor { return remoteWorker }))
	defer func() { _ = h.system.Root.StopFuture(remotePID).Wait() }()
	first := h.request(1, "parallel", proto.Int32(2))
	first.SerialNum = proto.Int64(7)
	h.send(first)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(7), Attempt: proto.Int32(0)})
	late := h.request(2, "parallel", proto.Int32(2))
	late.SerialNum = proto.Int64(7)
	response, err := h.system.Root.RequestFuture(remotePID, late, time.Second).Result()
	if err != nil || !response.(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatalf("late delivery response: %v / %v", response, err)
	}
	h.assertNoStart()
	retry := h.request(2, "parallel", proto.Int32(2))
	retry.Attempt = proto.Int32(1)
	response, err = h.system.Root.RequestFuture(remotePID, retry, time.Second).Result()
	if err != nil || !response.(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatalf("retry response: %v / %v", response, err)
	}
	if id := h.started(); id != 2 {
		t.Fatalf("retry did not reopen across routes: %d", id)
	}
}

type delayedKillProcessor struct {
	*blockingProcessor
	killStarted chan struct{}
	releaseKill chan struct{}
}

func (p *delayedKillProcessor) Kill(_ *jobcontext.JobContext) error {
	close(p.killStarted)
	<-p.releaseKill
	return nil
}

func TestLifecycleDelayedKillCannotReportIntoNewCycle(t *testing.T) {
	h := newPushHarness(t)
	direct := make(chan *schedulerx.ContainerReportTaskStatusRequest, 4)
	masterPID, err := h.system.Root.SpawnNamed(actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case *schedulerx.ContainerReportTaskStatusRequest:
			direct <- msg
		case string:
			ctx.Respond(true)
		}
	}), "map_master_router")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.system.Root.StopFuture(masterPID).Wait() }()
	p := &delayedKillProcessor{h.processor, make(chan struct{}), make(chan struct{})}
	tasks.GetTaskMap().Register(t.Name(), p)
	var once sync.Once
	defer once.Do(func() { close(p.releaseKill) })
	remoteWorker := newContainerActorWithState(h.worker.containerAdmissionState)
	remotePID := h.system.Root.Spawn(actor.PropsFromProducer(func() actor.Actor { return remoteWorker }))
	defer func() { _ = h.system.Root.StopFuture(remotePID).Wait() }()
	first := h.request(1, "parallel", proto.Int32(2))
	first.SerialNum = proto.Int64(7)
	h.send(first)
	h.started()
	future := h.system.Root.RequestFuture(h.pid, &schedulerx.MasterKillContainerRequest{
		JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1),
		SerialNum: proto.Int64(7), Attempt: proto.Int32(0), MayInterruptIfRunning: proto.Bool(false),
	}, 3*time.Second)
	select {
	case <-p.killStarted:
	case <-time.After(time.Second):
		t.Fatal("Kill callback did not run")
	}
	next := h.request(2, "parallel", proto.Int32(2))
	next.SerialNum = proto.Int64(8)
	response, err := h.system.Root.RequestFuture(remotePID, next, time.Second).Result()
	if err != nil || !response.(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatalf("new cycle could not start while old Kill runs: %v / %v", response, err)
	}
	h.started()
	handler := h.worker.statusReqBatchHandlerPool.Get(1001)
	once.Do(func() { close(p.releaseKill) })
	if _, err := future.Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.system.Root.RequestFuture(masterPID, "barrier", time.Second).Result(); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-direct:
		t.Fatalf("retired Kill bypassed generation status handler: %v", msg)
	default:
	}
	latest := handler.GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
	if latest.GetTaskId() != 2 || latest.GetSerialNum() != 8 {
		t.Fatalf("old Kill contaminated new status handler: %v", latest)
	}
}

func TestLifecycleCurrentKillReportsFailureInItsOwnCycle(t *testing.T) {
	h := newPushHarness(t)
	first := h.request(1, "parallel", proto.Int32(1))
	first.SerialNum = proto.Int64(8)
	h.send(first)
	h.started()
	h.send(&schedulerx.MasterKillContainerRequest{
		JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1),
		SerialNum: proto.Int64(8), Attempt: proto.Int32(0), MayInterruptIfRunning: proto.Bool(false),
	})
	latest := h.worker.statusReqBatchHandlerPool.Get(1001).GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
	if latest.GetTaskId() != 1 || latest.GetSerialNum() != 8 || latest.GetStatus() != int32(processor.InstanceStatusFailed) || latest.GetResult() != "killed" {
		t.Fatalf("current Kill lost its terminal generation status: %v", latest)
	}
}

func TestLifecycleStaleDestroyRetainsCurrentHandlerAndQueuedWork(t *testing.T) {
	h := newPushHarness(t)
	current := h.request(1, "parallel", proto.Int32(1))
	current.SerialNum = proto.Int64(8)
	current.Attempt = proto.Int32(1)
	h.send(current)
	h.started()
	queued := h.request(2, "parallel", proto.Int32(1))
	queued.SerialNum = proto.Int64(8)
	queued.Attempt = proto.Int32(1)
	h.send(queued)
	handler := h.worker.statusReqBatchHandlerPool.Get(1001)
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{
		JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001),
		SerialNum: proto.Int64(7), Attempt: proto.Int32(0),
	})
	if h.worker.statusReqBatchHandlerPool.Get(1001) != handler {
		t.Fatal("stale Destroy stopped the current generation's status handler")
	}
	if !h.worker.containerPool.Contain("1_1001_1") || !h.worker.containerPool.Contain("1_1001_2") {
		t.Fatal("stale Destroy removed current running/queued task registrations")
	}
	h.release()
	if id := h.started(); id != 2 {
		t.Fatalf("stale Destroy discarded queued task %d", id)
	}
}
