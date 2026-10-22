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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
)

func TestRunningCancellationDeduplicatesUntilExit(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(2))
	h.started()
	h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1), Attempt: proto.Int32(0), SerialNum: proto.Int64(0), MayInterruptIfRunning: proto.Bool(false)})
	// No KillProcessor is registered: the first real Process is still blocked.
	// Re-delivery is identical, not a newer attempt/cycle.
	h.push(1, "parallel", proto.Int32(2))
	select {
	case id := <-h.processor.started:
		t.Fatalf("same task %d entered Process twice while its first invocation is still alive", id)
	case <-time.After(100 * time.Millisecond):
	}
	h.release()
	deadline := time.Now().Add(time.Second)
	for h.worker.containerPool.Contain("1_1001_1") {
		if time.Now().After(deadline) {
			t.Fatal("cancelled invocation did not reclaim its registration")
		}
		time.Sleep(time.Millisecond)
	}
	latest := h.worker.statusReqBatchHandlerPool.Get(1001).GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
	if latest.GetStatus() != int32(processor.InstanceStatusFailed) || latest.GetResult() != "killed" {
		t.Fatalf("late completion overwrote cancellation: %v", latest)
	}
	h.assertNoStart()
}

func TestRetryGenerationsShareInstanceQuota(t *testing.T) {
	h := newPushHarness(t)
	starts := 0
	for attempt := int32(0); attempt < 5; attempt++ {
		req := h.request(1, "parallel", proto.Int32(1))
		req.Attempt = proto.Int32(attempt)
		h.send(req)
		select {
		case <-h.processor.started:
			starts++
		case <-time.After(100 * time.Millisecond):
		}
	}
	// Every invocation still waits on the shared release channel.
	if starts != 1 {
		t.Fatalf("consumerSize=1 but %d real Process invocations for instance1001 are simultaneously blocked across retry generations", starts)
	}
	h.release()
	if id := h.started(); id != 1 {
		t.Fatalf("latest generation not admitted after old exit: %d", id)
	}
	h.assertNoStart()
}

func TestAdmissionRejectionBatchReachesMaster(t *testing.T) {
	h := newPushHarness(t)
	starter, err := ants.NewPool(1)
	if err != nil {
		t.Fatal(err)
	}
	defer starter.Release()
	h.worker.containerStarter = starter
	const rejectedID = int64(maxInstanceAdmissions + 1)
	delivered := make(chan *schedulerx.ContainerReportTaskStatusRequest, 1)
	lost := make(chan *schedulerx.ContainerReportTaskStatusRequest, 1)
	masterPID, err := h.system.Root.SpawnNamed(actor.PropsFromFunc(func(ctx actor.Context) {
		switch req := ctx.Message().(type) {
		case *schedulerx.ContainerReportTaskStatusRequest:
			delivered <- req
		case string:
			ctx.Respond(true)
		}
	}), "map_master_router")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.system.Root.StopFuture(masterPID).Wait() }()
	subscription := h.system.EventStream.Subscribe(func(event interface{}) {
		if dead, ok := event.(*actor.DeadLetterEvent); ok {
			if req, ok := dead.Message.(*schedulerx.ContainerReportTaskStatusRequest); ok && req.GetJobInstanceId() == rejectedID {
				select {
				case lost <- req:
				default:
				}
			}
		}
	})
	defer h.system.EventStream.Unsubscribe(subscription)
	h.worker.admissionMu.Lock()
	for id := int64(1); id <= maxInstanceAdmissions; id++ {
		if err := h.worker.closeInstance(id, 1, 0); err != nil {
			h.worker.admissionMu.Unlock()
			t.Fatal(err)
		}
	}
	req := h.request(1, "parallel", proto.Int32(1))
	req.JobInstanceId = proto.Int64(rejectedID)
	req.SerialNum = proto.Int64(7)
	// Admission is deliberately paused so the ordinary batch ACK wins the race.
	// Its async body resumes after the RequestFuture has already ended.
	response, err := h.system.Root.RequestFuture(h.pid, &schedulerx.MasterBatchStartContainersRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(rejectedID), StartReqs: []*schedulerx.MasterStartContainerRequest{req}}, time.Second).Result()
	h.worker.admissionMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := response.(*schedulerx.MasterBatchStartContainersResponse)
	if !ok {
		t.Fatalf("wrong response type %T", response)
	}
	if !ack.GetSuccess() {
		t.Fatal("batch envelope ACK unexpectedly failed")
	}
	completed := make(chan struct{})
	if err := starter.Submit(func() { close(completed) }); err != nil {
		t.Fatal(err)
	}
	<-completed // async admission callback has finished, including its failure send.
	if _, err := h.system.Root.RequestFuture(masterPID, "barrier", time.Second).Result(); err != nil {
		t.Fatal(err)
	}
	select {
	case failure := <-delivered:
		if failure.GetSerialNum() != 7 || !strings.Contains(failure.GetResult(), "admission history") || failure.GetStatus() != int32(processor.InstanceStatusFailed) {
			t.Fatalf("incomplete failure metadata: %v", failure)
		}
	case failure := <-lost:
		t.Fatalf("batch ACK=true; admission rejection failure was sent to dead RequestFuture, not master: %v", failure)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("batch ACK=true after the rejected admission callback completed, but no execution or terminal failure at the actual master")
	}
}

func TestLegacyZeroSerialDestroyTargetsCurrentCycle(t *testing.T) {
	h := newPushHarness(t)
	req := h.request(1, "parallel", proto.Int32(1))
	req.SerialNum = proto.Int64(7)
	h.send(req)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(0)})
	if h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("legacy explicit zero was not treated as wildcard cleanup")
	}
}

func TestNewEpochZeroSerialDestroyIsStale(t *testing.T) {
	h := newPushHarness(t)
	req := h.request(1, "parallel", proto.Int32(1))
	req.SerialNum = proto.Int64(7)
	req.Attempt = proto.Int32(1)
	h.send(req)
	h.started()
	h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), SerialNum: proto.Int64(0), Attempt: proto.Int32(1)})
	if !h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("new protocol zero serial incorrectly became wildcard cleanup")
	}
}

func TestDelayedKillAfterExitCannotFailSameCycleReplacement(t *testing.T) {
	h := newPushHarness(t)
	p := &delayedKillProcessor{h.processor, make(chan struct{}), make(chan struct{})}
	tasks.GetTaskMap().Register(t.Name(), p)
	var once sync.Once
	defer once.Do(func() { close(p.releaseKill) })
	remoteWorker := newContainerActorWithState(h.worker.containerAdmissionState)
	remotePID := h.system.Root.Spawn(actor.PropsFromProducer(func() actor.Actor { return remoteWorker }))
	defer func() { _ = h.system.Root.StopFuture(remotePID).Wait() }()
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	future := h.system.Root.RequestFuture(h.pid, &schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1), MayInterruptIfRunning: proto.Bool(false)}, 3*time.Second)
	select {
	case <-p.killStarted:
	case <-time.After(time.Second):
		t.Fatal("Kill callback not entered")
	}
	handler := h.worker.statusReqBatchHandlerPool.Get(1001)
	latest := handler.GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
	if latest.GetStatus() != int32(processor.InstanceStatusFailed) || latest.GetResult() != "killed" {
		t.Fatalf("cancellation was not terminal before user callback: %v", latest)
	}
	h.release()
	waitEmpty := func() {
		deadline := time.Now().Add(time.Second)
		for h.worker.containerPool.Contain("1_1001_1") {
			if time.Now().After(deadline) {
				t.Fatal("execution did not exit")
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitEmpty()
	if _, err := h.system.Root.RequestFuture(remotePID, h.request(1, "parallel", proto.Int32(1)), time.Second).Result(); err != nil {
		t.Fatal(err)
	}
	h.started()
	waitEmpty()
	once.Do(func() { close(p.releaseKill) })
	if _, err := future.Result(); err != nil {
		t.Fatal(err)
	}
	latest = handler.GetLatestRequest().(*schedulerx.ContainerReportTaskStatusRequest)
	if latest.GetStatus() != int32(processor.InstanceStatusSucceed) {
		t.Fatalf("retired Kill callback overwrote replacement success: %v", latest)
	}
}

func TestCompletedTaskIDCanStillBeRetried(t *testing.T) {
	h := newPushHarness(t)
	h.release()
	for retry := 0; retry < 2; retry++ {
		h.push(1, "parallel", proto.Int32(1))
		if id := h.started(); id != 1 {
			t.Fatalf("wrong task: %d", id)
		}
		deadline := time.Now().Add(time.Second)
		for h.worker.containerPool.Contain("1_1001_1") {
			if time.Now().After(deadline) {
				t.Fatal("completed task retained dedup identity")
			}
			time.Sleep(time.Millisecond)
		}
	}
}
