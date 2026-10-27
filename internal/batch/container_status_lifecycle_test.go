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

package batch

import (
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"google.golang.org/protobuf/proto"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
)

func TestStatusSubmissionCannotEnterReplacementHandler(t *testing.T) {
	p := NewContainerStatusReqHandlerPool()
	old := NewContainerStatusReqHandler(1, 1, 1, 10, NewReqQueue(1), "old")
	current := NewContainerStatusReqHandler(1, 1, 1, 10, NewReqQueue(1), "new")
	p.handlers.Store(int64(1), current)
	req := &schedulerx.ContainerReportTaskStatusRequest{SerialNum: proto.Int64(7)}
	if p.SubmitReqTo(1, old, req) {
		t.Fatal("old generation status accepted by new handler")
	}
	if current.GetLatestRequest() != nil || current.reqsQueue.Size() != 0 {
		t.Fatal("old status contaminated current batch")
	}
}

func TestStatusSubmissionUnblocksWhenHandlerStops(t *testing.T) {
	p := NewContainerStatusReqHandlerPool()
	h := NewContainerStatusReqHandler(1, 1, 1, 10, NewReqQueue(1), "old")
	// A running retriever's queue can be full while its network batch is blocked.
	h.stopBatchRetrieveCh = make(chan struct{})
	p.handlers.Store(int64(1), h)
	req := &schedulerx.ContainerReportTaskStatusRequest{}
	h.reqsQueue.SubmitRequest(req)
	done := make(chan bool, 1)
	go func() { done <- p.SubmitReqTo(1, h, req) }()
	p.Stop(1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopped handler stranded a status submitter")
	}
}

func TestEmptyBatchRetrievalDoesNotRetainActivity(t *testing.T) {
	h := NewContainerStatusReqHandler(1, 1, 1, 10, NewReqQueue(2), "worker")
	for i := 0; i < 100; i++ {
		if len(h.AsyncHandleReqs(h)) != 0 || h.IsActive() {
			t.Fatal("empty retrieval retained active work")
		}
	}
}

func TestRejectedStatusBatchReleasesActivity(t *testing.T) {
	pool, err := ants.NewPool(1)
	if err != nil {
		t.Fatal(err)
	}
	pool.Release()
	previous := globalPool
	globalPool = pool
	t.Cleanup(func() { globalPool = previous })

	h := NewContainerStatusReqHandler(9201, 1, 1, 10, NewReqQueue(2), "worker")
	h.SubmitRequest(&schedulerx.ContainerReportTaskStatusRequest{JobInstanceId: proto.Int64(9201)})
	h.AsyncHandleReqs(h)
	if h.IsActive() {
		t.Fatal("rejected status batch permanently retains activity and prevents idle reclamation")
	}
}

func TestPanickingStatusBatchReleasesActivity(t *testing.T) {
	recovered := make(chan struct{})
	pool, err := ants.NewPool(1, ants.WithPanicHandler(func(interface{}) { close(recovered) }))
	if err != nil {
		t.Fatal(err)
	}
	previous := globalPool
	globalPool = pool
	t.Cleanup(func() { globalPool = previous; pool.Release() })

	h := NewContainerStatusReqHandler(9202, 1, 1, 10, NewReqQueue(2), "worker")
	// A typed nil reaches the real asynchronous encoder and panics there.
	// Accounting must be released even when the executor recovers that panic.
	h.SubmitRequest((*schedulerx.ContainerReportTaskStatusRequest)(nil))
	h.AsyncHandleReqs(h)
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("asynchronous status encoder did not reach the panic handler")
	}
	if h.IsActive() {
		t.Fatal("panicking status batch permanently retains activity and prevents idle reclamation")
	}
}

func TestMalformedStatusBatchReleasesActivity(t *testing.T) {
	h := NewContainerStatusReqHandler(9203, 1, 1, 10, NewReqQueue(2), "worker")
	h.SubmitRequest(struct{}{})
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		h.AsyncHandleReqs(h)
	}()
	if !panicked {
		t.Fatal("malformed status batch must not silently swallow the encoding panic")
	}
	if h.IsActive() {
		t.Fatal("malformed status batch permanently retains activity before executor submission")
	}
}

func TestBlockedStatusDeliveryRetainsActivity(t *testing.T) {
	output := actorcomm.AtLeastOnceDeliveryMsgReceiver()
	if len(output) != 0 {
		t.Fatal("status delivery channel was not drained by an earlier test")
	}
	for i := 0; i < cap(output); i++ {
		output <- nil
	}
	t.Cleanup(func() {
		for {
			select {
			case <-output:
			default:
				return
			}
		}
	})
	h := NewContainerStatusReqHandler(9204, 1, 1, 10, NewReqQueue(2), "worker")
	h.SubmitRequest(&schedulerx.ContainerReportTaskStatusRequest{JobInstanceId: proto.Int64(9204)})
	h.AsyncHandleReqs(h)
	if !h.IsActive() {
		t.Fatal("unsent status batch became idle while delivery was blocked")
	}
	<-output // Make room for the actual status without discarding it.
	deadline := time.Now().Add(time.Second)
	for h.IsActive() {
		if time.Now().After(deadline) {
			t.Fatal("delivered status batch did not release activity")
		}
		time.Sleep(time.Millisecond)
	}
}
