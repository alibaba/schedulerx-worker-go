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

	"google.golang.org/protobuf/proto"

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
