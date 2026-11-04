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
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"google.golang.org/protobuf/proto"
)

func TestCompletedInstanceWithoutDestroyExpires(t *testing.T) {
	h := newPushHarness(t)
	tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(*jobcontext.JobContext) (*processor.ProcessResult, error) {
		return processor.NewProcessResult(processor.WithSucceed()), nil
	}))
	h.push(1, "standalone", nil)
	waitBaselineTerminal(t, h, 1, int32(processor.InstanceStatusSucceed))
	handler := h.worker.statusReqBatchHandlerPool.Get(1001)
	deadline := time.Now().Add(3 * time.Second)
	for handler.IsActive() {
		if time.Now().After(deadline) {
			t.Fatal("real terminal status did not flush")
		}
		time.Sleep(time.Millisecond)
	}
	h.worker.admissionMu.Lock()
	for id := int64(1); len(h.worker.instances) < maxInstanceAdmissions; id++ {
		if id == 1001 {
			continue
		}
		if err := h.worker.closeInstance(id, 1, 0); err != nil {
			h.worker.admissionMu.Unlock()
			t.Fatal(err)
		}
	}
	h.worker.pruneInstanceAdmissions(time.Now().Add(2 * closedInstanceRetention))
	remaining := len(h.worker.instances)
	h.worker.admissionMu.Unlock()
	if remaining != 0 {
		t.Fatalf("completed instance without Destroy permanently retains admission: %d entries", remaining)
	}
	if h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("idle orphan status handler was retained")
	}
	next := h.request(1, "standalone", nil)
	next.JobInstanceId = proto.Int64(maxInstanceAdmissions + 1)
	if !h.send(next).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("capacity did not recover")
	}
	t.Cleanup(func() { batch.GetContainerStatusReqHandlerPool().Stop(maxInstanceAdmissions + 1) })
}

func TestIdleExpiryKeepsRunningAndQueuedWork(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.worker.admissionMu.Lock()
	h.worker.pruneInstanceAdmissions(time.Now().Add(2 * closedInstanceRetention))
	state := h.worker.instances[1001]
	h.worker.admissionMu.Unlock()
	if state == nil || !h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("live instance lost its admission or status handler")
	}
	h.release()
	if id := h.started(); id != 2 {
		t.Fatalf("live queue lost task %d", id)
	}
}

func TestExpiryExtendsActiveHeadAndReclaimsClosedTail(t *testing.T) {
	h := newPushHarness(t)
	h.push(1, "parallel", proto.Int32(1))
	h.started()
	h.worker.admissionMu.Lock()
	if err := h.worker.closeInstance(2002, 1, 7); err != nil {
		h.worker.admissionMu.Unlock()
		t.Fatal(err)
	}
	h.worker.pruneInstanceAdmissions(time.Now().Add(2 * closedInstanceRetention))
	active, closed := h.worker.instances[1001], h.worker.instances[2002]
	h.worker.admissionMu.Unlock()
	if active == nil || closed != nil {
		t.Fatalf("active head hid expired closed tail: active=%v closed=%v", active, closed)
	}
}

func TestIdleExpiryPreservesPendingTerminalStatus(t *testing.T) {
	h := newPushHarness(t)
	tasks.GetTaskMap().Register(t.Name(), baselineProcessorFunc(func(*jobcontext.JobContext) (*processor.ProcessResult, error) {
		return processor.NewProcessResult(processor.WithSucceed()), nil
	}))
	h.push(1, "parallel", proto.Int32(1))
	terminal := waitBaselineTerminal(t, h, 1, int32(processor.InstanceStatusSucceed))
	deadline := time.Now().Add(time.Second)
	for h.worker.containerPool.IsInstanceActive(1001) {
		if time.Now().After(deadline) {
			t.Fatal("executor did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	// Hold the real terminal in an ordinary handler queue before its retriever is
	// scheduled. Expiry must keep it, including while the queue becomes a batch.
	h.worker.statusReqBatchHandlerPool.Stop(1001)
	pending := batch.NewContainerStatusReqHandler(1001, 1, 1, 10, batch.NewReqQueue(2), terminal.GetInstanceMasterActorPath())
	pending.SubmitRequest(terminal)
	h.worker.statusReqBatchHandlerPool.GetHandlers().Store(int64(1001), pending)
	h.worker.admissionMu.Lock()
	h.worker.pruneInstanceAdmissions(time.Now().Add(2 * closedInstanceRetention))
	retained := h.worker.instances[1001] != nil
	h.worker.admissionMu.Unlock()
	if !retained || h.worker.statusReqBatchHandlerPool.Get(1001) != pending {
		t.Fatal("expiry lost unsent terminal status")
	}
	pending.AsyncHandleReqs(pending)
	deadline = time.Now().Add(time.Second)
	for pending.IsActive() {
		if time.Now().After(deadline) {
			t.Fatal("terminal batch did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	h.worker.admissionMu.Lock()
	h.worker.pruneInstanceAdmissions(time.Now().Add(4 * closedInstanceRetention))
	remaining := h.worker.instances[1001]
	h.worker.admissionMu.Unlock()
	if remaining != nil || h.worker.statusReqBatchHandlerPool.Contains(1001) {
		t.Fatal("flushed orphan did not expire")
	}
}
