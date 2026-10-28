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
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	pa "github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
	"os"
	"sync"
	"testing"
	"time"
)

func r7Grid(t *testing.T) (*master.GridTaskMaster, *pushHarness) {
	t.Helper()
	h := newPushHarness(t)
	wd, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	info := new(common.JobInstanceInfo)
	info.SetJobId(1)
	info.SetJobInstanceId(1001)
	info.SetAttempt(0)
	info.SetExecuteMode("grid")
	info.SetJobType("golang")
	info.SetContent(`{"jobName":"` + t.Name() + `"}`)
	info.SetAllWorkers([]string{"worker@127.0.0.1:12345"})
	info.SetXattrs(`{"taskDispatchMode":"pull","consumerSize":2,"globalConsumerSize":100}`)
	m := master.NewGridTaskMaster(info, adversarialMasterContext{system: h.system})
	t.Cleanup(func() { m.Stop(); masterpool.GetTaskMasterPool().Remove(1001) })
	return m, h
}
func r7Request() *schedulerx.PullTaskFromMasterRequest {
	return &schedulerx.PullTaskFromMasterRequest{JobInstanceId: proto.Int64(1001), PageSize: proto.Int32(4), WorkerIdAddr: proto.String("worker@127.0.0.1:12345")}
}
func r7Wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deterministic handoff")
	}
}

type r7Reply struct {
	pa.Context
	sent chan *schedulerx.PullTaskFromMasterResponse
}

func (c *r7Reply) Sender() *pa.PID { return pa.NewPID("worker", "reply") }
func (c *r7Reply) Send(_ *pa.PID, msg interface{}) {
	c.sent <- msg.(*schedulerx.PullTaskFromMasterResponse)
}

type r7ReceiptOwner struct {
	*master.GridTaskMaster
	once             sync.Once
	entered, release chan struct{}
	serials          []int64
}

func (m *r7ReceiptOwner) SyncPullTasks(n int32, w string, serials ...int64) []*schedulerx.MasterStartContainerRequest {
	m.once.Do(func() { m.serials = append([]int64(nil), serials...); close(m.entered); <-m.release })
	return m.GridTaskMaster.SyncPullTasks(n, w, serials...)
}

func TestReceiptCycleCannotConsumeNewCycleQueue(t *testing.T) {
	m, _ := r7Grid(t)
	wrapped := &r7ReceiptOwner{GridTaskMaster: m, entered: make(chan struct{}), release: make(chan struct{})}
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, wrapped)
	reply := &r7Reply{sent: make(chan *schedulerx.PullTaskFromMasterResponse, 1)}
	done := make(chan struct{})
	go func() { defer close(done); (&taskActor{taskMasterPool: pool}).handlePullTasks(reply, r7Request()) }()
	r7Wait(t, wrapped.entered)
	if len(wrapped.serials) != 1 || wrapped.serials[0] != 0 {
		t.Fatalf("actor did not carry receipt generation: %v", wrapped.serials)
	}
	// Isolate the precise generation boundary; full Clear/restart is separately
	// exercised by the production Second regression. The same owner is now active at 1.
	m.AcquireSerialNum()
	if _, err := m.Map(nil, [][]byte{[]byte("new-cycle-body")}, "new-cycle"); err != nil {
		t.Fatal(err)
	}
	close(wrapped.release)
	r7Wait(t, done)
	response := <-reply.sent
	if response.GetSuccess() || len(response.GetRequest()) != 0 {
		t.Fatalf("stale RPC received tasks: %+v", response)
	}
	current := m.SyncPullTasks(4, "worker@127.0.0.1:12345", 1)
	if len(current) != 1 || current[0].GetTaskName() != "new-cycle" || current[0].GetSerialNum() != 1 {
		t.Fatalf("old RPC ate next cycle queue: %+v", current)
	}
}

func TestDequeueToReplyCannotCrossSerial(t *testing.T) {
	m, _ := r7Grid(t)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	m.Map(nil, [][]byte{[]byte("old")}, "old")
	reply := &r6PullReplyContext{reached: make(chan struct{}), release: make(chan struct{}), sent: make(chan *schedulerx.PullTaskFromMasterResponse, 1)}
	done := make(chan struct{})
	go func() { defer close(done); (&taskActor{taskMasterPool: pool}).handlePullTasks(reply, r7Request()) }()
	r7Wait(t, reply.reached)
	m.AcquireSerialNum()
	close(reply.release)
	r7Wait(t, done)
	response := <-reply.sent
	if response.GetSuccess() || len(response.GetRequest()) != 0 {
		t.Fatalf("old cycle response escaped: %+v", response)
	}
}

func TestNormalActorPullReplyRunsActualWorker(t *testing.T) {
	m, h := r7Grid(t)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	m.Map(nil, [][]byte{[]byte("normal")}, "normal")
	reply := &r7Reply{sent: make(chan *schedulerx.PullTaskFromMasterResponse, 1)}
	(&taskActor{taskMasterPool: pool}).handlePullTasks(reply, r7Request())
	response := <-reply.sent
	if !response.GetSuccess() || len(response.GetRequest()) != 1 {
		t.Fatalf("normal reply regressed: %+v", response)
	}
	req := response.GetRequest()[0]
	if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		t.Fatal("normal worker rejected task")
	}
	if id := h.started(); id != req.GetTaskId() {
		t.Fatalf("normal task not executed: %d", id)
	}
}

type r7HeldSend struct {
	r7Reply
	entered, release chan struct{}
}

func (c *r7HeldSend) Send(pid *pa.PID, msg interface{}) {
	close(c.entered)
	<-c.release
	c.r7Reply.Send(pid, msg)
}
func TestActualSendJoinsRetirement(t *testing.T) {
	m, h := r7Grid(t)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	m.Map(nil, [][]byte{[]byte("admitted")}, "admitted")
	reply := &r7HeldSend{r7Reply: r7Reply{sent: make(chan *schedulerx.PullTaskFromMasterResponse, 1)}, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); (&taskActor{taskMasterPool: pool}).handlePullTasks(reply, r7Request()) }()
	r7Wait(t, reply.entered)
	next := master.NewGridTaskMaster(m.GetJobInstanceInfo(), adversarialMasterContext{system: h.system})
	replaced := make(chan struct{})
	go func() { defer close(replaced); pool.Put(1001, next) }()
	select {
	case <-replaced:
		t.Fatal("replacement published before admitted Send completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(reply.release)
	r7Wait(t, done)
	r7Wait(t, replaced)
	if response := <-reply.sent; len(response.GetRequest()) != 1 {
		t.Fatal("admitted reply lost its task")
	}
}
