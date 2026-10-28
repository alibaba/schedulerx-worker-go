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
	"testing"
	"time"
)

type r6PullReplyContext struct {
	pa.Context
	reached, release chan struct{}
	sent             chan *schedulerx.PullTaskFromMasterResponse
}

func (c *r6PullReplyContext) Sender() *pa.PID {
	close(c.reached)
	<-c.release
	return pa.NewPID("worker", "receiver")
}
func (c *r6PullReplyContext) Send(_ *pa.PID, msg interface{}) {
	c.sent <- msg.(*schedulerx.PullTaskFromMasterResponse)
}

func TestPullReplyHandoffCannotLaunchRetiredOwner(t *testing.T) {
	h := newPushHarness(t)
	wd, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	// The worker already knows this same wire identity. Our claim concerns a
	// newly sent local response after retirement, not a previously sent message.
	first := h.request(1, "grid", proto.Int32(2))
	first.Attempt = proto.Int32(0)
	first.SerialNum = proto.Int64(0)
	h.send(first)
	h.started()
	info := new(common.JobInstanceInfo)
	info.SetJobId(1)
	info.SetJobInstanceId(1001)
	info.SetAttempt(0)
	info.SetExecuteMode("grid")
	info.SetJobType("golang")
	info.SetContent(`{"jobName":"` + t.Name() + `"}`)
	info.SetAllWorkers([]string{"worker@127.0.0.1:12345"})
	info.SetXattrs(`{"taskDispatchMode":"pull","consumerSize":2,"globalConsumerSize":100}`)
	ctx := adversarialMasterContext{system: h.system}
	old := master.NewGridTaskMaster(info, ctx)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	if _, err := old.Map(nil, [][]byte{[]byte("old-body")}, "old-subtask"); err != nil {
		t.Fatal(err)
	}
	reply := &r6PullReplyContext{reached: make(chan struct{}), release: make(chan struct{}), sent: make(chan *schedulerx.PullTaskFromMasterResponse, 1)}
	a := &taskActor{taskMasterPool: pool}
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.handlePullTasks(reply, &schedulerx.PullTaskFromMasterRequest{JobInstanceId: proto.Int64(1001), PageSize: proto.Int32(4), WorkerIdAddr: proto.String("worker@127.0.0.1:12345")})
	}()
	select {
	case <-reply.reached:
	case <-time.After(time.Second):
		t.Fatal("reply handoff not reached")
	}
	current := master.NewGridTaskMaster(info, ctx)
	pool.Put(1001, current)
	defer pool.Remove(1001)
	close(reply.release)
	<-done
	response := <-reply.sent
	if len(response.GetRequest()) == 0 {
		return
	}
	req := response.GetRequest()[0]
	if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		return
	}
	if id := h.started(); id == req.GetTaskId() {
		t.Fatalf("NEW pull reply sent after pool replacement launched retired task=%d attempt=%d serial=%d", id, req.GetAttempt(), req.GetSerialNum())
	}
}
