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
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

type adversarialMasterContext struct {
	actor.Context
	system *actor.ActorSystem
}

func (c adversarialMasterContext) ActorSystem() *actor.ActorSystem { return c.system }
func TestRetiredPullCannotExecuteOnCurrentWorker(t *testing.T) {
	h := newPushHarness(t)
	wd, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(wd) })
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
	// Map queued before retirement. A Pull RPC has already resolved old.
	if _, err := old.Map(nil, [][]byte{[]byte("retired body")}, "old-subtask"); err != nil {
		t.Fatal(err)
	}
	current := master.NewGridTaskMaster(info, ctx)
	pool.Put(1001, current)
	defer pool.Remove(1001)
	response := old.SyncPullTasks(4, "worker@127.0.0.1:12345")
	if len(response) == 0 {
		return
	}
	req := response[0]
	t.Logf("retired Pull returned task=%d attempt=%d serial=%d; Worker is already running same epoch", req.GetTaskId(), req.GetAttempt(), req.GetSerialNum())
	if !h.send(req).(*schedulerx.MasterStartContainerResponse).GetSuccess() {
		return
	}
	if id := h.started(); id == req.GetTaskId() {
		t.Fatalf("retired owner task %d entered actual updated Worker Process despite rejected owner persistence", id)
	}
}
