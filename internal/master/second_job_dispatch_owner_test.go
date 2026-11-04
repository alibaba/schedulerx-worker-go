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

package master

import (
	"os"
	"os/exec"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	pa "github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

// Dispatch has already claimed each plan. Retirement/replacement of a master
// must not queue its replacement behind an old owner's delayed network reply.
func TestSecondJobDispatchReplacementDoesNotWaitForOldOwner(t *testing.T) {
	const child = "SCHEDULERX_DISPATCH_REPLACEMENT"
	if os.Getenv(child) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSecondJobDispatchReplacementDoesNotWaitForOldOwner$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), child+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("replacement dispatch child: %v\n%s", err, out)
		}
		return
	}
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	system := pa.NewActorSystem()
	defer system.Shutdown()
	system.ProcessRegistry.Address = "127.0.0.1:39273"
	oldReached := make(chan *pa.PID, 1)
	replacementReached := make(chan struct{}, 1)
	_, err := system.Root.SpawnNamed(pa.PropsFromFunc(func(ctx pa.Context) {
		if req, ok := ctx.Message().(*schedulerx.MasterStartContainerRequest); ok {
			if req.GetAttempt() == 0 {
				oldReached <- ctx.Sender()
				return
			}
			replacementReached <- struct{}{}
			ctx.Respond(&schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)})
		}
	}), actorcomm.ContainerRouterPidId)
	if err != nil {
		t.Fatal(err)
	}
	ctx := scannerBackpressureContext{system: system}
	makeOwner := func(attempt int32) (*StandaloneTaskMaster, *TimePlanEntry) {
		info := new(common.JobInstanceInfo)
		info.SetJobId(103)
		info.SetJobInstanceId(103)
		info.SetAttempt(attempt)
		info.SetExecuteMode("standalone")
		info.SetJobType("golang")
		info.SetContent(`{"jobName":"backpressure"}`)
		info.SetAllWorkers([]string{"worker@" + system.Address()})
		info.SetTimeType(int32(common.TimeTypeSecondDelay))
		info.SetTimeExpression("1")
		m := &StandaloneTaskMaster{currentSelection: system.Address()}
		h := NewSecondJobUpdateInstanceStatusHandler(ctx, m, info)
		m.TaskMaster = NewTaskMaster(ctx, info, h)
		return m, NewTimePlanEntry(103, time.Now().UnixMilli(), h)
	}
	old, first := makeOwner(0)
	masterpool.GetTaskMasterPool().Put(103, old)
	scanner := newTimeScheduler()
	scanner.dispatchPlan(first)
	var reply *pa.PID
	select {
	case reply = <-oldReached:
	case <-time.After(time.Second):
		t.Fatal("old real Start did not reach worker")
	}
	replacement, next := makeOwner(1)
	masterpool.GetTaskMasterPool().Put(103, replacement)
	scanner.dispatchPlan(next)
	select {
	case <-replacementReached:
	case <-time.After(300 * time.Millisecond):
		t.Error("replacement owner waited for old owner's pending Start ACK")
	}
	system.Root.Send(reply, &schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)})
}
