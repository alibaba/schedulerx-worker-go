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

type scannerBackpressureContext struct {
	pa.Context
	system *pa.ActorSystem
}

func (c scannerBackpressureContext) ActorSystem() *pa.ActorSystem { return c.system }
func (c scannerBackpressureContext) Self() *pa.PID {
	return pa.NewPID(c.system.Address(), "scanner-test")
}
func (c scannerBackpressureContext) RequestFuture(pid *pa.PID, body interface{}, timeout time.Duration) *pa.Future {
	return c.system.Root.RequestFuture(pid, body, timeout)
}

// Both masters, both production Second handlers, the real global-style scanner,
// and actual actor.Future wait are used. Only the worker's ACK is delayed.
func TestSlowStandaloneStartDoesNotBlockOtherDuePlan(t *testing.T) {
	const child = "SCHEDULERX_BLOCKED_SCANNER"
	if os.Getenv(child) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSlowStandaloneStartDoesNotBlockOtherDuePlan$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), child+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("real scanner child failed: %v\n%s", err, out)
		}
		return
	}
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	system := pa.NewActorSystem()
	defer system.Shutdown()
	system.ProcessRegistry.Address = "127.0.0.1:39271"
	firstReached := make(chan *pa.PID, 1)
	secondReached := make(chan struct{}, 1)
	sameInstanceReached := make(chan int64, 1)
	_, err := system.Root.SpawnNamed(pa.PropsFromFunc(func(ctx pa.Context) {
		if req, ok := ctx.Message().(*schedulerx.MasterStartContainerRequest); ok {
			if req.GetJobInstanceId() == 101 {
				if req.GetSerialNum() == 1 {
					firstReached <- ctx.Sender()
					return
				}
				sameInstanceReached <- req.GetSerialNum()
				ctx.Respond(&schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)})
				return
			}
			secondReached <- struct{}{}
			ctx.Respond(&schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)})
		}
	}), actorcomm.ContainerRouterPidId)
	if err != nil {
		t.Fatal(err)
	}
	scanner := newTimeScheduler()
	ctx := scannerBackpressureContext{system: system}
	plan := func(id, deadline int64) *TimePlanEntry {
		info := new(common.JobInstanceInfo)
		info.SetJobId(id)
		info.SetJobInstanceId(id)
		info.SetExecuteMode("standalone")
		info.SetJobType("golang")
		info.SetContent(`{"jobName":"review"}`)
		info.SetAllWorkers([]string{"worker@" + system.Address()})
		info.SetTimeType(int32(common.TimeTypeSecondDelay))
		info.SetTimeExpression("1")
		master := &StandaloneTaskMaster{currentSelection: system.Address()}
		handler := NewSecondJobUpdateInstanceStatusHandler(ctx, master, info)
		master.TaskMaster = NewTaskMaster(ctx, info, handler)
		masterpool.GetTaskMasterPool().Put(id, master)
		return NewTimePlanEntry(id, deadline, handler)
	}
	now := time.Now().UnixMilli()
	first := plan(101, now-2000)
	scanner.add(first)
	scanner.add(NewTimePlanEntry(101, now-1500, first.Handler()))
	scanner.add(plan(102, now-1000))
	go scanner.timeScan()
	var pending *pa.PID
	select {
	case pending = <-firstReached:
	case <-time.After(time.Second):
		t.Fatal("first actual Standalone request never reached worker")
	}
	isolationPassed := false
	select {
	case <-secondReached:
		isolationPassed = true
	case <-time.After(200 * time.Millisecond):
		t.Errorf("unrelated due plan blocked behind first Standalone Future; remaining queue=%d", scanner.timeQueue.Size())
	}
	select {
	case serial := <-sameInstanceReached:
		t.Errorf("same instance started serial %d before previous Start completed", serial)
	default:
	}
	// Releasing only this ACK must allow the next plan for the same instance.
	system.Root.Send(pending, &schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)})
	select {
	case serial := <-sameInstanceReached:
		if serial != 2 {
			t.Errorf("next instance serial=%d, want 2", serial)
		}
	case <-time.After(time.Second):
		t.Error("same-instance FIFO did not resume")
	}
	if !isolationPassed {
		select {
		case <-secondReached:
		case <-time.After(time.Second):
			t.Fatal("other due task never started after release")
		}
	}
}
