/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package master

import (
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

func TestTimePlanOwnersCanShareDeadline(t *testing.T) {
	queue := NewTimeQueue()
	oldHandler := new(SecondJobUpdateInstanceStatusHandler)
	newHandler := new(SecondJobUpdateInstanceStatusHandler)
	oldPlan := NewTimePlanEntry(900001, 1234, oldHandler)
	newPlan := NewTimePlanEntry(900001, 1234, newHandler)
	queue.Add(oldPlan)
	queue.Add(newPlan)
	queue.Add(newPlan) // Re-enqueuing one owner's plan remains idempotent.
	if got := queue.Size(); got != 2 {
		t.Fatalf("replacement owner lost its same-deadline plan: size=%d, want 2", got)
	}
	seen := map[*TimePlanEntry]bool{queue.Pop(): true, queue.Pop(): true}
	if !seen[oldPlan] || !seen[newPlan] || queue.Size() != 0 {
		t.Fatal("each owner must retain one independently claimable plan")
	}
}

func TestTimePlanCancellationRetainsCapturedOwner(t *testing.T) {
	scheduler := newTimeScheduler()
	oldHandler := new(SecondJobUpdateInstanceStatusHandler)
	newHandler := new(SecondJobUpdateInstanceStatusHandler)
	oldPlan := NewTimePlanEntry(900003, 1234, oldHandler)
	laterOldPlan := NewTimePlanEntry(900003, 1235, oldHandler)
	newPlan := NewTimePlanEntry(900003, 1234, newHandler)
	otherInstancePlan := NewTimePlanEntry(900004, 1234, oldHandler)
	scheduler.add(oldPlan)
	scheduler.add(laterOldPlan)
	scheduler.add(otherInstancePlan)
	// The cancellation has already captured its owner when its goroutine is
	// delayed. Replacement publication must not change that cancellation's key.
	ready, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func(owner *SecondJobUpdateInstanceStatusHandler) {
		close(ready)
		<-resume
		scheduler.removeIfOwner(900003, owner)
		close(done)
	}(oldHandler)
	<-ready
	scheduler.add(newPlan)
	close(resume)
	<-done
	scheduler.add(newPlan) // Cancelling old plans must not clear new membership.
	if got := scheduler.timeQueue.Size(); got != 2 {
		t.Fatalf("old owner cancellation affected replacement/other instance: size=%d, want 2", got)
	}
	seen := map[*TimePlanEntry]bool{scheduler.timeQueue.Pop(): true, scheduler.timeQueue.Pop(): true}
	if !seen[newPlan] || !seen[otherInstancePlan] {
		t.Fatal("cancellation must match both instance and captured handler")
	}
}

// Only the actor context adapter is test-owned; Kill, its worker request/ACK,
// the Second status handler, owner-checked pool removal and time queue are real.
type planOwnerActorContext struct {
	actor.Context
	system *actor.ActorSystem
}

func (c planOwnerActorContext) ActorSystem() *actor.ActorSystem { return c.system }
func (c planOwnerActorContext) RequestFuture(pid *actor.PID, msg interface{}, timeout time.Duration) *actor.Future {
	return c.system.Root.RequestFuture(pid, msg, timeout)
}

func TestLateStandaloneKillPreservesReplacementPlan(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	pool := masterpool.GetTaskMasterPool()
	const instanceID int64 = 900002
	scheduler := GetTimeScheduler()
	scheduler.timeQueue.Remove(instanceID)
	t.Cleanup(func() { scheduler.timeQueue.Remove(instanceID); pool.Remove(instanceID) })
	system := actor.NewActorSystem()
	t.Cleanup(system.Shutdown)
	worker, err := system.Root.SpawnNamed(actor.PropsFromFunc(func(ctx actor.Context) {
		if _, ok := ctx.Message().(*schedulerx.MasterKillContainerRequest); ok {
			ctx.Respond(&schedulerx.MasterKillContainerResponse{Success: proto.Bool(true)})
		}
	}), actorcomm.ContainerRouterPidId)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { system.Root.Stop(worker) })
	ctx := planOwnerActorContext{system: system}
	deadline := time.Now().Add(time.Hour).UnixMilli()
	create := func(attempt int32) (*StandaloneTaskMaster, *TimePlanEntry) {
		info := new(common.JobInstanceInfo)
		info.SetJobId(9)
		info.SetJobInstanceId(instanceID)
		info.SetAttempt(attempt)
		info.SetTimeType(int32(common.TimeTypeSecondDelay))
		info.SetTimeExpression("1")
		m := &StandaloneTaskMaster{currentSelection: system.Address()}
		h := NewSecondJobUpdateInstanceStatusHandler(ctx, m, info)
		m.TaskMaster = NewTaskMaster(ctx, info, h)
		return m, NewTimePlanEntry(instanceID, deadline+int64(attempt), h)
	}
	old, oldPlan := create(0)
	replacement, newPlan := create(1)
	pool.Put(instanceID, old)
	scheduler.add(oldPlan)
	if err := old.KillInstance("killed from server"); err != nil {
		t.Fatal(err)
	}
	if pool.Contains(instanceID) || scheduler.timeQueue.Size() != 0 {
		t.Fatal("server Kill did not clear the old owner and its pending plan")
	}
	// The first Kill has completed, so normal Submit's Contains check admits a
	// new owner. A delayed callback can still retain and Kill the old master.
	pool.Put(instanceID, replacement)
	scheduler.add(newPlan)
	if err := old.KillInstance("killed from server"); err != nil {
		t.Fatal(err)
	}
	if pool.Get(instanceID) != replacement || replacement.IsKilled() {
		t.Fatal("late old Kill changed the replacement owner")
	}
	if scheduler.timeQueue.Size() != 1 || scheduler.timeQueue.Peek() != newPlan {
		t.Fatalf("late old Kill deleted replacement's queued plan: size=%d", scheduler.timeQueue.Size())
	}
	if err := replacement.KillInstance("killed from server"); err != nil {
		t.Fatal(err)
	}
	if scheduler.timeQueue.Size() != 0 || pool.Contains(instanceID) {
		t.Fatal("current owner Kill must still cancel its own plan and owner")
	}
}
