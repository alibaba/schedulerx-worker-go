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
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/asynkron/protoactor-go/actor"
)

type r6StartCapture struct {
	retirementActorContext
	starts chan *schedulerx.MasterStartContainerRequest
}

func (c r6StartCapture) RequestFuture(pid *actor.PID, message interface{}, timeout time.Duration) *actor.Future {
	if batch, ok := message.(*schedulerx.MasterBatchStartContainersRequest); ok {
		for _, start := range batch.GetStartReqs() {
			c.starts <- start
		}
	}
	return c.retirementActorContext.RequestFuture(pid, message, timeout)
}

// Exercise production constructor, terminal handler, scheduler plan submission,
// root recreation, reduce finalization and pull availability through two cycles.
func TestProductionSecondPlanRestartsAndFinalizesTwice(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	drainR4(actorcomm.AtLeastOnceDeliveryMsgReceiver())
	defer drainR4(actorcomm.AtLeastOnceDeliveryMsgReceiver())
	m := newR4ProductionSecondGrid(t)
	defer m.Stop()
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	defer pool.Remove(1001)
	capture := r6StartCapture{retirementActorContext{system: actor.NewActorSystem()}, make(chan *schedulerx.MasterStartContainerRequest, 8)}
	m.actorCtx, m.actorContext = capture, capture
	m.localWorkIdAddr = "worker@127.0.0.1:1234"
	m.jobInstanceInfo.SetContent(`{"jobName":"r6reduce"}`)
	reduce := &r5Reduce{serials: make(chan int64, 8)}
	pool.Tasks().Register("r6reduce", reduce)
	m.xAttrs.GlobalConsumerSize = 10
	q := GetTimeScheduler().timeQueue
	q.Clear()
	defer q.Clear()
	if err := m.SubmitInstance(m.jobInstanceInfo); err != nil {
		t.Fatal(err)
	}
	first := <-capture.starts
	if first.GetSerialNum() != 0 {
		t.Fatalf("first root serial = %d", first.GetSerialNum())
	}
	if err := m.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, "done"); err != nil {
		t.Fatal(err)
	}
	if q.Size() != 1 || !m.cycleClosed {
		t.Fatalf("first terminal did not prepare exactly one next plan: plans=%d closed=%v", q.Size(), m.cycleClosed)
	}
	GetTimeScheduler().submitPlan(q.Pop())
	second := <-capture.starts
	if second.GetSerialNum() != 1 || m.cycleClosed || !m.cycleActive(1) {
		t.Fatalf("second root unavailable: serial=%d closed=%v", second.GetSerialNum(), m.cycleClosed)
	}
	ctx := new(jobcontext.JobContext)
	ctx.SetSerialNum(1)
	ctx.SetAttempt(0)
	if _, err := m.Map(ctx, [][]byte{[]byte("second-round")}, "sub"); err != nil {
		t.Fatal(err)
	}
	tasks := m.SyncPullTasks(1, "worker@127.0.0.1:1234")
	if len(tasks) != 1 || tasks[0].GetSerialNum() != 1 {
		t.Fatalf("second round pull returned %#v", tasks)
	}
	if err := m.UpdateNewInstanceStatus(1, processor.InstanceStatusSucceed, "done"); err != nil {
		t.Fatal(err)
	}
	if q.Size() != 1 {
		t.Fatalf("second terminal plans=%d", q.Size())
	}
	for want := int64(0); want < 2; want++ {
		select {
		case got := <-reduce.serials:
			if got != want {
				t.Fatalf("reduce serial=%d want=%d", got, want)
			}
		default:
			t.Fatalf("missing reduce for serial=%d", want)
		}
	}
	if err := m.UpdateNewInstanceStatus(1, processor.InstanceStatusFailed, "late duplicate"); err != nil {
		t.Fatal(err)
	}
	if q.Size() != 1 {
		t.Fatalf("duplicate terminal added plan: %d", q.Size())
	}
	select {
	case got := <-reduce.serials:
		t.Fatalf("duplicate reduce serial=%d", got)
	default:
	}
}
