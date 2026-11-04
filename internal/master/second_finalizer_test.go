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
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"google.golang.org/protobuf/proto"
	"sync"
	"testing"
	"time"
)

func TestLifecycleR5RepeatedSecondTerminalSchedulesOnlyOneNextCycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	m := newR4ProductionSecondGrid(t)
	defer m.Stop()
	m.Init()
	m.jobInstanceInfo.SetAllWorkers(nil)
	q := GetTimeScheduler().timeQueue
	q.Clear()
	defer q.Clear()
	if err := m.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := m.UpdateNewInstanceStatus(0, processor.InstanceStatusFailed, "delayed terminal"); err != nil {
		t.Fatal(err)
	}
	if n := q.Size(); n != 1 {
		t.Fatalf("same serial scheduled %d next-cycle entries; want 1", n)
	}
}

type r5Reduce struct {
	processor.MapReduceJobProcessor
	serials chan int64
}

func (r *r5Reduce) RunReduceIfFail(*jobcontext.JobContext) bool { return true }
func (r *r5Reduce) Reduce(c *jobcontext.JobContext) (*processor.ProcessResult, error) {
	r.serials <- c.SerialNum()
	return processor.NewProcessResult(processor.WithSucceed()), nil
}

func TestLifecycleR5OldSecondFinalizerDoesNotReduceNewCycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	GetTimeScheduler().once.Do(func() {})
	useRetirementTestDirectory(t)
	m := newR4ProductionSecondGrid(t)
	defer m.Stop()
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.jobInstanceInfo.SetContent(`{"jobName":"r5reduce"}`)
	r := &r5Reduce{serials: make(chan int64, 1)}
	masterpool.GetTaskMasterPool().Tasks().Register("r5reduce", r)
	h := m.statusHandler.(*SecondJobUpdateInstanceStatusHandler)
	// Pause the old terminal continuation at its real progress snapshot lock,
	// after its entry generation check but before its public PostFinish call.
	h.progressMu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); h.triggerNextCycle("old", 0, processor.InstanceStatusSucceed) }()
	r4WaitStack(t, "(*SecondJobUpdateInstanceStatusHandler).triggerNextCycle")
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	h.progressMu.Unlock()
	waitRetirementTest(t, done)
	select {
	case serial := <-r.serials:
		t.Fatalf("old terminal ran user Reduce for new serial %d", serial)
	default:
	}
}

func TestLifecycleR5NormalPullAfterCycleRestart(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.xAttrs.GlobalConsumerSize = 5
	defer m.Stop()
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	ctx := new(jobcontext.JobContext)
	ctx.SetSerialNum(1)
	ctx.SetAttempt(0)
	if _, err := m.Map(ctx, [][]byte{[]byte("body")}, "sub"); err != nil {
		t.Fatal(err)
	}
	reqs := m.SyncPullTasks(1, "worker@127.0.0.1:1234")
	if len(reqs) != 1 || reqs[0].GetSerialNum() != 1 {
		t.Fatalf("normal worker received %#v", reqs)
	}
}

func TestLifecycleR5ConcurrentWorkerSelection(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0, "push")
	defer m.Stop()
	m.jobInstanceInfo.SetAllWorkers([]string{"a@127.0.0.1:1234", "b@127.0.0.1:1235"})
	m.taskProgressMap.Store("sub", common.NewTaskProgressCounter("sub"))
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1), TaskName: proto.String("sub"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.BatchHandlePulledProgress([]*schedulerx.MasterStartContainerRequest{req}, "")
			}
		}()
	}
	wg.Wait()
}
