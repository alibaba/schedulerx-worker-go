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
	"context"
	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
	"sync"
	"testing"
	"time"
)

// Adversarial tests: these assert the required behavior, and fail on HEAD.
func TestSyncPullDoesNotReturnRejectedRequests(t *testing.T) {
	for _, kind := range []string{"retired-same-generation", "old-explicit-serial"} {
		t.Run(kind, func(t *testing.T) {
			base, _ := attemptTestMaster(0)
			m := &MapTaskMaster{TaskMaster: base, xAttrs: &common.MapTaskXAttrs{GlobalConsumerSize: 100}}
			m.taskBlockingQueue = batch.NewReqQueue(4)
			m.taskDispatchReqHandler = batch.NewTaskPullReqHandler(1001, 1, 1, 4, m.taskBlockingQueue)
			m.taskDispatchReqHandler.(interface {
				BindTaskMaster(taskmaster.MapTaskMaster)
			}).BindTaskMaster(m)
			req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
			m.taskBlockingQueue.SubmitRequest(req)
			if kind == "retired-same-generation" {
				m.RetireStatusWrites()
			} else {
				m.AcquireSerialNum()
			}
			got := m.SyncPullTasks(4, "worker@127.0.0.1:1234")
			if len(got) != 0 {
				t.Fatalf("rejected request returned to Pull caller: count=%d attempt=%d serial=%d ownerSerial=%d retired=%v", len(got), got[0].GetAttempt(), got[0].GetSerialNum(), m.GetSerialNum(), m.statusWritesRetired)
			}
		})
	}
}

type adversarialCheckPersistence struct {
	persistence.TaskPersistence
	entered, release chan struct{}
}

func (p *adversarialCheckPersistence) CheckInstanceStatus(int64) processor.InstanceStatus {
	close(p.entered)
	<-p.release
	return processor.InstanceStatusSucceed
}

type adversarialSerialStatus struct{ calls chan int64 }

func (h *adversarialSerialStatus) Handle(serial int64, status processor.InstanceStatus, _ string) error {
	if status.IsFinished() {
		h.calls <- serial
	}
	return nil
}
func TestCancelledStatusQueryCannotCompleteNewSerial(t *testing.T) {
	config.InitWorkerConfig(config.NewWorkerConfig(config.WithMapMasterStatusCheckInterval(time.Millisecond)))
	base, _ := attemptTestMaster(0)
	base.instanceStatus = processor.InstanceStatusRunning
	sink := &adversarialSerialStatus{make(chan int64, 1)}
	base.statusHandler = sink
	p := &adversarialCheckPersistence{entered: make(chan struct{}), release: make(chan struct{})}
	m := &MapTaskMaster{TaskMaster: base, taskPersistence: p}
	m.taskBlockingQueue = batch.NewReqQueue(4)
	m.taskDispatchReqHandler = batch.NewTaskPullReqHandler(1001, 1, 1, 4, m.taskBlockingQueue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.checkInstanceStatus(ctx) }()
	waitRetirementTest(t, p.entered)
	cancel()             // old cycle is closed while its persistence read waits
	m.AcquireSerialNum() // next cycle has started; old read still in flight
	close(p.release)
	waitRetirementTest(t, done)
	select {
	case serial := <-sink.calls:
		t.Fatalf("old canceled cycle completed new serial=%d", serial)
	default:
	}
}

type adversarialHeldClear struct {
	*GridTaskMaster
	entered, release chan struct{}
}

func (h *adversarialHeldClear) DestroyContainerPool() { close(h.entered); <-h.release }

func TestClearSerializesConcurrentCycleRestart(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	held := &adversarialHeldClear{GridTaskMaster: m, entered: make(chan struct{}), release: make(chan struct{})}
	cleared := make(chan struct{})
	restarted := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(held.release) }) }
	go func() { defer close(cleared); m.Clear(held) }()
	waitRetirementTest(t, held.entered)
	defer func() { release(); waitRetirementTest(t, cleared); waitRetirementTest(t, restarted); m.Stop() }()
	go func() {
		defer close(restarted)
		m.AcquireSerialNum()
		if err := m.startBatchHandler(); err != nil {
			return
		}
		req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("new-cycle"), Attempt: proto.Int32(0), SerialNum: proto.Int64(1)}
		m.BatchPullTasks([]*schedulerx.MasterStartContainerRequest{req}, "worker@127.0.0.1:1234")
	}()
	select {
	case <-restarted:
		t.Fatal("new cycle restarted before previous Clear completed")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	waitRetirementTest(t, cleared)
	waitRetirementTest(t, restarted)
	n, err := db.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if err != nil || n != 1 {
		t.Fatalf("new cycle not preserved: rows=%d err=%v", n, err)
	}
}
