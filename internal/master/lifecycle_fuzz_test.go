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
	"runtime"
	"sync"
	"testing"

	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"google.golang.org/protobuf/proto"
)

type goexitPersistence struct{ persistence.TaskPersistence }

func (*goexitPersistence) UpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest) error {
	runtime.Goexit()
	return nil
}
func TestStatusGoexitReleasesOwnerGateAndActivity(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.taskPersistence = &goexitPersistence{}
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	defer pool.Remove(1001)
	m.taskStatusReqQueue.SubmitRequest(attemptTestRequest(proto.Int32(0)))
	done := make(chan struct{})
	go func() { defer close(done); m.taskStatusReqBatchHandler.AsyncHandleReqs(m.taskStatusReqBatchHandler) }()
	waitRetirementTest(t, done)
	if m.taskStatusReqBatchHandler.IsActive() {
		t.Fatal("Goexit leaked status activity")
	}
	stopped := make(chan struct{})
	go func() { defer close(stopped); m.Stop() }()
	waitRetirementTest(t, stopped)
}

// Randomized operation order complements the channel-controlled interleaving
// regressions. The oracle is the actual replacement SQLite row, not gate flags.
func FuzzRetiredOwnerCannotChangeReplacementRows(f *testing.F) {
	for _, seed := range [][]byte{{0, 1, 2, 3, 4}, {4, 3, 2, 1, 0}, {1, 1, 4, 0, 3}, {2, 4, 2, 4}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, order []byte) {
		if len(order) == 0 || len(order) > 16 {
			return
		}
		useRetirementTestDirectory(t)
		old := newRetirementGrid(t, 0)
		old.GetJobInstanceInfo().SetAllWorkers(nil)
		db := old.taskPersistence
		db.ClearTasks(1001)
		defer db.ClearTasks(1001)
		current := newRetirementGrid(t, 0)
		pool := masterpool.NewTaskMasterPool(tasks.GetTaskMap())
		pool.Put(1001, old)
		defer func() { old.Stop(); current.Stop(); pool.Remove(1001) }()
		ready := make(chan struct{})
		var wg sync.WaitGroup
		for _, operation := range order {
			operation := operation
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-ready
				switch operation % 5 {
				case 0:
					old.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
				case 1:
					old.Map(nil, [][]byte{[]byte("old")}, "sub")
				case 2:
					old.Clear(old.TaskMaster)
				case 3:
					old.Stop()
				case 4:
					old.GetJobInstanceProgress()
				}
			}()
		}
		close(ready)
		pool.Put(1001, current)
		if err := db.ClearTasks(1001); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTask(1, 1001, 0, "replacement", nil); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if db.CheckInstanceStatus(1001) != processor.InstanceStatusRunning {
			t.Fatal("old lifecycle work removed replacement row")
		}
	})
}
