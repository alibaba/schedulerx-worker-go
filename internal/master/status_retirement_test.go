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
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
)

type blockedStatusPersistence struct {
	persistence.TaskPersistence
	entered, release chan struct{}
	once             sync.Once
}

func (p *blockedStatusPersistence) UpdateTaskStatues(reqs []*schedulerx.ContainerReportTaskStatusRequest) error {
	p.once.Do(func() { close(p.entered); <-p.release })
	return p.TaskPersistence.UpdateTaskStatues(reqs)
}
func waitRetirementTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle operation did not complete")
	}
}

// Keep test support compatible with the module's Go 1.21 minimum.
func useRetirementTestDirectory(t *testing.T) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Error(err)
		}
	})
}

func TestMasterRetirementDrainsInflightSQLiteStatus(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	useRetirementTestDirectory(t) // The SQLite constructor scans cwd; keep it test-owned.
	db := persistence.GetH2MemoryPersistence()
	db.InitTable()
	for _, operation := range []string{"stop", "remove", "replace"} {
		t.Run(operation, func(t *testing.T) {
			const id = int64(1001)
			if err := db.ClearTasks(id); err != nil {
				t.Fatal(err)
			}
			defer db.ClearTasks(id)
			if err := db.CreateTask(1, id, 0, "old", nil); err != nil {
				t.Fatal(err)
			}
			oldBase, _ := attemptTestMaster(0)
			blocked := &blockedStatusPersistence{TaskPersistence: db, entered: make(chan struct{}), release: make(chan struct{})}
			queue := batch.NewReqQueue(4)
			handler := batch.NewTMStatusReqHandler(id, 1, 1, 4, queue)
			oldMap := &MapTaskMaster{TaskMaster: oldBase, taskPersistence: blocked, taskStatusReqBatchHandler: handler}
			old := &GridTaskMaster{MapTaskMaster: oldMap}
			handler.BindTaskMaster(old)
			currentBase, _ := attemptTestMaster(1)
			current := &GridTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: currentBase, taskPersistence: db}}
			pool := masterpool.GetTaskMasterPool()
			pool.Put(id, old)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			flushDone := make(chan struct{})
			retired := make(chan struct{})
			defer func() { release(); waitRetirementTest(t, flushDone); waitRetirementTest(t, retired); pool.Remove(id) }()
			queue.SubmitRequest(attemptTestRequest(proto.Int32(0)))
			go func() { defer close(flushDone); handler.AsyncHandleReqs(handler) }()
			waitRetirementTest(t, blocked.entered)
			// Model completed Grid PostFinish before old Stop and row reuse.
			if err := db.ClearTasks(id); err != nil {
				t.Fatal(err)
			}
			go func() {
				defer close(retired)
				switch operation {
				case "stop":
					old.Stop()
				case "remove":
					pool.Remove(id)
				case "replace":
					pool.Put(id, current)
				}
			}()
			select {
			case <-retired:
				t.Fatal("retirement returned while old SQL could still delete replacement rows")
			case <-time.After(30 * time.Millisecond):
			}
			release()
			waitRetirementTest(t, flushDone)
			waitRetirementTest(t, retired)
			if operation != "replace" {
				pool.Remove(id)
				pool.Put(id, current)
			}
			if err := db.CreateTask(1, id, 0, "replacement", nil); err != nil {
				t.Fatal(err)
			}
			// Late work and old PostFinish must both remain fenced after retirement.
			oldMap.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
			old.PostFinish(id)
			if got := db.CheckInstanceStatus(id); got != processor.InstanceStatusRunning {
				t.Fatalf("old status/cleanup removed replacement row: %v", got)
			}
			if pool.Get(id) != current {
				t.Fatal("retirement removed replacement Master")
			}
		})
	}
}

func TestMapFailureStatusCallbackCanStopItsMaster(t *testing.T) {
	base, _ := attemptTestMaster(0)
	stopped := make(chan struct{})
	sink := &failingStatusPersistence{}
	m := &MapTaskMaster{TaskMaster: base, taskPersistence: sink}
	base.statusHandler = &stopOnStatus{master: m, done: stopped}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
	}()
	waitRetirementTest(t, done)
	waitRetirementTest(t, stopped)
}

type failingStatusPersistence struct{ persistence.TaskPersistence }

func (*failingStatusPersistence) UpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest) error {
	return errors.New("injected persistence failure")
}

type stopOnStatus struct {
	master *MapTaskMaster
	done   chan struct{}
}

func (s *stopOnStatus) Handle(_ int64, _ processor.InstanceStatus, _ string) error {
	s.master.Stop()
	close(s.done)
	return nil
}

func TestMasterRemovalUsesSharedWrapperIdentity(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	base, _ := attemptTestMaster(0)
	inner := &MapTaskMaster{TaskMaster: base}
	wrapped := &GridTaskMaster{MapTaskMaster: inner}
	currentBase, _ := attemptTestMaster(1)
	current := &GridTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: currentBase}}
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, wrapped)
	defer pool.Remove(1001)
	// Status handlers are constructed with the inner Map, not the Grid wrapper.
	if !pool.RemoveIfOwner(1001, inner) {
		t.Fatal("inner status handler could not remove its own Grid Master")
	}
	pool.Put(1001, current)
	if pool.RemoveIfOwner(1001, inner) || pool.Get(1001) != current {
		t.Fatal("old inner owner removed replacement")
	}
}

func TestGridCleanupWaitsForWritesWithoutRetiringReusableMaster(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	useRetirementTestDirectory(t)
	db := persistence.GetH2MemoryPersistence()
	db.InitTable()
	_ = db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	if err := db.CreateTask(1, 1001, 0, "old cycle", nil); err != nil {
		t.Fatal(err)
	}
	base, _ := attemptTestMaster(0)
	blocked := &blockedStatusPersistence{TaskPersistence: db, entered: make(chan struct{}), release: make(chan struct{})}
	m := &GridTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: base, taskPersistence: blocked}}
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, m)
	defer pool.Remove(1001)
	done, cleared := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer func() { release(); waitRetirementTest(t, done); waitRetirementTest(t, cleared) }()
	go func() {
		defer close(done)
		m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
	}()
	waitRetirementTest(t, blocked.entered)
	go func() { defer close(cleared); m.PostFinish(1001) }()
	select {
	case <-cleared:
		t.Fatal("cleanup passed unfinished status SQL")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	waitRetirementTest(t, done)
	waitRetirementTest(t, cleared)
	if err := db.CreateTask(1, 1001, 0, "next cycle", nil); err != nil {
		t.Fatal(err)
	}
	m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
	if got := db.CheckInstanceStatus(1001); got != processor.InstanceStatusSucceed {
		t.Fatal("ordinary cleanup permanently retired reusable Master")
	}
}

func TestConcurrentRemoveAndReplacePreserveNewMaster(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	base, _ := attemptTestMaster(0)
	sink := new(attemptPersistenceRecorder)
	blocked := &blockedStatusPersistence{TaskPersistence: sink, entered: make(chan struct{}), release: make(chan struct{})}
	old := &MapTaskMaster{TaskMaster: base, taskPersistence: blocked}
	nextBase, _ := attemptTestMaster(1)
	next := &MapTaskMaster{TaskMaster: nextBase, taskPersistence: sink}
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	writeDone, removed, replaced := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer func() {
		release()
		waitRetirementTest(t, writeDone)
		waitRetirementTest(t, removed)
		waitRetirementTest(t, replaced)
		pool.Remove(1001)
	}()
	go func() {
		defer close(writeDone)
		old.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
	}()
	waitRetirementTest(t, blocked.entered)
	go func() { defer close(removed); pool.RemoveIfOwner(1001, old) }()
	go func() { defer close(replaced); pool.Put(1001, next) }()
	// Another instance must not wait behind this instance's SQL retirement.
	otherBase, _ := attemptTestMaster(0)
	otherBase.jobInstanceInfo.SetJobInstanceId(1002)
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		pool.Put(1002, &MapTaskMaster{TaskMaster: otherBase})
		pool.Remove(1002)
	}()
	waitRetirementTest(t, otherDone)
	release()
	waitRetirementTest(t, writeDone)
	waitRetirementTest(t, removed)
	waitRetirementTest(t, replaced)
	if pool.Get(1001) != next {
		t.Fatal("concurrent old removal deleted the new owner")
	}
}

func TestEveryRetiredCleanupPathPreservesReplacementRows(t *testing.T) {
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	useRetirementTestDirectory(t)
	db := persistence.GetH2MemoryPersistence()
	db.InitTable()
	for _, path := range []string{"grid-post-finish", "sharding-post-finish", "map-clear"} {
		t.Run(path, func(t *testing.T) {
			_ = db.ClearTasks(1001)
			defer db.ClearTasks(1001)
			oldBase, _ := attemptTestMaster(0)
			oldBase.taskIdGenerator = atomic.NewInt64(0)
			oldBase.aliveCheckWorkerSet = utils.NewConcurrentSet[string]()
			oldMap := &MapTaskMaster{TaskMaster: oldBase, taskPersistence: db, cycleCancel: func() {}}
			old := &GridTaskMaster{MapTaskMaster: oldMap}
			pool := masterpool.GetTaskMasterPool()
			pool.Put(1001, old)
			defer pool.Remove(1001)
			currentBase, _ := attemptTestMaster(1)
			current := &MapTaskMaster{TaskMaster: currentBase, taskPersistence: db}
			pool.Put(1001, current) // Retires old ownership before allowing row reuse.
			if err := db.CreateTask(1, 1001, 0, "replacement", nil); err != nil {
				t.Fatal(err)
			}
			switch path {
			case "grid-post-finish":
				old.PostFinish(1001)
			case "sharding-post-finish":
				(&ShardingTaskMaster{GridTaskMaster: old}).PostFinish(1001)
			case "map-clear":
				oldMap.Clear(oldMap)
			}
			if got := db.CheckInstanceStatus(1001); got != processor.InstanceStatusRunning {
				t.Fatalf("retired %s cleared replacement row", path)
			}
			if pool.Get(1001) != current {
				t.Fatal("retired cleanup changed replacement ownership")
			}
		})
	}
}
