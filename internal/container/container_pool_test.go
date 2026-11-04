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

package container

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

type testContainer struct{ start func() }

func (c *testContainer) Start() { c.start() }
func (c *testContainer) Kill()  {}

func submitContainer(t *testing.T, pool *ThreadContainerPool, instance, task int64, limit int32, start func()) {
	t.Helper()
	c := &testContainer{start: start}
	pool.Put(utils.GetUniqueId(1, instance, task), c)
	if err := pool.Submit(1, instance, task, c, limit); err != nil {
		t.Fatal(err)
	}
}

func receiveTask(t *testing.T, started <-chan int64) int64 {
	t.Helper()
	select {
	case id := <-started:
		return id
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a container to start")
		return 0
	}
}

func TestConsumersAreIndependentByInstance(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 6)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	for _, instance := range []int64{1, 2} {
		instance := instance
		for task := int64(1); task <= 3; task++ {
			submitContainer(t, pool, instance, task, 2, func() { started <- instance; <-release })
		}
	}
	counts := map[int64]int{}
	for i := 0; i < 4; i++ {
		counts[receiveTask(t, started)]++
	}
	if counts[1] != 2 || counts[2] != 2 {
		t.Fatalf("want two consumers in each instance, got %v", counts)
	}
	select {
	case instance := <-started:
		t.Fatalf("instance %d exceeded its configured consumers", instance)
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		receiveTask(t, started)
	}
}

func TestRemoveCancelsQueuedContainer(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 3)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	submitContainer(t, pool, 1, 1, 1, func() { started <- 1; <-release })
	receiveTask(t, started)
	submitContainer(t, pool, 1, 2, 1, func() { started <- 2 })
	submitContainer(t, pool, 1, 3, 1, func() { started <- 3 })
	uniqueID := utils.GetUniqueId(1, 1, 2)
	pool.Remove(uniqueID, pool.Get(uniqueID))
	once.Do(func() { close(release) })
	if id := receiveTask(t, started); id != 3 {
		t.Fatalf("removed queued container must not start, got task %d", id)
	}
}

func TestDestroyDiscardsQueueAndSharesBudgetWithNewGeneration(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 4)
	oldRelease, newRelease := make(chan struct{}), make(chan struct{})
	var oldOnce, newOnce sync.Once
	t.Cleanup(func() { oldOnce.Do(func() { close(oldRelease) }); newOnce.Do(func() { close(newRelease) }) })
	submitContainer(t, pool, 1, 1, 1, func() { started <- 1; <-oldRelease })
	receiveTask(t, started)
	submitContainer(t, pool, 1, 2, 1, func() { started <- 2 })
	pool.DestroyByInstance(1)
	if pool.Contain(utils.GetUniqueId(1, 1, 2)) {
		t.Fatal("destroyed instance retained queued work")
	}
	// Raising the next generation's quota to two leaves one available slot,
	// because the retired generation still owns one until it really exits.
	for task := int64(3); task <= 4; task++ {
		task := task
		submitContainer(t, pool, 1, task, 2, func() { started <- task; <-newRelease })
	}
	if id := receiveTask(t, started); id != 3 {
		t.Fatalf("expected first new task, got %d", id)
	}
	select {
	case id := <-started:
		t.Fatalf("retired execution was not counted; extra task %d started", id)
	case <-time.After(50 * time.Millisecond):
	}
	oldOnce.Do(func() { close(oldRelease) })
	if id := receiveTask(t, started); id != 4 {
		t.Fatalf("released old slot did not run latest queue: %d", id)
	}
	newOnce.Do(func() { close(newRelease) })
	select {
	case id := <-started:
		t.Fatalf("destroyed queue executed task %d", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestConsumerContinuesAfterContainerPanic(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 1)
	submitContainer(t, pool, 1, 1, 1, func() { panic("processor failure") })
	submitContainer(t, pool, 1, 2, 1, func() { started <- 2 })
	if id := receiveTask(t, started); id != 2 {
		t.Fatalf("consumer stopped after a panic, got task %d", id)
	}
}

func TestOldCycleCompletionDoesNotCancelReplacementTask(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 3)
	oldRelease, newRelease := make(chan struct{}), make(chan struct{})
	oldDone := make(chan struct{})
	var oldOnce, newOnce sync.Once
	t.Cleanup(func() {
		oldOnce.Do(func() { close(oldRelease) })
		newOnce.Do(func() { close(newRelease) })
	})
	uniqueID := utils.GetUniqueId(1, 1, 1)
	old := &testContainer{}
	old.start = func() {
		started <- 1
		<-oldRelease
		pool.Remove(uniqueID, old)
		close(oldDone)
	}
	pool.Put(uniqueID, old)
	if err := pool.Submit(1, 1, 1, old, 1); err != nil {
		t.Fatal(err)
	}
	receiveTask(t, started)
	pool.DestroyByInstance(1)
	submitContainer(t, pool, 1, 0, 1, func() { started <- 0; <-newRelease })
	submitContainer(t, pool, 1, 1, 1, func() { started <- 2 })
	oldOnce.Do(func() { close(oldRelease) })
	<-oldDone
	if id := receiveTask(t, started); id != 0 {
		t.Fatalf("old cleanup discarded new task: %d", id)
	}
	newOnce.Do(func() { close(newRelease) })
	if id := receiveTask(t, started); id != 2 {
		t.Fatalf("old cycle canceled replacement task: %d", id)
	}
}

func TestConcurrentSubmissionsRespectConsumerSize(t *testing.T) {
	pool := newTreadContainerPool()
	var active, peak atomic.Int32
	var submitted, completed sync.WaitGroup
	const tasks = 1000
	completed.Add(tasks)
	for task := int64(1); task <= tasks; task++ {
		task := task
		submitted.Add(1)
		go func() {
			defer submitted.Done()
			c := &testContainer{start: func() {
				defer completed.Done()
				n := active.Add(1)
				for previous := peak.Load(); n > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
			}}
			if err := pool.Submit(1, 1, task, c, 8); err != nil {
				t.Errorf("submit task %d: %v", task, err)
				completed.Done()
			}
		}()
	}
	submitted.Wait()
	done := make(chan struct{})
	go func() { completed.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued containers did not all finish")
	}
	if n := peak.Load(); n > 8 || n < 2 {
		t.Fatalf("want parallel execution capped at 8, peak=%d", n)
	}
}

func TestNonMapSubmissionKeepsExistingExecutionPath(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 8)
	release := make(chan struct{})
	defer close(release)
	for task := int64(1); task <= 8; task++ {
		task := task
		submitContainer(t, pool, 1, task, 0, func() { started <- task; <-release })
	}
	for i := 0; i < 8; i++ {
		receiveTask(t, started)
	}
}

// Cancellation has two stages: unlink pending work, then publish its terminal
// status before releasing the task identity. A second actor may push between them.
func TestQueuedCancellationRetainsIdentityUntilTerminalPublished(t *testing.T) {
	pool := newTreadContainerPool()
	started := make(chan int64, 3)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	submitContainer(t, pool, 1, 1, 1, func() { started <- 1; <-release })
	receiveTask(t, started)
	submitContainer(t, pool, 1, 2, 1, func() { started <- 2 })
	id := utils.GetUniqueId(1, 1, 2)
	canceled := pool.Get(id)
	pool.Cancel(id, canceled)
	replacement := &testContainer{start: func() { started <- 3 }}
	if err := pool.Submit(1, 1, 2, replacement, 1); err != nil {
		t.Fatal(err)
	}
	if pool.Get(id) != canceled {
		t.Fatal("queued cancellation released identity before terminal publication")
	}
	once.Do(func() { close(release) })
	select {
	case got := <-started:
		t.Fatalf("canceled or duplicate task started: %d", got)
	case <-time.After(50 * time.Millisecond):
	}
	// Models the terminal publication completing before user Kill callbacks.
	pool.Remove(id, canceled)
	if err := pool.Submit(1, 1, 2, replacement, 1); err != nil {
		t.Fatal(err)
	}
	if got := receiveTask(t, started); got != 3 {
		t.Fatalf("want replacement after cancellation, got %d", got)
	}
}
