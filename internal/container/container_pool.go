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
	"container/list"
	"math"
	"runtime/debug"
	"sync"
	syncatomic "sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var (
	_ Pool = &ThreadContainerPool{}

	threadContainerPool *ThreadContainerPool
	once                sync.Once

	globalPool, _ = ants.NewPool(
		math.MaxInt32,
		ants.WithExpiryDuration(30*time.Second),
		ants.WithPanicHandler(func(i interface{}) {
			if r := recover(); r != nil {
				logger.Errorf("Catch panic with PanicHandler in ThreadContainerPool, %v\n%s", r, debug.Stack())
			}
		}))
)

func GetThreadContainerPool() *ThreadContainerPool {
	once.Do(func() {
		threadContainerPool = newTreadContainerPool()
	})
	return threadContainerPool
}

type ThreadContainerPool struct {
	containerMap sync.Map // map[string]Container
	jobCtx       syncatomic.Pointer[jobcontext.JobContext]
	executionMu  sync.Mutex
	executors    map[int64]*instanceExecutor
	items        map[string]*queuedContainer
}

type instanceExecutor struct {
	instanceID int64
	limit      int
	workers    int
	queue      list.List
	items      map[string]*queuedContainer
	destroyed  bool
}

type queuedContainer struct {
	uniqueID  string
	container Container
	executor  *instanceExecutor
	element   *list.Element
	admitted  bool
}

func newTreadContainerPool() *ThreadContainerPool {
	return &ThreadContainerPool{
		containerMap: sync.Map{},
	}
}

func (p *ThreadContainerPool) GetContainerMap() *sync.Map {
	return &p.containerMap
}

// Submit registers and enqueues atomically. Re-delivery while a container is
// queued or executing must not replace its registry entry or run a second copy.
func (p *ThreadContainerPool) Submit(jobId, jobInstanceId, taskId int64, c Container, consumerNum int32) error {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	if p.executors == nil {
		p.executors = make(map[int64]*instanceExecutor)
	}
	if p.items == nil {
		p.items = make(map[string]*queuedContainer)
	}
	uniqueID := utils.GetUniqueId(jobId, jobInstanceId, taskId)
	if p.items[uniqueID] != nil {
		return nil
	}
	if previous, loaded := p.containerMap.LoadOrStore(uniqueID, c); loaded && previous != c {
		return nil
	}
	executor := p.executors[jobInstanceId]
	if executor == nil {
		executor = &instanceExecutor{instanceID: jobInstanceId, limit: int(consumerNum), items: make(map[string]*queuedContainer)}
		p.executors[jobInstanceId] = executor
	}
	// A new generation reuses the instance budget while retired executions
	// still own worker slots. Its queue and task identities remain independent.
	if executor.destroyed {
		executor.destroyed = false
		executor.limit = int(consumerNum)
	}
	if executor.items == nil {
		executor.items = make(map[string]*queuedContainer)
	}
	item := &queuedContainer{uniqueID: uniqueID, container: c, executor: executor}
	p.items[uniqueID] = item
	executor.items[uniqueID] = item
	if consumerNum <= 0 {
		item.admitted = true
		executor.workers++
		if err := globalPool.Submit(func() { defer p.runnerExited(executor); p.execute(item, c) }); err != nil {
			executor.workers--
			p.removeLocked(uniqueID, c)
			return err
		}
		return nil
	}
	item.element = executor.queue.PushBack(item)
	if executor.workers < executor.limit {
		if err := p.startRunner(executor); err != nil {
			p.removeLocked(uniqueID, c)
			return err
		}
	}
	return nil
}

// The execution lock is held by callers. Starting a runner never waits for a
// consumer slot; the actor must remain able to process kill/destroy messages.
func (p *ThreadContainerPool) startRunner(executor *instanceExecutor) error {
	executor.workers++
	if err := globalPool.Submit(func() { p.run(executor) }); err != nil {
		executor.workers--
		return err
	}
	return nil
}

func (p *ThreadContainerPool) run(executor *instanceExecutor) {
	// Goexit runs defers without producing a recoverable panic. Accounting and
	// replacement must therefore happen here, not only at the normal loop exit.
	defer p.runnerExited(executor)
	for {
		p.executionMu.Lock()
		element := executor.queue.Front()
		if executor.destroyed || element == nil || executor.limit > 0 && executor.workers > executor.limit {
			p.executionMu.Unlock()
			return
		}
		item := element.Value.(*queuedContainer)
		executor.queue.Remove(element)
		item.element = nil
		item.admitted = true
		c := item.container
		p.executionMu.Unlock()
		p.execute(item, c)
	}
}

func (p *ThreadContainerPool) runnerExited(executor *instanceExecutor) {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	executor.workers--
	defer p.releaseIdleExecutor(executor)
	if !executor.destroyed && executor.queue.Len() > 0 && executor.workers < executor.limit {
		if err := p.startRunner(executor); err != nil {
			logger.Errorf("restart container consumer: %v", err)
		}
	}
}

func (p *ThreadContainerPool) execute(item *queuedContainer, c Container) {
	defer p.Remove(item.uniqueID, c)
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("Catch panic in container %s, %v\n%s", item.uniqueID, r, debug.Stack())
		}
	}()
	c.Start()
}

func (p *ThreadContainerPool) DestroyByInstance(jobInstanceId int64) bool {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	if executor := p.executors[jobInstanceId]; executor != nil {
		executor.destroyed = true
		for uniqueID, item := range executor.items {
			if p.items[uniqueID] == item {
				p.removeLocked(uniqueID, item.container)
			}
		}
		executor.queue.Init()
		executor.items = nil
		p.releaseIdleExecutor(executor)
	}
	if len(p.executors) == 0 {
		p.executors = nil
	}
	return true
}

func (p *ThreadContainerPool) Get(uniqueId string) Container {
	ret, ok := p.containerMap.Load(uniqueId)
	if !ok {
		return nil
	}
	return ret.(Container)
}

func (p *ThreadContainerPool) Put(uniqueId string, container Container) {
	p.containerMap.LoadOrStore(uniqueId, container)
}

func (p *ThreadContainerPool) Contain(uniqueId string) bool {
	_, ok := p.containerMap.Load(uniqueId)
	return ok
}

// Cancel unlinks queued work but keeps its identity reserved until the caller
// publishes its terminal status and calls Remove. Already-admitted executions
// retain their identity until execution returns; cancellation is cooperative.
// The return value indicates whether execution has already been admitted.
func (p *ThreadContainerPool) Cancel(uniqueID string, expected Container) bool {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	if item := p.items[uniqueID]; item != nil && item.container == expected {
		if item.element != nil {
			item.executor.queue.Remove(item.element)
			item.element = nil
		}
		return item.admitted
	}
	return false
}

func (p *ThreadContainerPool) Remove(uniqueId string, expected Container) {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	p.removeLocked(uniqueId, expected)
}

func (p *ThreadContainerPool) removeLocked(uniqueID string, expected Container) {
	// Match identity so a late old-cycle completion cannot cancel its replacement.
	if item := p.items[uniqueID]; item != nil && item.container == expected {
		if item.element != nil {
			item.executor.queue.Remove(item.element)
			item.element = nil
		}
		delete(item.executor.items, uniqueID)
		delete(p.items, uniqueID)
		item.container = nil
		p.releaseIdleExecutor(item.executor)
	}
	if len(p.items) == 0 {
		p.items = nil
	}
	p.containerMap.CompareAndDelete(uniqueID, expected)
}

// Called under executionMu; no execution state or large map buckets need
// survive an idle instance while waiting for the master's close notification.
func (p *ThreadContainerPool) releaseIdleExecutor(executor *instanceExecutor) {
	if executor.workers == 0 && len(executor.items) == 0 && executor.queue.Len() == 0 && p.executors[executor.instanceID] == executor {
		delete(p.executors, executor.instanceID)
		executor.items = nil
		if len(p.executors) == 0 {
			p.executors = nil
		}
	}
}

func (p *ThreadContainerPool) GetContext() *jobcontext.JobContext {
	return p.jobCtx.Load()
}

func (p *ThreadContainerPool) SetContext(jobContext *jobcontext.JobContext) {
	p.jobCtx.Store(jobContext)
}

func (p *ThreadContainerPool) RemoveContext() {
	p.jobCtx.Store(nil)
}
