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
	"math"
	"runtime/debug"
	"sync"
	syncatomic "sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"

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
	containerMap       sync.Map // map[string]Container
	jobInstanceLockMap sync.Map // map[int64]*JobInstanceLock
	jobCtx             syncatomic.Pointer[jobcontext.JobContext]
	executionMu        sync.Mutex
	executors          map[int64]*instanceExecutor
	pending            map[string]*queuedContainer
}

type instanceExecutor struct {
	limit     int
	workers   int
	queue     []*queuedContainer
	destroyed bool
}

type queuedContainer struct {
	uniqueID  string
	container Container
	canceled  bool
}

func newTreadContainerPool() *ThreadContainerPool {
	return &ThreadContainerPool{
		containerMap:       sync.Map{},
		jobInstanceLockMap: sync.Map{},
	}
}

func (p *ThreadContainerPool) GetContainerMap() *sync.Map {
	return &p.containerMap
}

func (p *ThreadContainerPool) Submit(jobId, jobInstanceId, taskId int64, c Container, consumerNum int32) error {
	if consumerNum <= 0 {
		return globalPool.Submit(c.Start)
	}
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	if p.executors == nil {
		p.executors = make(map[int64]*instanceExecutor)
		p.pending = make(map[string]*queuedContainer)
	}
	executor := p.executors[jobInstanceId]
	if executor == nil {
		executor = &instanceExecutor{limit: int(consumerNum)}
		p.executors[jobInstanceId] = executor
	}
	item := &queuedContainer{uniqueID: utils.GetUniqueId(jobId, jobInstanceId, taskId), container: c}
	executor.queue = append(executor.queue, item)
	p.pending[item.uniqueID] = item
	if executor.workers < executor.limit {
		executor.workers++
		if err := globalPool.Submit(func() { p.run(executor) }); err != nil {
			executor.workers--
			executor.queue[len(executor.queue)-1] = nil
			executor.queue = executor.queue[:len(executor.queue)-1]
			delete(p.pending, item.uniqueID)
			return err
		}
	}
	return nil
}

// Queue without blocking the container actor: it must still acknowledge push
// batches and process kill/destroy messages when all consumers are occupied.
// Only consumerNum runners per job instance enter Container.Start at a time.
func (p *ThreadContainerPool) run(executor *instanceExecutor) {
	for {
		p.executionMu.Lock()
		if executor.destroyed || len(executor.queue) == 0 {
			executor.workers--
			p.executionMu.Unlock()
			return
		}
		item := executor.queue[0]
		executor.queue[0] = nil
		executor.queue = executor.queue[1:]
		if p.pending[item.uniqueID] == item {
			delete(p.pending, item.uniqueID)
		}
		canceled := item.canceled
		p.executionMu.Unlock()
		if !canceled {
			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Errorf("Catch panic in container %s, %v\n%s", item.uniqueID, r, debug.Stack())
					}
				}()
				item.container.Start()
			}()
		}
	}
}

func (p *ThreadContainerPool) DestroyByInstance(jobInstanceId int64) bool {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	if executor := p.executors[jobInstanceId]; executor != nil {
		delete(p.executors, jobInstanceId)
		executor.destroyed = true
		for _, item := range executor.queue {
			item.canceled = true
			if p.pending[item.uniqueID] == item {
				delete(p.pending, item.uniqueID)
				p.containerMap.CompareAndDelete(item.uniqueID, item.container)
			}
		}
		executor.queue = nil
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
	p.containerMap.Store(uniqueId, container)
}

func (p *ThreadContainerPool) Contain(uniqueId string) bool {
	_, ok := p.containerMap.Load(uniqueId)
	return ok
}

func (p *ThreadContainerPool) Remove(uniqueId string, expected Container) {
	p.executionMu.Lock()
	defer p.executionMu.Unlock()
	// A delayed completion from an older second-delay cycle must not remove
	// a replacement container that reuses the same task ID.
	if item := p.pending[uniqueId]; item != nil && item.container == expected {
		item.canceled = true
		delete(p.pending, uniqueId)
	}
	p.containerMap.CompareAndDelete(uniqueId, expected)
}

func (p *ThreadContainerPool) GetInstanceLock(jobInstanceId, serialNum int64) *JobInstanceLock {
	lock, loaded := p.jobInstanceLockMap.LoadOrStore(jobInstanceId, &JobInstanceLock{
		Mutex:     new(sync.Mutex),
		SerialNum: atomic.NewInt64(serialNum),
	})
	jobInstanceLock := lock.(*JobInstanceLock)
	if loaded && serialNum > 0 {
		jobInstanceLock.SerialNum.Store(serialNum)
	}
	return jobInstanceLock
}

func (p *ThreadContainerPool) ReleaseInstanceLock(jobInstanceId int64) {
	p.jobInstanceLockMap.Delete(jobInstanceId)
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
