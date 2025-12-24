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
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"

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
	containerMap       *sync.Map // map[string]Container
	jobInstanceLockMap *sync.Map // map[int64]*JobInstanceLock
	jobCtx             *jobcontext.JobContext
}

func newTreadContainerPool() *ThreadContainerPool {
	return &ThreadContainerPool{
		containerMap:       new(sync.Map),
		jobInstanceLockMap: new(sync.Map),
	}
}

func (p *ThreadContainerPool) GetContainerMap() *sync.Map {
	return p.containerMap
}

func (p *ThreadContainerPool) Submit(jobId, jobInstanceId, taskId int64, container Container) (err error) {
	// in go, it has been simplified into a global pool
	return globalPool.Submit(container.Start)
}

func (p *ThreadContainerPool) DestroyByInstance(jobInstanceId int64) bool {
	// in go, it has been simplified into a global pool and does not need to be destroyed
	return true
}

func (p *ThreadContainerPool) Get(uniqueId string) Container {
	ret, _ := p.containerMap.Load(uniqueId)
	return ret.(Container)
}

func (p *ThreadContainerPool) Put(uniqueId string, container Container) {
	p.containerMap.Store(uniqueId, container)
}

func (p *ThreadContainerPool) Contain(uniqueId string) bool {
	_, ok := p.containerMap.Load(uniqueId)
	return ok
}

func (p *ThreadContainerPool) Remove(uniqueId string) {
	p.containerMap.Delete(uniqueId)
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
	return p.jobCtx
}

func (p *ThreadContainerPool) SetContext(jobContext *jobcontext.JobContext) {
	p.jobCtx = jobContext
}

func (p *ThreadContainerPool) RemoveContext() {
	p.jobCtx = nil
}
