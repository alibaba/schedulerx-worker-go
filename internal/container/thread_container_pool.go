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
	"runtime/debug"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var (
	_ ContainerPool = &ThreadContainerPool{}

	threadContainerPool *ThreadContainerPool
	once                sync.Once
)

func GetThreadContainerPool() *ThreadContainerPool {
	once.Do(func() {
		threadContainerPool = newTreadContainerPool()
	})
	return threadContainerPool
}

type ThreadContainerPool struct {
	*BaseContainerPool
	threadPoolMap            sync.Map // Map<Long, ExecutorService>
	enableShareContainerPool bool
	jobInstanceLockMap       sync.Map // Map<Long, Object>
	sharedThreadPool         *ants.Pool
	jobCtx                   *jobcontext.JobContext
}

func newTreadContainerPool() *ThreadContainerPool {
	gopool, _ := ants.NewPool(
		int(config.GetWorkerConfig().SharePoolSize()),
		ants.WithExpiryDuration(30*time.Second),
		ants.WithPanicHandler(func(i interface{}) {
			if r := recover(); r != nil {
				logger.Errorf("Catch panic with PanicHandler in sharedThreadPool, %v\n%s", r, debug.Stack())
			}
		}))
	return &ThreadContainerPool{
		BaseContainerPool:        NewBaseContainerPool(),
		threadPoolMap:            sync.Map{},
		jobInstanceLockMap:       sync.Map{},
		enableShareContainerPool: config.GetWorkerConfig().IsShareContainerPool(),
		sharedThreadPool:         gopool,
	}
}

func (p *ThreadContainerPool) Submit(jobId, jobInstanceId, taskId int64, container Container, consumerSize int32) (err error) {
	if p.enableShareContainerPool {
		return p.sharedThreadPool.Submit(container.Start)
	}

	pool, ok := p.threadPoolMap.Load(jobInstanceId)
	if !ok {
		pool, err = ants.NewPool(
			int(consumerSize),
			ants.WithExpiryDuration(30*time.Second),
			ants.WithPanicHandler(func(i interface{}) {
				if r := recover(); r != nil {
					logger.Errorf("Catch panic with PanicHandler in ThreadContainerPool, %v\n%s", r, debug.Stack())
				}
			}))
		if err != nil {
			return err
		}
		p.threadPoolMap.Store(jobInstanceId, pool)
	}
	return pool.(*ants.Pool).Submit(container.Start)
}

func (p *ThreadContainerPool) DestroyByInstance(jobInstanceId int64) bool {
	if val, ok := p.threadPoolMap.LoadAndDelete(jobInstanceId); ok {
		val.(*ants.Pool).Release()
	}
	return true
}

func (p *ThreadContainerPool) GetContext() *jobcontext.JobContext {
	return p.jobCtx
}

func (p *ThreadContainerPool) SetContext(jobContext *jobcontext.JobContext) {
	p.jobCtx = jobContext
}

func (p *ThreadContainerPool) GetInstanceLock(jobInstanceId int64) interface{} {
	placeholder := new(interface{})
	val, _ := p.jobInstanceLockMap.LoadOrStore(jobInstanceId, placeholder)
	return val
}

func (p *ThreadContainerPool) ReleaseInstanceLock(jobInstanceId int64) {
	p.jobInstanceLockMap.Delete(jobInstanceId)
}

func (p *ThreadContainerPool) RemoveContext() {
	p.jobCtx = nil
}

func (p *ThreadContainerPool) GetSharedThreadPool() *ants.Pool {
	return p.sharedThreadPool
}
