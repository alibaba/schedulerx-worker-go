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

	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var _ ContainerPool = &BaseContainerPool{}

type BaseContainerPool struct {
	containerMap *sync.Map // map[string]Container
}

func NewBaseContainerPool() *BaseContainerPool {
	return &BaseContainerPool{
		containerMap: new(sync.Map),
	}
}

func (b *BaseContainerPool) Contain(uniqueId string) bool {
	_, ok := b.containerMap.Load(uniqueId)
	return ok
}

func (b *BaseContainerPool) DestroyByInstance(jobInstanceId int64) bool {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) Get(uniqueId string) Container {
	ret, _ := b.containerMap.Load(uniqueId)
	return ret.(Container)
}

func (b *BaseContainerPool) GetContainerMap() *sync.Map {
	return b.containerMap
}

func (b *BaseContainerPool) GetContext() *jobcontext.JobContext {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) GetInstanceLock(jobInstanceId int64) interface{} {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) Put(uniqueId string, container Container) {
	b.containerMap.Store(uniqueId, container)
}

func (b *BaseContainerPool) ReleaseInstanceLock(jobInstanceId int64) {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) Remove(uniqueId string) {
	b.containerMap.Delete(uniqueId)
}

func (b *BaseContainerPool) RemoveContext() {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) SetContext(jobContext *jobcontext.JobContext) {
	//TODO implement me
	panic("implement me")
}

func (b *BaseContainerPool) Submit(jobId int64, jobInstanceId int64, taskId int64, container Container, consumerSize int32) error {
	//TODO implement me
	panic("implement me")
}
