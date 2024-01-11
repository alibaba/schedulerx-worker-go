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

type Container interface {
	Start()
	Kill()
}

type ContainerPool interface {
	Contain(uniqueId string) bool
	DestroyByInstance(jobInstanceId int64) bool
	Get(uniqueId string) Container
	GetContainerMap() *sync.Map // map[string]Container
	GetContext() *jobcontext.JobContext
	GetInstanceLock(jobInstanceId int64) interface{}
	Put(uniqueId string, container Container)
	ReleaseInstanceLock(jobInstanceId int64)
	Remove(uniqueId string)
	RemoveContext()
	SetContext(jobContext *jobcontext.JobContext)
	Submit(jobId int64, jobInstanceId int64, taskId int64, container Container, consumerSize int32) error
}
