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

package masterpool

import (
	"sync"

	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	taskMasterPool *TaskMasterPool
	once           sync.Once
)

func InitTaskMasterPool(masterPool *TaskMasterPool) {
	once.Do(func() {
		taskMasterPool = masterPool
	})
}

// GetTaskMasterPool must be executed after InitTaskMasterPool, otherwise it returns nil
func GetTaskMasterPool() *TaskMasterPool {
	return taskMasterPool
}

type TaskMasterPool struct {
	taskMasters sync.Map
	tasks       *tasks.TaskMap
}

func NewTaskMasterPool(tasks *tasks.TaskMap) *TaskMasterPool {
	return &TaskMasterPool{
		taskMasters: sync.Map{},
		tasks:       tasks,
	}
}
func (p *TaskMasterPool) Tasks() *tasks.TaskMap {
	return p.tasks
}

func (p *TaskMasterPool) Get(jobInstanceId int64) taskmaster.TaskMaster {
	val, ok := p.taskMasters.Load(jobInstanceId)
	if ok {
		return val.(taskmaster.TaskMaster)
	}
	return nil
}

func (p *TaskMasterPool) Put(jobInstanceId int64, master taskmaster.TaskMaster) {
	p.taskMasters.Store(jobInstanceId, master)
}

func (p *TaskMasterPool) Remove(jobInstanceId int64) {
	p.taskMasters.Delete(jobInstanceId)
}

func (p *TaskMasterPool) Contains(jobInstanceId int64) bool {
	_, ok := p.taskMasters.Load(jobInstanceId)
	return ok
}

func (p *TaskMasterPool) GetInstanceIds(specifiedAppGroupId int64) []int64 {
	set := utils.NewSet()
	p.taskMasters.Range(func(key, val interface{}) bool {
		if master, ok := val.(taskmaster.TaskMaster); ok {
			if master.GetJobInstanceInfo() != nil {
				if appGroupId := master.GetJobInstanceInfo().GetAppGroupId(); appGroupId == specifiedAppGroupId {
					set.Add(master.GetJobInstanceInfo().GetJobInstanceId())
					return true
				}
			}
		} else {
			logger.Infof("TaskMaster=%+v is not a valid type, expect TaskMaster", val)
		}
		return true
	})
	return set.ToInt64Slice()
}
