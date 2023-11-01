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

package tasks

import (
	"sync"

	"github.com/alibaba/schedulerx-worker-go/processor"
)

var (
	taskMap *TaskMap
	once    sync.Once
)

type TaskMap struct {
	tasks sync.Map // map[string]processor.Processor
}

func GetTaskMap() *TaskMap {
	once.Do(func() {
		taskMap = &TaskMap{tasks: sync.Map{}}
	})
	return taskMap
}

func (tl *TaskMap) Register(name string, task processor.Processor) {
	tl.tasks.Store(name, task)
}

func (tl *TaskMap) Find(name string) (processor.Processor, bool) {
	task, ok := tl.tasks.Load(name)
	if ok && task != nil {
		return task.(processor.Processor), ok
	}
	return nil, false
}
