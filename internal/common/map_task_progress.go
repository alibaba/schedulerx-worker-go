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

package common

type MapTaskProgress struct {
	TaskProgress   []*TaskProgressCounter   `json:"taskProgress"`
	WorkerProgress []*WorkerProgressCounter `json:"workerProgress"`
}

func NewMapTaskProgress() *MapTaskProgress {
	return &MapTaskProgress{
		TaskProgress:   make([]*TaskProgressCounter, 0, 10),
		WorkerProgress: make([]*WorkerProgressCounter, 0, 10),
	}
}

func (m *MapTaskProgress) GetTaskProgress() []*TaskProgressCounter {
	return m.TaskProgress
}

func (m *MapTaskProgress) SetTaskProgress(taskProgress []*TaskProgressCounter) {
	m.TaskProgress = taskProgress
}

func (m *MapTaskProgress) GetWorkerProgress() []*WorkerProgressCounter {
	return m.WorkerProgress
}

func (m *MapTaskProgress) SetWorkerProgress(workerProgress []*WorkerProgressCounter) {
	m.WorkerProgress = workerProgress
}
