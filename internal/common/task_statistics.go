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

type TaskStatistics struct {
	distinctInstanceCount int64
	taskCount             int64
}

func NewTaskStatistics() (rcvr *TaskStatistics) {
	rcvr = &TaskStatistics{}
	return
}

func (rcvr *TaskStatistics) GetDistinctInstanceCount() int64 {
	return rcvr.distinctInstanceCount
}

func (rcvr *TaskStatistics) GetTaskCount() int64 {
	return rcvr.taskCount
}

func (rcvr *TaskStatistics) SetDistinctInstanceCount(distinctInstanceCount int64) {
	rcvr.distinctInstanceCount = distinctInstanceCount
}

func (rcvr *TaskStatistics) SetTaskCount(taskCount int64) {
	rcvr.taskCount = taskCount
}
