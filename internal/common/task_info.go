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

type TaskInfo struct {
	jobId         int64
	jobInstanceId int64
	taskId        int64
	taskName      string
	taskBody      []byte
}

func (t *TaskInfo) JobId() int64 {
	return t.jobId
}

func (t *TaskInfo) SetJobId(jobId int64) {
	t.jobId = jobId
}

func (t *TaskInfo) JobInstanceId() int64 {
	return t.jobInstanceId
}

func (t *TaskInfo) SetJobInstanceId(jobInstanceId int64) {
	t.jobInstanceId = jobInstanceId
}

func (t *TaskInfo) TaskId() int64 {
	return t.taskId
}

func (t *TaskInfo) SetTaskId(taskId int64) {
	t.taskId = taskId
}

func (t *TaskInfo) TaskName() string {
	return t.taskName
}

func (t *TaskInfo) SetTaskName(taskName string) {
	t.taskName = taskName
}

func (t *TaskInfo) TaskBody() []byte {
	return t.taskBody
}

func (t *TaskInfo) SetTaskBody(taskBody []byte) {
	t.taskBody = taskBody
}
