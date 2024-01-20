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

package taskstatus

type TaskStatus int32

const (
	TaskStatusUnknown TaskStatus = 0
	TaskStatusInit    TaskStatus = 1
	TaskStatusPulled  TaskStatus = 2
	TaskStatusRunning TaskStatus = 3
	TaskStatusSucceed TaskStatus = 4
	TaskStatusFailed  TaskStatus = 5
)

var taskStatusDesc = map[TaskStatus]string{
	0: "未知",
	1: "初始化",
	2: "已拉取",
	3: "运行",
	4: "成功",
	5: "失败",
}

func Convert2TaskStatus(val int32) (TaskStatus, bool) {
	status := TaskStatus(val)
	_, ok := taskStatusDesc[status]
	if ok {
		return status, true
	}
	return TaskStatus(0), false
}

func (status TaskStatus) Descriptor() string {
	return taskStatusDesc[status]
}

func (status TaskStatus) IsFinished() bool {
	return status == TaskStatusSucceed || status == TaskStatusFailed
}
