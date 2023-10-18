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

// TaskDispatchMode is the task execution mode
type TaskDispatchMode string

const (
	TaskDispatchModePush TaskDispatchMode = "push"
	TaskDispatchModePull TaskDispatchMode = "pull"
)

var taskDispatchModeDesc = map[TaskDispatchMode]string{
	"push": "推模型",
	"pull": "拉模型",
}

func (mode TaskDispatchMode) Description() string {
	return taskDispatchModeDesc[mode]
}
