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

import "github.com/alibaba/schedulerx-worker-go/internal/constants"

type MapTaskXAttrs struct {
	// ConsumerSize is the maximum number of threads triggered for execution on a single machine during a single run, default value is 5
	ConsumerSize int32 `json:"consumerSize"`
	// DispatcherSize is the number of threads used for distributing child tasks，default value is 5
	DispatcherSize int32 `json:"dispatcherSize"`
	// TaskMaxAttempt is the number of retries for a Failed child task
	TaskMaxAttempt int32 `json:"taskMaxAttempt"`
	// TaskAttemptInterval is the interval between retries for a Failed child task
	TaskAttemptInterval int32 `json:"taskAttemptInterval"`
	// Child task distribution mode (push/pull）
	TaskDispatchMode string `json:"taskDispatchMode"`

	//==== Exclusive to pull model ======
	// PageSize is the number of child tasks Pulled per single machine per request，default value is 5
	PageSize int32 `json:"pageSize"`
	// QueueSize is the cache size for the child task queue on a single machine，default value is 10
	QueueSize int32 `json:"queueSize"`
	// GlobalConsumerSize is the global concurrent consumption of child tasks
	GlobalConsumerSize int32 `json:"globalConsumerSize"`
}

func NewMapTaskXAttrs() *MapTaskXAttrs {
	return &MapTaskXAttrs{
		ConsumerSize:       constants.DefaultXAttrsConsumerSize,
		DispatcherSize:     constants.DefaultXAttrsDispatcherSize,
		TaskDispatchMode:   string(TaskDispatchModePush),
		PageSize:           constants.DefaultXAttrsPageSize,
		QueueSize:          constants.DefaultXAttrsQueueSize,
		GlobalConsumerSize: constants.DefaultXAttrsGlobalConsumerSize,
	}
}

func (m MapTaskXAttrs) GetConsumerSize() int32 {
	return m.ConsumerSize
}

func (m MapTaskXAttrs) GetDispatcherSize() int32 {
	return m.DispatcherSize
}

func (m MapTaskXAttrs) GetTaskMaxAttempt() int32 {
	return m.TaskMaxAttempt
}

func (m MapTaskXAttrs) GetTaskAttemptInterval() int32 {
	return m.TaskAttemptInterval
}

func (m MapTaskXAttrs) GetTaskDispatchMode() string {
	return m.TaskDispatchMode
}

func (m MapTaskXAttrs) GetPageSize() int32 {
	return m.PageSize
}

func (m MapTaskXAttrs) GetQueueSize() int32 {
	return m.QueueSize
}

func (m MapTaskXAttrs) GetGlobalConsumerSize() int32 {
	return m.GlobalConsumerSize
}
