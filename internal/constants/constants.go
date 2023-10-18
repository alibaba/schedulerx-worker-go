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

package constants

import "time"

const (
	// Transport
	TransportHeaderSize = 4

	// DefaultXAttrs job default conf
	DefaultXAttrsPageSize           = 5
	DefaultXAttrsConsumerSize       = 5
	DefaultXAttrsQueueSize          = 10
	DefaultXAttrsDispatcherSize     = 5
	DefaultXAttrsGlobalConsumerSize = 1000

	InstanceResultSizeMax = 1000

	PullModeTaskSizeMax = 10000

	SecondDelayStandaloneDispatch        = "second_delay.standalone.dispatch"
	SecondDelayStandaloneDispatchDefault = false

	MapMasterPageSize       = "map.master.page.size"
	MapMasterQueueSize      = "map.master.queue.size"
	MapMasterDispatcherSize = "map.master.dispatcher.size"

	MapMasterPageSizeDefault       = 100
	MapMasterQueueSizeDefault      = 10000
	MapMasterDispatcherSizeDefault = 5
	MapMasterDispatcherSizeMax     = 200
	TaskBodySizeMaxDefault         = 65536

	UserSpacePercentMax = 0.9

	MapMasterStatusCheckInterval        = "map.master.status.check.interval"
	MapMasterStatusCheckIntervalDefault = 3 * time.Second

	MapTaskRootName = "MAP_TASK_ROOT"
	ReduceTaskName  = "REDUCE_TASK"

	SharedPoolSizeDefault = 64

	ConsumerNumDefault = 64

	ParallelTaskListSizeMaxAdvanced = 1000
	ParallelTaskListSizeMax         = 300

	TimeFormat = "2006-01-02 15:04:05"

	WorkerMapPageSizeDefault = 1000
)

type AppVersion int32

const (
	Basic    AppVersion = iota + 1 // Alibaba Cloud Foundation Edition
	Advanced                       // Alibaba Cloud Premium Edition
)
