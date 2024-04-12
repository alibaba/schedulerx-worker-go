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

package actorcomm

import (
	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
)

var (
	sxMsgCh                  = make(chan interface{}, config.GetWorkerConfig().QueueSize())
	heartbeatMsgCh           = make(chan interface{}, config.GetWorkerConfig().QueueSize())
	taskMasterMsgCh          = make(chan interface{}, config.GetWorkerConfig().QueueSize())
	containerRouterMsgCh     = make(chan interface{}, config.GetWorkerConfig().QueueSize())
	atLeastOnceDeliveryMsgCh = make(chan interface{}, config.GetWorkerConfig().QueueSize())

	workerMapTaskRespMsgCh                = make(chan *schedulerx.WorkerMapTaskResponse, config.GetWorkerConfig().QueueSize())
	workerBatchUpdateTaskStatusRespMsgCh  = make(chan *schedulerx.WorkerBatchUpdateTaskStatusResponse, config.GetWorkerConfig().QueueSize())
	workerQueryJobInstanceStatusRespMsgCh = make(chan *schedulerx.WorkerQueryJobInstanceStatusResponse, config.GetWorkerConfig().QueueSize())
	workerClearTasksRespMsgCh             = make(chan *schedulerx.WorkerClearTasksResponse, config.GetWorkerConfig().QueueSize())
	workerBatchCreateTasksRespMsgCh       = make(chan *schedulerx.WorkerBatchCreateTasksResponse, config.GetWorkerConfig().QueueSize())
	workerPullTasksRespMsgCh              = make(chan *schedulerx.WorkerPullTasksResponse, config.GetWorkerConfig().QueueSize())
	workerReportTaskListStatusRespMsgCh   = make(chan *schedulerx.WorkerReportTaskListStatusResponse, config.GetWorkerConfig().QueueSize())
)

func SxMsgReceiver() chan interface{} {
	return sxMsgCh
}

func TaskMasterMsgReceiver() chan interface{} {
	return taskMasterMsgCh
}

func WorkerMapTaskRespMsgSender() chan *schedulerx.WorkerMapTaskResponse {
	return workerMapTaskRespMsgCh
}

func WorkerBatchUpdateTaskStatusRespMsgSender() chan *schedulerx.WorkerBatchUpdateTaskStatusResponse {
	return workerBatchUpdateTaskStatusRespMsgCh
}

func WorkerQueryJobInstanceStatusRespMsgSender() chan *schedulerx.WorkerQueryJobInstanceStatusResponse {
	return workerQueryJobInstanceStatusRespMsgCh
}

func WorkerClearTasksRespMsgSender() chan *schedulerx.WorkerClearTasksResponse {
	return workerClearTasksRespMsgCh
}

func WorkerBatchCreateTasksRespMsgSender() chan *schedulerx.WorkerBatchCreateTasksResponse {
	return workerBatchCreateTasksRespMsgCh
}

func WorkerPullTasksRespMsgSender() chan *schedulerx.WorkerPullTasksResponse {
	return workerPullTasksRespMsgCh
}

func WorkerReportTaskListStatusRespMsgSender() chan *schedulerx.WorkerReportTaskListStatusResponse {
	return workerReportTaskListStatusRespMsgCh
}

func ContainerRouterMsgReceiver() chan interface{} {
	return containerRouterMsgCh
}

func AtLeastOnceDeliveryMsgReceiver() chan interface{} {
	return atLeastOnceDeliveryMsgCh
}

func HeartbeatMsgReceiver() chan interface{} {
	return heartbeatMsgCh
}
