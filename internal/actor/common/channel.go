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

import "github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"

var (
	sxMsgCh                  = make(chan interface{}, 10000)
	heartbeatMsgCh           = make(chan interface{}, 10000)
	taskMasterMsgCh          = make(chan interface{}, 10000)
	containerRouterMsgCh     = make(chan interface{}, 10000)
	atLeastOnceDeliveryMsgCh = make(chan interface{}, 10000)

	workerMapTaskRespMsgCh                = make(chan *schedulerx.WorkerMapTaskResponse, 10000)
	workerBatchUpdateTaskStatusRespMsgCh  = make(chan *schedulerx.WorkerBatchUpdateTaskStatusResponse, 10000)
	workerQueryJobInstanceStatusRespMsgCh = make(chan *schedulerx.WorkerQueryJobInstanceStatusResponse, 10000)
	workerClearTasksRespMsgCh             = make(chan *schedulerx.WorkerClearTasksResponse, 10000)
	workerBatchCreateTasksRespMsgCh       = make(chan *schedulerx.WorkerBatchCreateTasksResponse, 10000)
	workerPullTasksRespMsgCh              = make(chan *schedulerx.WorkerPullTasksResponse, 10000)
	workerReportTaskListStatusRespMsgCh   = make(chan *schedulerx.WorkerReportTaskListStatusResponse, 10000)
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
