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

package batch

import (
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

// ContainerStatusReqHandler batch report container task status to task master
type ContainerStatusReqHandler struct {
	*BaseReqHandler
	taskMasterAkkaPath string
}

func NewContainerStatusReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue, taskMasterAkkaPath string) *ContainerStatusReqHandler {
	return &ContainerStatusReqHandler{
		BaseReqHandler: NewBaseReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
			"Schedulerx-Container-Batch-Statuses-Process-Thread-", "Schedulerx-Container-Batch-Statues-Retrieve-Thread-"),
		taskMasterAkkaPath: taskMasterAkkaPath,
	}
}

func (h *ContainerStatusReqHandler) GetTaskMasterAkkaPath() string {
	return h.taskMasterAkkaPath
}

func (h *ContainerStatusReqHandler) Process(jobInstanceId int64, requests []interface{}, workerAddr string) {
	reqs := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(requests))
	for _, req := range requests {
		reqs = append(reqs, req.(*schedulerx.ContainerReportTaskStatusRequest))
	}
	if len(reqs) == 0 {
		logger.Warnf("Process ContainerStatusReqHandler, but reqs is empty, jobInstanceId=%d, workerAddr=%s", jobInstanceId, workerAddr)
		return
	}

	err := globalPool.Submit(func() {
		taskStatuses := make([]*schedulerx.TaskStatusInfo, 0, len(reqs))
		// some attrs are duplicated in all reqs, for example: workAddr, workerId, jobId, jobInstanceId, taskMasterPath
		// get first one used for all reqs.
		taskStatusRequest := reqs[0]
		for _, req := range reqs {
			taskStatusInfo := &schedulerx.TaskStatusInfo{
				TaskId: proto.Int64(req.GetTaskId()),
				Status: proto.Int32(req.GetStatus()),
			}
			if req.GetTaskName() != "" {
				taskStatusInfo.TaskName = proto.String(req.GetTaskName())
			}
			if req.GetResult() != "" {
				taskStatusInfo.Result = proto.String(req.GetResult())
			}
			if req.GetProgress() != "" {
				taskStatusInfo.Progress = proto.String(req.GetProgress())
			}
			if req.GetTraceId() != "" {
				taskStatusInfo.TraceId = proto.String(req.GetTraceId())
			}
			taskStatuses = append(taskStatuses, taskStatusInfo)
		}
		req := &schedulerx.ContainerBatchReportTaskStatuesRequest{
			JobId:              taskStatusRequest.JobId,
			JobInstanceId:      taskStatusRequest.JobInstanceId,
			TaskStatues:        taskStatuses,
			TaskMasterAkkaPath: taskStatusRequest.InstanceMasterActorPath,
			WorkerAddr:         taskStatusRequest.WorkerAddr,
			WorkerId:           taskStatusRequest.WorkerId,
			SerialNum:          taskStatusRequest.SerialNum,
		}
		actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: req,
		}

		h.activeRunnableNum.Dec()
	})
	if err != nil {
		logger.Errorf("Process ContainerStatusReqHandler failed, submit to batchProcessSvc failed, err=%s", err.Error())
	}
}
