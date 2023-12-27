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

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

// ContainerStatusReqHandler batch report container task status to task master
type ContainerStatusReqHandler struct {
	*BaseReqHandler
	taskMasterAkkaPath       string
	enableShareContainerPool bool
}

func NewContainerStatusReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue, taskMasterAkkaPath string) *ContainerStatusReqHandler {
	return &ContainerStatusReqHandler{
		BaseReqHandler: NewBaseReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
			"Schedulerx-Container-Batch-Statuses-Process-Thread-", "Schedulerx-Container-Batch-Statues-Retrieve-Thread-"),
		taskMasterAkkaPath:       taskMasterAkkaPath,
		enableShareContainerPool: config.GetWorkerConfig().IsShareContainerPool(),
	}
}

func (h *ContainerStatusReqHandler) GetTaskMasterAkkaPath() string {
	return h.taskMasterAkkaPath
}

type Pair struct {
	jobInstanceId int64
	serialNum     int64
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

	err := h.batchProcessSvc.Submit(func() {
		if h.enableShareContainerPool {
			// FIXME fix import cycle
			//// 如果开启共享线程池，statues 可能会有多个 jobInstanceId，需要先 split 成不同的 list
			//taskStatusRequestMap := make(map[*Pair][]*schedulerx.ContainerReportTaskStatusRequest)
			//for _, req := range reqs {
			//	pair := &Pair{
			//		jobInstanceId: req.GetJobInstanceId(),
			//		serialNum:     req.GetSerialNum(),
			//	}
			//	if _, ok := taskStatusRequestMap[pair]; ok {
			//		taskStatusRequestMap[pair] = append(taskStatusRequestMap[pair], req)
			//	} else {
			//		taskStatusRequestMap[pair] = []*schedulerx.ContainerReportTaskStatusRequest{req}
			//	}
			//}
			//
			//// 针对不同的 jobInstanceId，构造 batchStatusRequests
			//for pair, reqs := range taskStatusRequestMap {
			//	var (
			//		instanceMasterActorPath string
			//		finishCount             = 0
			//		taskStatuses            = make([]*schedulerx.TaskStatusInfo, 0, len(reqs))
			//	)
			//	for _, req := range reqs {
			//		finishCount := 0
			//		instanceMasterActorPath = req.GetInstanceMasterActorPath()
			//		taskStatusInfo := &schedulerx.TaskStatusInfo{
			//			TaskId: proto.Int64(req.GetTaskId()),
			//			Status: proto.Int32(req.GetStatus()),
			//		}
			//		if req.GetTaskName() != "" {
			//			taskStatusInfo.TaskName = proto.String(req.GetTaskName())
			//		}
			//		if req.GetResult() != "" {
			//			taskStatusInfo.Result = proto.String(req.GetResult())
			//		}
			//		if req.GetProgress() != "" {
			//			taskStatusInfo.Progress = proto.String(req.GetProgress())
			//		}
			//		if req.GetTraceId() != "" {
			//			taskStatusInfo.TraceId = proto.String(req.GetTraceId())
			//		}
			//		if common.TaskStatus(req.GetStatus()).IsFinished() {
			//			finishCount++
			//		}
			//		taskStatuses = append(taskStatuses, taskStatusInfo)
			//	}
			//
			//	if instanceMasterActorPath != "" {
			//		taskStatusRequest := reqs[0]
			//		sharedThreadPool := container.GetThreadContainerPool().GetSharedThreadPool()
			//		if finishCount > 0 && sharedThreadPool != nil {
			//			// 可用大小可用线程数 + 线程数等量缓冲区
			//			// TODO implement it
			//			//metrics := common.Metrics{}
			//			//availableSize := float64(sharedThreadPool.Cap() - sharedThreadPool.Running()) +
			//			//	(math.Sqrt(sharedThreadPool.Cap()) - float64(sharedThreadPool.Free())) + float64(finishCount)
			//			//metrics.setSharePoolAvailableSize(availableSize)
			//		}
			//		req := &schedulerx.ContainerBatchReportTaskStatuesRequest{
			//			GetJobId:              taskStatusRequest.GetJobId,
			//			JobInstanceId:      proto.Int64(pair.jobInstanceId),
			//			TaskStatues:        taskStatuses,
			//			TaskMasterAkkaPath: proto.String(instanceMasterActorPath),
			//			WorkerAddr:         taskStatusRequest.WorkerAddr,
			//			WorkerId:           taskStatusRequest.WorkerId,
			//			SerialNum:          proto.Int64(pair.serialNum),
			//		}
			//		actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			//			Msg: req,
			//		}
			//		logger.Infof("jobInstanceId=%v, serialNum=%v, batch report status=%v to task master, size:%v",
			//			pair.jobInstanceId, pair.serialNum, taskStatusRequest.GetStatus(), len(taskStatuses))
			//	} else {
			//		logger.Errorf("instanceMasterActorPath is null, jobInstanceId=%d", jobInstanceId)
			//	}
			//}
		} else {
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
		}

		h.activeRunnableNum.Dec()
	})
	if err != nil {
		logger.Errorf("Process ContainerStatusReqHandler failed, submit to batchProcessSvc failed, err=%s", err.Error())
	}
}
