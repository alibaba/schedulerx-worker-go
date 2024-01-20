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
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ TaskDispatchReqHandler = &TaskPushReqHandler{}

type TaskPushReqHandler struct {
	*BaseTaskDispatchReqHandler
	dispatchSize int32
}

func NewTaskPushReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue, dispatchSize int32) TaskDispatchReqHandler {
	h := new(TaskPushReqHandler)
	h.BaseTaskDispatchReqHandler = NewBaseTaskDispatchReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
		"Schedulerx-Batch-Tasks-Dispatch-Thread", "Schedulerx-Batch-Tasks-Retrieve-Thread")
	h.BaseReqHandler.defaultSleepMs = 100 * time.Millisecond
	h.dispatchSize = dispatchSize
	return h
}

func (h *TaskPushReqHandler) Process(jobInstanceId int64, reqs []interface{}, workerAddr string) {
	masterStartContainerReqs := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
	for _, req := range reqs {
		masterStartContainerReqs = append(masterStartContainerReqs, req.(*schedulerx.MasterStartContainerRequest))
	}

	err := h.batchProcessSvc.Submit(func() {
		if taskMaster := h.taskMasterPool.Get(jobInstanceId); taskMaster != nil {
			if mapTaskMaster, ok := taskMaster.(taskmaster.MapTaskMaster); ok {
				startTime := time.Now()
				mapTaskMaster.BatchDispatchTasks(masterStartContainerReqs, "")
				logger.Infof("jobInstance=%d, batch dispatch cost:%dms, dispatchSize:%d, size:%d", jobInstanceId, time.Since(startTime).Milliseconds(), h.dispatchSize, len(reqs))
			} else {
				logger.Warnf("TaskMaster get from taskMasterPool is empty, jobInstanceId:%d", jobInstanceId)
			}
		} else {
			logger.Warnf("TaskMaster get from taskMasterPool is not MapTaskMaster, jobInstanceId:%d", jobInstanceId)
		}

		h.activeRunnableNum.Dec()
	})
	if err != nil {
		logger.Errorf("TaskPushReqHandler process failed, submit to batchProcessSvc failed, err=%s", err.Error())
	}
}

func (h *TaskPushReqHandler) Start(handler ReqHandler) error {
	return h.BaseTaskDispatchReqHandler.Start(h)
}
