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

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ TaskDispatchReqHandler = (*TaskPushReqHandler)(nil)

type TaskPushReqHandler struct {
	*BaseTaskDispatchReqHandler
	dispatchSize int32
}

func NewTaskPushReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue, dispatchSize int32) TaskDispatchReqHandler {
	h := new(TaskPushReqHandler)
	h.BaseTaskDispatchReqHandler = NewBaseTaskDispatchReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
		"Schedulerx-Batch-Tasks-Dispatch-Thread", "Schedulerx-Batch-Tasks-Retrieve-Thread")
	h.defaultSleepMs = 100 * time.Millisecond
	h.dispatchSize = dispatchSize
	return h
}

func (h *TaskPushReqHandler) Process(jobInstanceId int64, reqs []interface{}, workerAddr string) {
	requests := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
	for _, req := range reqs {
		requests = append(requests, req.(*schedulerx.MasterStartContainerRequest))
	}
	owner := h.owner
	err := globalPool.Submit(func() {
		defer h.activeRunnableNum.Dec()
		if owner != nil {
			owner.BatchDispatchTasks(requests, "")
		}
	})
	if err != nil {
		h.activeRunnableNum.Dec()
		logger.Errorf("TaskPushReqHandler submit failed: %s", err)
	}
}

func (h *TaskPushReqHandler) Start(handler ReqHandler) error {
	return h.BaseTaskDispatchReqHandler.Start(h)
}
