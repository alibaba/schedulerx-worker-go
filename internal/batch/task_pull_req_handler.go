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
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
)

var _ TaskDispatchReqHandler = (*TaskPullReqHandler)(nil)

type TaskPullReqHandler struct {
	*BaseTaskDispatchReqHandler
}

func NewTaskPullReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue) TaskDispatchReqHandler {
	return &TaskPullReqHandler{
		NewBaseTaskDispatchReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
			"Schedulerx-Batch-Tasks-Pull-Thread", "Schedulerx-Batch-Tasks-Retrieve-Thread"),
	}
}

func (h *TaskPullReqHandler) Process(jobInstanceId int64, reqs []interface{}, workerIdAddr string) {
	requests := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
	for _, req := range reqs {
		requests = append(requests, req.(*schedulerx.MasterStartContainerRequest))
	}
	if h.owner != nil {
		h.owner.BatchPullTasks(requests, workerIdAddr)
	}
}

func (h *TaskPullReqHandler) Start(handler ReqHandler) error {
	return h.BaseTaskDispatchReqHandler.Start(h)
}
