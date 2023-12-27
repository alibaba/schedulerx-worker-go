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

var _ ReqHandler = &TMStatusReqHandler{}

type TMStatusReqHandler struct {
	*BaseReqHandler
}

func NewTMStatusReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32, queue *ReqQueue) (rcvr *TMStatusReqHandler) {
	rcvr = new(TMStatusReqHandler)
	rcvr.BaseReqHandler = NewBaseReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue,
		"TM-Batch-Statuses-Process", "TM-Batch-Statues-Retrieve")
	rcvr.BaseReqHandler.defaultSleepMs = 100 * time.Millisecond
	return rcvr
}

func (rcvr *TMStatusReqHandler) Process(jobInstanceId int64, reqs []interface{}, workerIdAddr string) {
	statusReqs := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(reqs))
	for _, req := range reqs {
		statusReqs = append(statusReqs, req.(*schedulerx.ContainerReportTaskStatusRequest))
	}
	if taskMaster := rcvr.taskMasterPool.Get(jobInstanceId); taskMaster != nil {
		if mapTaskMaster, ok := taskMaster.(taskmaster.MapTaskMaster); ok {
			startTime := time.Now()
			mapTaskMaster.BatchUpdateTaskStatues(statusReqs)
			logger.Infof("jobInstanceId=%d, batch update status cost:%dms, size:%d", jobInstanceId, time.Since(startTime).Milliseconds(), len(statusReqs))
		} else {
			logger.Warnf("TaskMaster get from taskMasterPool is empty, jobInstanceId:%d", jobInstanceId)
		}
	} else {
		logger.Warnf("TaskMaster get from taskMasterPool is not MapTaskMaster, jobInstanceId:%d", jobInstanceId)
	}

	rcvr.activeRunnableNum.Dec()
}
