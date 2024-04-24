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

package master

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/schedulerx-worker-go/config"

	"github.com/asynkron/protoactor-go/actor"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var _ taskmaster.MapTaskMaster = &GridTaskMaster{}

type GridTaskMaster struct {
	*MapTaskMaster
}

func NewGridTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) *GridTaskMaster {
	jobInstanceId := jobInstanceInfo.GetJobInstanceId()

	gridTaskMaster := &GridTaskMaster{
		NewMapTaskMaster(jobInstanceInfo, actorCtx).(*MapTaskMaster),
	}
	gridTaskMaster.taskPersistence = persistence.GetH2MemoryPersistence()
	gridTaskMaster.taskPersistence.InitTable()
	gridTaskMaster.taskStatusReqQueue = batch.NewReqQueue(config.GetWorkerConfig().QueueSize())
	gridTaskMaster.taskStatusReqBatchHandler = batch.NewTMStatusReqHandler(jobInstanceId, 1, 1, 3000, gridTaskMaster.taskStatusReqQueue)
	gridTaskMaster.taskBlockingQueue = batch.NewReqQueue(config.GetWorkerConfig().QueueSize())
	if jobInstanceInfo.GetXattrs() != "" {
		gridTaskMaster.xAttrs = new(common.MapTaskXAttrs)
		if err := json.Unmarshal([]byte(jobInstanceInfo.GetXattrs()), gridTaskMaster.xAttrs); err != nil {
			logger.Errorf("Unmarshal xAttrs failed, err=%s", err.Error())
		}
	}
	if gridTaskMaster.xAttrs != nil && gridTaskMaster.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) {
		gridTaskMaster.taskDispatchReqHandler = batch.NewTaskPullReqHandler(jobInstanceId, 1, 1, gridTaskMaster.pageSize*int32(len(jobInstanceInfo.GetAllWorkers())), gridTaskMaster.taskBlockingQueue).(*batch.TaskPullReqHandler)
	} else {
		gridTaskMaster.taskDispatchReqHandler = batch.NewTaskPushReqHandler(jobInstanceId, 1, 1, gridTaskMaster.pageSize*int32(len(jobInstanceInfo.GetAllWorkers())), gridTaskMaster.taskBlockingQueue, 3000)
	}
	return gridTaskMaster
}

// doMetricsCheck checks indicator information of the master
func (m *GridTaskMaster) doMetricsCheck() {
	// FIXME how to get metric
	//vmDetail := MetricsCollector.getMetrics()
	//if vmDetail != nil {
	//	usedMemoryPercent := vmDetail.getHeap5Usage()
	//	if usedMemoryPercent > WorkerConstants.USER_MEMORY_PERCENT_MAX {
	//		throw(NewIOException(fmt.Sprintf("%v%v%v%v%v", "used memory:", usedMemoryPercent*100, ",beyond ", WorkerConstants.USER_MEMORY_PERCENT_MAX*100, "%!")))
	//	}
	//}
}

func (m *GridTaskMaster) Map(jobCtx *jobcontext.JobContext, taskList [][]byte, taskName string) (bool, error) {
	if len(taskList) == 0 {
		errMsg := fmt.Sprintf("map taskList is empty, taskName:%v", taskName)
		logger.Warnf(errMsg)
		return false, fmt.Errorf(fmt.Sprintf("map taskList is empty, taskName:%v", taskName))
	}
	logger.Infof("map taskList, jobInstanceId=%v, taskName:%v, taskList size:%v", m.GetJobInstanceInfo().GetJobInstanceId(), taskName, len(taskList))
	counter := m.taskCounter.Add(int64(len(taskList)))
	if m.xAttrs != nil && m.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) && counter > constants.PullModeTaskSizeMax {
		logger.Errorf("jobInstanceId=%v, pullModel, task counter=%v, beyond %v", m.GetJobInstanceInfo().GetJobInstanceId(), counter, constants.PullModeTaskSizeMax)
		return false, fmt.Errorf(fmt.Sprintf("task size of pullModel can't beyond %d", constants.PullModeTaskSizeMax))
	}
	m.doMetricsCheck()

	return m.MapTaskMaster.Map(jobCtx, taskList, taskName)
}

func (m *GridTaskMaster) PostFinish(jobInstanceId int64) *processor.ProcessResult {
	postResult := m.MapTaskMaster.PostFinish(jobInstanceId)
	if err := m.taskPersistence.ClearTasks(jobInstanceId); err != nil {
		logger.Errorf("Clear tasks failed in PostFinish, err=%s", err.Error())
	}
	return postResult
}
