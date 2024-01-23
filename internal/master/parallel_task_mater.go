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
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ taskmaster.ParallelTaskMaster = &ParallelTaskMaster{}

const batchSize = 256

// ParallelTaskMaster using persistence.ServerTaskPersistence
type ParallelTaskMaster struct {
	*MapTaskMaster
	actorCtx actor.Context
}

func NewParallelTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) *ParallelTaskMaster {
	jobInstanceId := jobInstanceInfo.GetJobInstanceId()

	parallelTaskMaster := &ParallelTaskMaster{
		actorCtx:      actorCtx,
		MapTaskMaster: NewMapTaskMaster(jobInstanceInfo, actorCtx).(*MapTaskMaster),
	}
	parallelTaskMaster.taskPersistence = persistence.NewServerTaskPersistence(jobInstanceInfo.GetGroupId())
	parallelTaskMaster.taskStatusReqQueue = batch.NewReqQueue(1024)
	parallelTaskMaster.taskStatusReqBatchHandler = batch.NewTMStatusReqHandler(jobInstanceId, 1, 1, batchSize*2*int32(len(jobInstanceInfo.GetAllWorkers())), parallelTaskMaster.taskStatusReqQueue)
	parallelTaskMaster.taskBlockingQueue = batch.NewReqQueue(batchSize * 4)
	if jobInstanceInfo.GetXattrs() != "" {
		parallelTaskMaster.xAttrs = new(common.MapTaskXAttrs)
		if err := json.Unmarshal([]byte(jobInstanceInfo.GetXattrs()), parallelTaskMaster.xAttrs); err != nil {
			logger.Errorf("Unmarshal xAttrs failed, err=%s", err.Error())
		}
	}
	if parallelTaskMaster.xAttrs != nil && parallelTaskMaster.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) {
		parallelTaskMaster.taskDispatchReqHandler = batch.NewTaskPullReqHandler(jobInstanceId, 1, 2, batchSize*int32(len(jobInstanceInfo.GetAllWorkers())), parallelTaskMaster.taskBlockingQueue)
	} else {
		curBatchSize := batchSize * len(jobInstanceInfo.GetAllWorkers())
		// FIXME implement it
		//if(isWorkerLoadRouter()) {
		//	batchSize = 2 * jobInstanceInfo.getAllWorkers().size();
		//}
		//Long dispatchDelay = parseDispatchSpeed();
		//if (dispatchDelay != null) {
		//	batchSize = 1;
		//}

		parallelTaskMaster.taskDispatchReqHandler = batch.NewTaskPushReqHandler(jobInstanceId, 1, 2, int32(curBatchSize), parallelTaskMaster.taskBlockingQueue, batchSize)
	}

	// Used to support secondary subtask retries to prevent taskId duplication
	parallelTaskMaster.taskIdGenerator = atomic.NewInt64(time.Now().Unix())
	return parallelTaskMaster
}

func (m *ParallelTaskMaster) Map(jobCtx *jobcontext.JobContext, taskList [][]byte, taskName string) (bool, error) {
	if len(taskList) == 0 {
		logger.Warnf("map taskList is empty, taskName=%s", taskName)
		return false, nil
	}

	logger.Infof("map taskList, jobInstanceId=%v, taskName=%v, taskList size=%v", m.jobInstanceInfo.GetJobInstanceId(), taskName, len(taskList))
	counter := m.taskCounter.Add(int64(len(taskList)))
	defaultTaskMaxSize := constants.ParallelTaskListSizeMaxDefault
	if discovery.GetGroupManager().IsAdvancedVersion(m.jobInstanceInfo.GetGroupId()) {
		defaultTaskMaxSize = constants.ParallelTaskListSizeMaxAdvanced
	}
	parallelTaskMaxSize := config.GetWorkerConfig().WorkerParallelTaskMaxSize()
	if parallelTaskMaxSize == 0 {
		parallelTaskMaxSize = int32(defaultTaskMaxSize)
	}
	if counter > int64(parallelTaskMaxSize) {
		return false, fmt.Errorf("jobInstanceId=%v, task counter=%v, task list size beyond %v", m.jobInstanceInfo.GetJobInstanceId(), counter, parallelTaskMaxSize)
	}

	return m.MapTaskMaster.Map(jobCtx, taskList, taskName)
}

func (m *ParallelTaskMaster) RetryTasks(taskEntities []*schedulerx.RetryTaskEntity) {
	// update tasks' status to INIT
	taskIdList := make([]int64, 0, len(taskEntities))
	for _, taskEntity := range taskEntities {
		if utils.IsRootTask(taskEntity.GetTaskName()) {
			logger.Warnf("root task can't retry")
		} else {
			taskIdList = append(taskIdList, taskEntity.GetTaskId())
		}
	}
	req := &schedulerx.WorkerReportTaskListStatusRequest{
		JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
		TaskId:        taskIdList,
		Status:        proto.Int32(int32(taskstatus.TaskStatusInit)),
	}

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}

	// Wait 30 seconds
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender():
		if resp.GetSuccess() {
			if !m.IsInited() {
				// If it has not been initialized, re-initialize it.
				m.startBatchHandler()
				m.init()
				for _, taskEntity := range taskEntities {
					taskName := taskEntity.GetTaskName()
					if counter, ok := m.taskProgressMap.LoadOrStore(taskName, common.NewTaskProgressCounter(taskName)); ok {
						counter.(*common.TaskProgressCounter).IncrementOneTotal()
					}
				}
			} else {
				for _, taskEntity := range taskEntities {
					taskName := taskEntity.GetTaskName()
					workerAddr := taskEntity.GetWorkerAddr()
					oldStatus := taskEntity.GetOldStatus()
					if taskProgressCounter, ok := m.taskProgressMap.Load(taskName); ok {
						switch taskstatus.TaskStatus(oldStatus) {
						case taskstatus.TaskStatusSucceed:
							taskProgressCounter.(*common.TaskProgressCounter).DecrementSuccess()
						case taskstatus.TaskStatusFailed:
							taskProgressCounter.(*common.TaskProgressCounter).DecrementFailed()
						}
					}
					if workerProgressCounter, ok := m.workerProgressMap.Load(workerAddr); ok {
						switch taskstatus.TaskStatus(oldStatus) {
						case taskstatus.TaskStatusSucceed:
							workerProgressCounter.(*common.WorkerProgressCounter).DecrementSuccess()
						case taskstatus.TaskStatusFailed:
							workerProgressCounter.(*common.WorkerProgressCounter).DecrementFailed()
						}
					}
				}
			}
		} else {
			logger.Errorf("RetryTasks in ParallelTaskMaster timeout, jobInstanceId=%d, errMsg=%s", m.jobInstanceInfo.GetJobInstanceId(), resp.GetMessage())
			//TODO 发送失败应该尝试另一个server
		}
	case <-timer.C:
		logger.Errorf("RetryTasks in ParallelTaskMaster timeout, jobInstanceId=%d", m.jobInstanceInfo.GetJobInstanceId())
	}
}
