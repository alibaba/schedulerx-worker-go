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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ taskmaster.TaskMaster = &StandaloneTaskMaster{}

type StandaloneTaskMaster struct {
	*TaskMaster
	currentSelection      string // workerName
	actorCtx              actor.Context
	tasks                 *tasks.TaskMap
	connpool              pool.ConnPool
	taskMasterPoolCleaner func(int64)
}

var (
	startCounter int32
	stopCounter  int32
	counterLock  sync.Mutex
)

func NewStandaloneTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) taskmaster.TaskMaster {
	var (
		connpool              = pool.GetConnPool()
		taskMasterPool        = masterpool.GetTaskMasterPool()
		taskMasterPoolCleaner = func(jobInstanceId int64) {
			if taskMaster := taskMasterPool.Get(jobInstanceId); taskMaster != nil {
				taskMaster.Stop()
				taskMasterPool.Remove(jobInstanceId)
			}
		}
	)

	standaloneTaskMaster := &StandaloneTaskMaster{
		actorCtx:              actorCtx,
		taskMasterPoolCleaner: taskMasterPoolCleaner,
		tasks:                 taskMasterPool.Tasks(),
		connpool:              connpool,
		currentSelection:      actorCtx.Self().Address,
	}

	statusHandler := NewCommonUpdateInstanceStatusHandler(actorCtx, standaloneTaskMaster, jobInstanceInfo)
	if utils.IsSecondTypeJob(common.TimeType(jobInstanceInfo.GetTimeType())) {
		statusHandler = NewSecondJobUpdateInstanceStatusHandler(actorCtx, standaloneTaskMaster, jobInstanceInfo)
	}
	standaloneTaskMaster.TaskMaster = NewTaskMaster(actorCtx, jobInstanceInfo, statusHandler)

	return standaloneTaskMaster
}

func (m *StandaloneTaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	var (
		err      error
		uniqueId string
		taskId   int64

		workerId   = utils.GetWorkerId()
		workerAddr = m.GetCurrentSelection()
	)
	defer func() {
		if err != nil {
			logger.Errorf("Standalone taskMaster submitInstance failed, workerAddr=%s, uniqueId=%s, err=%s", workerAddr, uniqueId, err.Error())
			m.taskStatusMap.Store(uniqueId, taskstatus.TaskStatusFailed)
			failedReq := &schedulerx.ContainerReportTaskStatusRequest{
				JobId:         proto.Int64(jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(jobInstanceInfo.GetJobInstanceId()),
				TaskId:        proto.Int64(taskId),
				Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
				WorkerId:      proto.String(workerId),
				WorkerAddr:    proto.String(workerAddr),
				SerialNum:     proto.Int64(m.serialNum.Load()),
			}
			m.UpdateTaskStatus(failedReq)
		}
	}()

	taskId = m.AcquireTaskId()
	uniqueId = utils.GetUniqueId(jobInstanceInfo.GetJobId(), jobInstanceInfo.GetJobInstanceId(), taskId)
	req, err := m.convert2StartContainerRequest(jobInstanceInfo, taskId, "", nil, false)
	if err != nil {
		logger.Errorf("SubmitInstance failed, jobInstanceInfo=%+v, err=%s.", jobInstanceInfo, err.Error())
		m.taskStatusMap.Store(uniqueId, taskstatus.TaskStatusFailed)
		return err
	}

	// If task execution distribution is turned on for second-level tasks, the execution machine will be selected in polling.
	if config.GetWorkerConfig().IsDispatchSecondDelayStandalone() && common.TimeType(jobInstanceInfo.GetTimeType()) == common.TimeTypeSecondDelay {
		workerIdAddr := m.selectWorker()
		workerInfo := strings.Split(workerIdAddr, "@")
		workerId = workerInfo[0]
		workerAddr = actorcomm.GetRealWorkerAddr(workerIdAddr)
		m.currentSelection = workerAddr
	}

	response, e := m.actorContext.RequestFuture(actorcomm.GetContainerRouterPid(m.currentSelection), req, 10*time.Second).Result()
	if e != nil {
		err = fmt.Errorf("request to containerPid failed, err=%s", e.Error())
		return err
	}
	resp, ok := response.(*schedulerx.MasterStartContainerResponse)
	if !ok {
		m.taskStatusMap.Store(uniqueId, taskstatus.TaskStatusFailed)
		err = fmt.Errorf("response is not MasterStartContainerResponse, resp=%+v", response)
		return err
	}
	if resp.GetSuccess() {
		m.taskStatusMap.Store(uniqueId, taskstatus.TaskStatusInit)
		logger.Infof("Standalone taskMaster init worker succeed, workerAddr=%s, uniqueId=%s", workerAddr, uniqueId)

		counterLock.Lock()
		startCounter++
		logger.Infof("=====standalone_task_master.startCounter=%d\n", startCounter)
		counterLock.Unlock()
		return nil
	}

	err = fmt.Errorf("start container request failed: %s", resp.GetMessage())
	return err
}

// Poll to get the executable machine
func (m *StandaloneTaskMaster) selectWorker() string {
	workers := m.GetJobInstanceInfo().GetAllWorkers()
	workersCnt := len(workers)
	idx := 0

	if workersCnt == 0 {
		return ""
	} else if serialNum := m.GetSerialNum(); serialNum > int64(workersCnt) {
		idx = int(serialNum % int64(workersCnt))
	}

	return workers[idx]
}

func (m *StandaloneTaskMaster) KillInstance(reason string) error {
	uniqueId := utils.GetUniqueIdWithoutTaskId(m.jobInstanceInfo.GetJobId(), m.jobInstanceInfo.GetJobInstanceId())
	req := &schedulerx.MasterKillContainerRequest{
		JobId:                 proto.Int64(m.jobInstanceInfo.GetJobId()),
		JobInstanceId:         proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
		MayInterruptIfRunning: proto.Bool(false),
	}

	response, err := m.actorContext.RequestFuture(actorcomm.GetContainerRouterPid(m.currentSelection), req, 10*time.Second).Result()
	if err != nil {
		return fmt.Errorf("send kill instance request exception, workerAddr=%v, uninqueId=%v, err=%s", m.currentSelection, uniqueId, err.Error())
	}
	resp, ok := response.(*schedulerx.MasterKillContainerResponse)
	if !ok {
		return fmt.Errorf("response is not MasterKillContainerResponse, resp=%+v", response)
	}
	if resp.GetSuccess() {
		logger.Infof("Standalone taskMaster kill instance succeed, workerAddr=%s, uniqueId=%s", m.currentSelection, uniqueId)
		return nil
	}

	if err = m.updateNewInstanceStatus(m.GetSerialNum(), m.jobInstanceInfo.GetJobInstanceId(), processor.InstanceStatusFailed, reason); err != nil {
		return fmt.Errorf("UpdateNewInstanceStatus failed, err=%s", err.Error())
	}
	if !m.instanceStatus.IsFinished() {
		m.lock.Lock()
		m.instanceStatus = processor.InstanceStatusFailed
		m.lock.Unlock()
	}
	return nil
}

func (m *StandaloneTaskMaster) DestroyContainerPool() {
	req := &schedulerx.MasterDestroyContainerPoolRequest{
		JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
		SerialNum:     proto.Int64(m.GetSerialNum()),
	}

	if err := m.actorContext.RequestFuture(actorcomm.GetContainerRouterPid(m.currentSelection), req, 5*time.Second).Wait(); err != nil {
		logger.Errorf("Destroy containerPool failed, err: %s", err.Error())
	}

	counterLock.Lock()
	stopCounter++
	logger.Infof("=====standalone_task_master.stopCounter=%d\n", stopCounter)
	counterLock.Unlock()
}

func (m *StandaloneTaskMaster) CheckProcessor() error {
	// TODO Implement me
	return nil
}

func (m *StandaloneTaskMaster) GetCurrentSelection() string {
	return m.currentSelection
}
