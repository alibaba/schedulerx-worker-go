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

package persistence

import (
	"fmt"
	"sync"
	"time"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var (
	_ TaskPersistence = &H2Persistence{}

	h2pOnce       sync.Once
	h2Persistence *H2Persistence
)

func GetH2Persistence() *H2Persistence {
	h2pOnce.Do(func() {
		h2Persistence = NewH2Persistence()
	})
	return h2Persistence
}

// H2Persistence is a singleton, so that only executes initTable on the first initialization
type H2Persistence struct {
	h2CP    *H2ConnectionPool
	taskDao *TaskDao
	inited  bool
	lock    sync.RWMutex
}

func NewH2Persistence() *H2Persistence {
	return &H2Persistence{
		inited: false,
	}
}

func (rcvr *H2Persistence) BatchUpdateTaskStatus(jobInstanceId int64, status taskstatus.TaskStatus, workerId string, workerAddr string) int64 {
	var (
		affectCnt int64
		err       error
	)
	for i := 0; i < 3; i++ {
		if config.GetWorkerConfig().IsMapMasterFailover() {
			affectCnt, err = rcvr.taskDao.BatchUpdateStatus2(jobInstanceId, int(status), workerId, workerAddr)
		} else {
			affectCnt, err = rcvr.taskDao.BatchDeleteTasks2(jobInstanceId, workerId, workerAddr)
		}

		if err != nil {
			logger.Errorf("batchUpdateTaskStatus error, try after 1000ms, err=%s", err.Error())
			time.Sleep(1000 * time.Millisecond)
			continue
		}
	}
	return affectCnt
}

func (rcvr *H2Persistence) CheckInstanceStatus(jobInstanceId int64) processor.InstanceStatus {
	instanceStatus := processor.InstanceStatusSucceed
	existed, err := rcvr.taskDao.Exist(jobInstanceId)
	if err != nil {
		logger.Errorf("CheckInstanceStatus from H2Persistence failed, err=%s", err.Error())
		return instanceStatus
	}
	if existed {
		instanceStatus = processor.InstanceStatusRunning
	}

	return instanceStatus
}

func (rcvr *H2Persistence) ClearTasks(jobInstanceId int64) error {
	_, err := rcvr.taskDao.DeleteByJobInstanceId(jobInstanceId)
	return err
}

func (rcvr *H2Persistence) convert2TaskInfo(taskSnapshot *TaskSnapshot) *common.TaskInfo {
	taskInfo := new(common.TaskInfo)
	taskInfo.SetTaskId(taskSnapshot.GetTaskId())
	taskInfo.SetTaskName(taskSnapshot.GetTaskName())
	taskInfo.SetTaskBody(taskSnapshot.GetTaskBody())
	taskInfo.SetJobId(taskSnapshot.GetJobId())
	taskInfo.SetJobInstanceId(taskSnapshot.GetJobInstanceId())
	return taskInfo
}

func (rcvr *H2Persistence) convert2TaskSnapshot(taskInfo *common.TaskInfo) *TaskSnapshot {
	taskSnapshot := NewTaskSnapshot()
	taskSnapshot.SetJobId(taskInfo.JobId())
	taskSnapshot.SetJobInstanceId(taskInfo.JobInstanceId())
	taskSnapshot.SetTaskId(taskInfo.TaskId())
	taskSnapshot.SetTaskName(taskInfo.TaskName())
	taskSnapshot.SetTaskBody(taskInfo.TaskBody())
	return taskSnapshot
}

func (rcvr *H2Persistence) CreateTask(jobId, jobInstanceId, taskId int64, taskName string, taskBody []byte) error {
	return rcvr.taskDao.Insert(jobId, jobInstanceId, taskId, taskName, taskBody)
}

func (rcvr *H2Persistence) CreateTasks(containers []*schedulerx.MasterStartContainerRequest, workerId, workerAddr string) error {
	succeed := false
	for i := 0; i < 3; i++ {
		affectCnt, err := rcvr.taskDao.BatchInsert(containers, workerId, workerAddr)
		if err != nil {
			logger.Warnf("batch insert tasks error, try after 1000ms, err=%s", err.Error())
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		logger.Infof("batch insert tasks succeed, affectCnt=%d", affectCnt)
		succeed = true
		break
	}
	if !succeed {
		return fmt.Errorf(fmt.Sprintf("batch insert tasks error, workerId=%s, workerAddr=%s", workerId, workerAddr))
	}
	return nil
}

// GetDistinctInstanceIds get the remaining terminated but undeleted instances in H2
func (rcvr *H2Persistence) GetDistinctInstanceIds() []int64 {
	ret, err := rcvr.taskDao.GetDistinctInstanceIds()
	if err != nil {
		logger.Errorf("GetDistinctInstanceIds failed, err=%s", err.Error())
		return nil
	}
	return ret
}

// GetTaskStatistics get H2 tasks summary statistics
func (rcvr *H2Persistence) GetTaskStatistics() *common.TaskStatistics {
	ret, err := rcvr.taskDao.GetTaskStatistics()
	if err != nil {
		logger.Errorf("GetDistinctInstanceIds failed, err=%s", err.Error())
		return nil
	}
	return ret
}

func (rcvr *H2Persistence) InitTable() {
	rcvr.lock.Lock()
	defer rcvr.lock.Unlock()
	if !rcvr.inited {
		rcvr.taskDao.DropTable()
		rcvr.taskDao.CreateTable()
		rcvr.inited = true
	}
}

func (rcvr *H2Persistence) IsInited() bool {
	rcvr.lock.RLock()
	defer rcvr.lock.RUnlock()
	return rcvr.inited
}

func (rcvr *H2Persistence) Pull(jobInstanceId int64, pageSize int32) ([]*common.TaskInfo, error) {
	var taskInfoList []*common.TaskInfo
	taskSnapshots, err := rcvr.taskDao.QueryTaskList(jobInstanceId, int(taskstatus.TaskStatusInit), pageSize)
	if err != nil {
		return nil, err
	}
	if len(taskSnapshots) > 0 {
		var taskIdList []int64
		for _, taskSnapshot := range taskSnapshots {
			taskIdList = append(taskIdList, taskSnapshot.GetTaskId())
			taskInfoList = append(taskInfoList, rcvr.convert2TaskInfo(taskSnapshot))
		}
		// FIXME if failed 3 times?
		for i := 0; i < 3; i++ {
			_, err := rcvr.taskDao.BatchUpdateStatus(jobInstanceId, taskIdList, int(taskstatus.TaskStatusPulled))
			if err != nil {
				logger.Warnf("batchUpdateStatus error, try after 1000ms, err=%s", err.Error())
				time.Sleep(1000 * time.Millisecond)
				continue
			}
			break
		}
	}
	return taskInfoList, nil
}

// UpdateTaskStatues .
/*
 * !!!Attention!!! For Grid/Batch tasks, this method invoked only when finish statuses updated.
 * In order to reduce h2 size, this method will delete all finish tasks;
 * taskStatusInfos list of task Status
 */
func (rcvr *H2Persistence) UpdateTaskStatues(taskStatusInfos []*schedulerx.ContainerReportTaskStatusRequest) error {
	if len(taskStatusInfos) == 0 {
		return nil
	}

	// update task statues always batch by same job instance
	jobInstanceId := taskStatusInfos[0].GetJobInstanceId()
	var taskIds []int64
	for _, taskStatusInfo := range taskStatusInfos {
		taskStatus := taskstatus.TaskStatus(taskStatusInfo.GetStatus())
		if taskStatus.IsFinished() {
			taskIds = append(taskIds, taskStatusInfo.GetTaskId())
		}
	}
	_, err := rcvr.taskDao.BatchDeleteTasks(jobInstanceId, taskIds)
	return err
}

func (rcvr *H2Persistence) UpdateTaskStatus(jobInstanceId int64, taskIds []int64, status taskstatus.TaskStatus, workerId, workerAddr string) (int64, error) {
	var (
		affectCnt int64
		err       error
	)
	if len(taskIds) == 0 {
		return 0, nil
	}
	affectCnt, err = rcvr.taskDao.UpdateStatus2(jobInstanceId, taskIds, int(status), workerId, workerAddr)
	if err != nil {
		logger.Errorf("JobInstanceId=%d, updateTaskStatus failed, err=%s", jobInstanceId, err.Error())
	}
	return affectCnt, nil
}
