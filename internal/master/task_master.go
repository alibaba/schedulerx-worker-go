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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ taskmaster.TaskMaster = &TaskMaster{}

type TaskMaster struct {
	actorContext             actor.Context               `json:"actorContext,omitempty"`
	instanceStatus           processor.InstanceStatus    `json:"instanceStatus,omitempty"` // WARNING: not concurrency-safe
	taskStatusMap            sync.Map                    `json:"taskStatusMap"`            // key:string, val:TaskStatus
	taskIdGenerator          *atomic.Int64               `json:"taskIdGenerator"`          // WARNING: not concurrency-safe
	localWorkIdAddr          string                      `json:"localWorkIdAddr,omitempty"`
	localContainerRouterPath string                      `json:"localContainerRouterPath,omitempty"`
	localTaskRouterPath      string                      `json:"localTaskRouterPath,omitempty"`
	localInstanceRouterPath  string                      `json:"localInstanceRouterPath,omitempty"`
	jobInstanceInfo          *common.JobInstanceInfo     `json:"jobInstanceInfo,omitempty"`
	jobInstanceProgress      string                      `json:"jobInstanceProgress,omitempty"`
	statusHandler            UpdateInstanceStatusHandler `json:"statusHandler,omitempty"`
	killed                   bool                        `json:"killed,omitempty"`              // WARNING: not concurrency-safe
	inited                   bool                        `json:"inited,omitempty"`              // WARNING: not concurrency-safe
	aliveCheckWorkerSet      *utils.ConcurrentSet        `json:"aliveCheckWorkerSet,omitempty"` // string
	serverDiscovery          discovery.ServiceDiscover   `json:"serverDiscovery"`
	serialNum                *atomic.Int64               `json:"serialNum,omitempty"`          // 秒级任务使用，当前循环次数
	existInvalidWorker       bool                        `json:"existInvalidWorker,omitempty"` // 是否存在失效Worker WARNING: not concurrency-safe

	lock sync.RWMutex
}

func NewTaskMaster(actorCtx actor.Context, jobInstanceInfo *common.JobInstanceInfo, statusHandler UpdateInstanceStatusHandler) *TaskMaster {
	workerIdAddr := fmt.Sprintf("%s@%s", utils.GetWorkerId(), actorCtx.ActorSystem().Address())
	taskMaster := &TaskMaster{
		inited:              true,
		instanceStatus:      processor.InstanceStatusRunning,
		taskStatusMap:       sync.Map{},
		taskIdGenerator:     atomic.NewInt64(-1),
		aliveCheckWorkerSet: utils.NewConcurrentSet(),
		jobInstanceInfo:     jobInstanceInfo,
		actorContext:        actorCtx,
		localWorkIdAddr:     workerIdAddr,
		localTaskRouterPath: workerIdAddr,
		serialNum:           atomic.NewInt64(0),
		statusHandler:       statusHandler,
	}
	return taskMaster
}

func (m *TaskMaster) Init() {
	//	m.lock.Lock()
	//	defer m.lock.Unlock()
	if !m.inited {
		m.inited = true
	}
}

func (m *TaskMaster) GetActorContext() actor.Context {
	return m.actorContext
}

func (m *TaskMaster) GetLocalWorkerIdAddr() string {
	return m.localWorkIdAddr
}

func (m *TaskMaster) GetLocalJobInstanceRouterPath() string {
	return m.localInstanceRouterPath
}

func (m *TaskMaster) GetLocalContainerRouterPath() string {
	return m.localContainerRouterPath
}

func (m *TaskMaster) GetLocalTaskRouterPath() string {
	return m.localTaskRouterPath
}

func (m *TaskMaster) IsJobInstanceFinished() bool {
	isFinished := true
	m.taskStatusMap.Range(func(key, value interface{}) bool {
		status := value.(taskstatus.TaskStatus)
		if !status.IsFinished() {
			isFinished = false
			return false
		}
		return true
	})
	return isFinished
}

func (m *TaskMaster) UpdateTaskStatus(req *schedulerx.ContainerReportTaskStatusRequest) error {
	var (
		jobId         = req.GetJobId()
		jobInstanceId = req.GetJobInstanceId()
		taskId        = req.GetTaskId()
	)
	taskStatus, ok := taskstatus.Convert2TaskStatus(req.GetStatus())
	if !ok {
		return fmt.Errorf("Invalid taskstatus: %d get from ContainerReportTaskStatusRequest: %+v ", req.GetStatus(), req)
	}
	uniqueId := utils.GetUniqueId(jobId, jobInstanceId, taskId)
	m.taskStatusMap.Store(uniqueId, taskStatus)

	newStatus := processor.InstanceStatusUnknown
	if utils.SyncMapLen(&m.taskStatusMap) > 0 {
		if !m.IsJobInstanceFinished() {
			newStatus = processor.InstanceStatusRunning
		} else {
			newStatus = processor.InstanceStatusSucceed
			// 只要有一个子任务状态为 Failed，则返回 Failed
			if newStatus != processor.InstanceStatusFailed {
				m.taskStatusMap.Range(func(key, val interface{}) bool {
					if val.(taskstatus.TaskStatus) == taskstatus.TaskStatusFailed {
						newStatus = processor.InstanceStatusFailed
						return false
					}
					return true
				})
			}
		}
	}

	m.jobInstanceProgress = req.GetProgress()
	if err := m.updateNewInstanceStatus(req.GetSerialNum(), jobInstanceId, newStatus, req.GetResult()); err != nil {
		return fmt.Errorf("updateNewInstanceStatus failed, err=%s", err.Error())
	}
	return nil
}

func (m *TaskMaster) updateNewInstanceStatus(serialNum int64, jobInstanceId int64, newStatus processor.InstanceStatus, result string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	fmt.Printf("serialNum=%d, jobInstanceId=%d, status=%s\n", serialNum, jobInstanceId, newStatus)
	if err := m.statusHandler.Handle(serialNum, newStatus, result); err != nil {
		return fmt.Errorf("update status failed, err=%s", err.Error())
	}
	return nil
}

// BatchUpdateTaskStatus
// TODO: MapTaskMaster may override this method do really batch process
func (m *TaskMaster) BatchUpdateTaskStatus(taskMaster taskmaster.TaskMaster, req *schedulerx.ContainerBatchReportTaskStatuesRequest) error {
	for _, status := range req.GetTaskStatues() {
		containerReportTaskStatusReq := &schedulerx.ContainerReportTaskStatusRequest{
			JobId:         proto.Int64(req.GetJobId()),
			JobInstanceId: proto.Int64(req.GetJobInstanceId()),
			TaskId:        proto.Int64(status.GetTaskId()),
			WorkerAddr:    proto.String(req.GetWorkerAddr()),
			WorkerId:      proto.String(req.GetWorkerId()),
			Status:        proto.Int32(status.GetStatus()),
		}

		if result := status.GetResult(); result != "" {
			containerReportTaskStatusReq.Result = proto.String(result)
		}
		if taskName := status.GetTaskName(); taskName != "" {
			containerReportTaskStatusReq.TaskName = proto.String(taskName)
		}
		if progress := status.GetProgress(); progress != "" {
			containerReportTaskStatusReq.Progress = proto.String(progress)
		}
		if serialNum := req.GetSerialNum(); serialNum != 0 {
			containerReportTaskStatusReq.SerialNum = proto.Int64(serialNum)
		}

		switch taskMaster.(type) {
		case *BroadcastTaskMaster:
			if err := taskMaster.(*BroadcastTaskMaster).UpdateTaskStatus(containerReportTaskStatusReq); err != nil {
				if err != nil {
					return fmt.Errorf("BroadcastTaskMaster UpdateTaskStatus failed, err=%s ", err.Error())
				}
			}
		case *BatchTaskMaster:
			if err := taskMaster.(*BatchTaskMaster).UpdateTaskStatus(containerReportTaskStatusReq); err != nil {
				if err != nil {
					return fmt.Errorf("BatchTaskMaster UpdateTaskStatus failed, err=%s ", err.Error())
				}
			}
		case *GridTaskMaster:
			if err := taskMaster.(*GridTaskMaster).UpdateTaskStatus(containerReportTaskStatusReq); err != nil {
				if err != nil {
					return fmt.Errorf("GridTaskMaster UpdateTaskStatus failed, err=%s ", err.Error())
				}
			}
		case *MapTaskMaster:
			if err := taskMaster.(*MapTaskMaster).UpdateTaskStatus(containerReportTaskStatusReq); err != nil {
				if err != nil {
					return fmt.Errorf("MapTaskMaster UpdateTaskStatus failed, err=%s ", err.Error())
				}
			}
		default:
			if err := m.UpdateTaskStatus(containerReportTaskStatusReq); err != nil {
				if err != nil {
					return fmt.Errorf("UpdateTaskStatus failed, err=%s ", err.Error())
				}
			}
		}
	}
	return nil
}

func (m *TaskMaster) KillInstance(reason string) error {
	//	m.lock.Lock()
	m.killed = true
	//	m.lock.Unlock()

	GetTimeScheduler().remove(m.jobInstanceInfo.GetJobInstanceId())
	return nil
}

func (m *TaskMaster) DestroyContainerPool() {
	// TODO Implement me
	return
}

func (m *TaskMaster) KillTask(uniqueId, workerId, workerAddr string) {
	// TODO Implement me
	return
}

func (m *TaskMaster) RetryTasks(taskEntities []schedulerx.RetryTaskEntity) {
	// TODO Implement me
	return
}

func (m *TaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	// TODO Implement me
	return nil
}

func (m *TaskMaster) AcquireTaskId() int64 {
	return m.taskIdGenerator.Inc()
}

func (m *TaskMaster) GetJobInstanceProgress() (string, error) {
	return m.jobInstanceProgress, nil
}

func (m *TaskMaster) UpdateNewInstanceStatus(serialNum int64, newStatus processor.InstanceStatus, result string) error {
	return m.updateNewInstanceStatus(serialNum, m.jobInstanceInfo.GetJobInstanceId(), newStatus, result)
}

func (m *TaskMaster) Stop() {
	// do nothing
}

func (m *TaskMaster) Clear(taskMaster taskmaster.TaskMaster) {
	m.taskStatusMap = sync.Map{} // Clear the sync.Map by reassigning it to an empty sync.Map
	m.taskIdGenerator.Store(0)
	m.instanceStatus = processor.InstanceStatusRunning
	m.aliveCheckWorkerSet.Clear()

	if !config.GetWorkerConfig().IsShareContainerPool() {
		taskMaster.DestroyContainerPool()
	}
}

func (m *TaskMaster) PostFinish(jobInstanceId int64) *processor.ProcessResult {
	return nil
}

func (m *TaskMaster) GetInstanceStatus() processor.InstanceStatus {
	//	m.lock.RLock()
	//	defer m.lock.RUnlock()
	return m.instanceStatus
}

func (m *TaskMaster) SetInstanceStatus(instanceStatus processor.InstanceStatus) {
	//	m.lock.Lock()
	m.instanceStatus = instanceStatus
	//	m.lock.Unlock()
}

func (m *TaskMaster) IsKilled() bool {
	//	m.lock.RLock()
	//	defer m.lock.RUnlock()
	return m.killed
}

func (m *TaskMaster) GetJobInstanceInfo() *common.JobInstanceInfo {
	return m.jobInstanceInfo
}

// GetAliveCheckWorkerSet return set<string>
func (m *TaskMaster) GetAliveCheckWorkerSet() *utils.ConcurrentSet {
	return m.aliveCheckWorkerSet
}

func (m *TaskMaster) IsInited() bool {
	//	m.lock.RLock()
	//	defer m.lock.RUnlock()
	return m.inited
}

func (m *TaskMaster) GetSerialNum() int64 {
	return m.serialNum.Load()
}

func (m *TaskMaster) AcquireSerialNum() int64 {
	return m.serialNum.Inc()
}

func (m *TaskMaster) ExistInvalidWorker() bool {
	return m.existInvalidWorker
}

func (m *TaskMaster) ResetJobInstanceWorkerList() {
	freeWorkersNum := m.aliveCheckWorkerSet.Len()
	if freeWorkersNum > 0 {
		m.jobInstanceInfo.SetAllWorkers(m.aliveCheckWorkerSet.ToStringSlice())
		m.existInvalidWorker = false
		logger.Infof("restJobInstanceWorkerList appGroupId=%d, instanceId=%d, workerSize=%d.",
			m.jobInstanceInfo.GetAppGroupId(), m.jobInstanceInfo.GetJobInstanceId(), freeWorkersNum)
	} else {
		logger.Warnf("restJobInstanceWorkerList update appGroupId=%d, instanceId=%d, workers=0.",
			m.jobInstanceInfo.GetAppGroupId(), m.jobInstanceInfo.GetJobInstanceId())
	}
}

func (m *TaskMaster) GetCurrentSelection() string {
	// TODO implement me
	return ""
}

func (m *TaskMaster) convert2StartContainerRequest(jobInstanceInfo *common.JobInstanceInfo, taskId int64, taskName string, taskBody []byte, failover bool) (*schedulerx.MasterStartContainerRequest, error) {
	req := &schedulerx.MasterStartContainerRequest{
		JobId:                  proto.Int64(jobInstanceInfo.GetJobId()),
		JobInstanceId:          proto.Int64(jobInstanceInfo.GetJobInstanceId()),
		TaskId:                 proto.Int64(taskId),
		User:                   proto.String(jobInstanceInfo.GetUser()),
		JobType:                proto.String(jobInstanceInfo.GetJobType()),
		Content:                proto.String(jobInstanceInfo.GetContent()),
		ScheduleTime:           proto.Int64(jobInstanceInfo.GetScheduleTime().Nanoseconds()), // FIXME check if Nanoseconds
		DataTime:               proto.Int64(jobInstanceInfo.GetDataTime().Nanoseconds()),     // FIXME check if Nanoseconds
		Parameters:             proto.String(jobInstanceInfo.GetParameters()),
		InstanceParameters:     proto.String(jobInstanceInfo.GetInstanceParameters()),
		GroupId:                proto.String(jobInstanceInfo.GetGroupId()),
		MaxAttempt:             proto.Int32(jobInstanceInfo.GetMaxAttempt()),
		Attempt:                proto.Int32(jobInstanceInfo.GetAttempt()),
		InstanceMasterAkkaPath: proto.String(m.GetLocalTaskRouterPath()),
	}

	if upstreamDatas := jobInstanceInfo.GetUpstreamData(); len(upstreamDatas) > 0 {
		req.UpstreamData = []*schedulerx.UpstreamData{}

		for _, jobInstanceData := range upstreamDatas {
			req.UpstreamData = append(req.UpstreamData, &schedulerx.UpstreamData{
				JobName: proto.String(jobInstanceData.GetJobName()),
				Data:    proto.String(jobInstanceData.GetData()),
			})
		}
	}

	if xattrs := jobInstanceInfo.GetXattrs(); len(xattrs) > 0 {
		mapTaskXAttrs := common.NewMapTaskXAttrs()
		if err := json.Unmarshal([]byte(xattrs), mapTaskXAttrs); err != nil {
			return nil, fmt.Errorf("Json unmarshal to mapTaskXAttrs failed, xattrs=%s, err=%s ", xattrs, err.Error())
		}
		req.ConsumerNum = proto.Int32(mapTaskXAttrs.GetConsumerSize())
		req.MaxAttempt = proto.Int32(mapTaskXAttrs.GetTaskMaxAttempt())
		req.TaskAttemptInterval = proto.Int32(mapTaskXAttrs.GetTaskAttemptInterval())
	}

	if taskName != "" {
		req.TaskName = proto.String(taskName)
	}
	if len(taskBody) > 0 {
		req.Task = taskBody
	}
	if failover {
		req.Failover = proto.Bool(true)
	}
	if jobInstanceInfo.GetWfInstanceId() >= 0 {
		req.WfInstanceId = proto.Int64(jobInstanceInfo.GetWfInstanceId())
	}

	req.SerialNum = proto.Int64(m.GetSerialNum())
	req.ExecuteMode = proto.String(jobInstanceInfo.GetExecuteMode())

	if len(jobInstanceInfo.GetJobName()) > 0 {
		req.JobName = proto.String(jobInstanceInfo.GetJobName())
	}
	req.TimeType = proto.Int32(jobInstanceInfo.GetTimeType())
	req.TimeExpression = proto.String(jobInstanceInfo.GetTimeExpression())

	return req, nil
}

func (m *TaskMaster) RestJobInstanceWorkerList(freeWorkers *utils.Set) {
	if freeWorkers.Size() > 0 {
		m.jobInstanceInfo.SetAllWorkers(freeWorkers.ToStringSlice())
		m.existInvalidWorker = false
		logger.Infof("restJobInstanceWorkerList appGroupId=%v instanceId=%v workerSize=%v", m.jobInstanceInfo.GetAppGroupId(),
			m.jobInstanceInfo.GetJobInstanceId(), freeWorkers.Size())
	} else {
		logger.Warnf("restJobInstanceWorkerList update appGroupId=%v instanceId=%v workers=0.", m.jobInstanceInfo.GetAppGroupId(),
			m.jobInstanceInfo.GetJobInstanceId())
	}
}
