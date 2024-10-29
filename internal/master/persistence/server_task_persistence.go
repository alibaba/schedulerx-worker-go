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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob/bizsubtask"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ TaskPersistence = &ServerTaskPersistence{}

type ServerTaskPersistence struct {
	serverDiscovery *discovery.ServiceDiscover
	groupManager    *discovery.GroupManager
	groupId         string
}

func NewServerTaskPersistence(groupId string) (rcvr *ServerTaskPersistence) {
	return &ServerTaskPersistence{
		groupId:         groupId,
		serverDiscovery: discovery.GetDiscovery(groupId),
	}
}

func (rcvr *ServerTaskPersistence) BatchUpdateTaskStatus(jobInstanceId int64, status taskstatus.TaskStatus, workerId string, workerAddr string) int64 {
	var affectCnt int64
	req := &schedulerx.WorkerBatchUpdateTaskStatusRequest{
		JobInstanceId: proto.Int64(jobInstanceId),
		Status:        proto.Int32(int32(status)),
	}
	if workerAddr != "" {
		req.WorkerAddr = proto.String(workerAddr)
		req.WorkerId = proto.String(workerId)
	}

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}

	// Wait 5 seconds
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender():
		if resp.GetSuccess() {
			affectCnt = int64(resp.GetAffectCnt())
			logger.Debugf("batch update Status=>%d to Server succeed, JobInstanceId=%d, workerAddr=%s", status, jobInstanceId, workerAddr)
		} else {
			logger.Errorf("batch update Status failed, response message=%s", resp.GetMessage())
		}
	case <-timer.C:
		logger.Errorf("BatchUpdateTaskStatus of JobInstanceId=%d in ServerTaskPersistence timeout", jobInstanceId)
	}

	return affectCnt
}

func (rcvr *ServerTaskPersistence) CheckInstanceStatus(jobInstanceId int64) processor.InstanceStatus {
	status := processor.InstanceStatusUnknown
	req := &schedulerx.WorkerQueryJobInstanceStatusRequest{
		JobInstanceId: proto.Int64(jobInstanceId),
	}

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}

	// Wait 30 seconds
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerQueryJobInstanceStatusRespMsgSender():
		if resp.GetSuccess() {
			status = processor.InstanceStatus(resp.GetStatus())
		} else {
			logger.Errorf("query job instance Status failed, resp message=%s", resp.GetMessage())
		}
	case <-timer.C:
		logger.Errorf("CheckInstanceStatus of JobInstanceId=%d in ServerTaskPersistence timeout", jobInstanceId)
	}

	return status
}

func (rcvr *ServerTaskPersistence) ClearTasks(jobInstanceId int64) error {
	req := &schedulerx.WorkerClearTasksRequest{
		JobInstanceId: proto.Int64(jobInstanceId),
	}

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}

	// Wait 5 seconds
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerClearTasksRespMsgSender():
		if resp.GetSuccess() {
			logger.Infof("clear tasks of jobInstance[%d] succeed", jobInstanceId)
		} else {
			errMsg := fmt.Sprintf("clear tasks of jobInstance[%d] failed, error=%s", jobInstanceId, resp.GetMessage())
			logger.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	case <-timer.C:
		logger.Errorf("ClearTasks of JobInstanceId=%d in ServerTaskPersistence timeout", jobInstanceId)
	}

	return nil
}

func (rcvr *ServerTaskPersistence) convert2TaskInfo(taskMessage *schedulerx.TaskMessage) *common.TaskInfo {
	req := new(common.TaskInfo)
	req.SetTaskId(taskMessage.GetTaskId())
	req.SetTaskName(taskMessage.GetTaskName())
	req.SetTaskBody(taskMessage.GetTaskBody())
	return req
}

// CreateTask do nothing, already create by server.
func (rcvr *ServerTaskPersistence) CreateTask(jobId int64, jobInstanceId int64, taskId int64, taskName string, taskBody []byte) error {
	// do nothing
	return nil
}

func (rcvr *ServerTaskPersistence) CreateTasks(containers []*schedulerx.MasterStartContainerRequest, workerId string, workerAddr string) error {
	if len(containers) == 0 {
		return fmt.Errorf("createTasks container list empty")
	}
	jobInstanceId := containers[0].GetJobInstanceId()
	batchReqs := new(schedulerx.WorkerBatchCreateTasksRequest)

	isAdvancedVersion := rcvr.groupManager.IsAdvancedVersion(rcvr.groupId)
	for _, taskInfo := range containers {
		req := &schedulerx.WorkerCreateTaskRequest{
			JobId:         proto.Int64(taskInfo.GetJobId()),
			JobInstanceId: proto.Int64(taskInfo.GetJobInstanceId()),
			TaskId:        proto.Int64(taskInfo.GetTaskId()),
			TaskName:      proto.String(taskInfo.GetTaskName()),
			TaskBody:      taskInfo.GetTask(),
		}

		task := new(interface{})
		if err := json.Unmarshal(taskInfo.GetTask(), task); err != nil {
			return fmt.Errorf("json unmarshal TaskBody failed, err=%s", err.Error())
		}

		bisSubTask, ok := (*task).(bizsubtask.BizSubTask)
		if isAdvancedVersion && ok {
			tmp, err := json.Marshal(bisSubTask.LabelMap())
			if err != nil {
				return fmt.Errorf("json marshal TaskBody's labelMap failed, err=%s", err.Error())
			}
			req.LabelMap = proto.String(string(tmp))
		}
		batchReqs.Task = append(batchReqs.Task, req)
	}
	batchReqs.JobInstanceId = proto.Int64(jobInstanceId)
	batchReqs.WorkerId = proto.String(workerId)
	batchReqs.WorkerAddr = proto.String(workerAddr)

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: batchReqs,
	}

	// Wait 90 seconds
	timer := time.NewTimer(90 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerBatchCreateTasksRespMsgSender():
		if resp.GetSuccess() {
			logger.Infof("batch create tasks to server succeed, JobInstanceId=%d, size=%d", jobInstanceId, len(containers))
		} else {
			errMsg := fmt.Sprintf("batch create tasks error, JobInstanceId=%d, reason=%s.", jobInstanceId, resp.GetMessage())
			logger.Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	case <-timer.C:
		logger.Errorf("ClearTasks of JobInstanceId=%d in ServerTaskPersistence timeout", jobInstanceId)
	}

	return nil
}

func (rcvr *ServerTaskPersistence) InitTable() {
	// FXIME do nothing
}

func (rcvr *ServerTaskPersistence) Pull(jobInstanceId int64, pageSize int32) ([]*common.TaskInfo, error) {
	taskInfos := make([]*common.TaskInfo, 0, 10)
	req := &schedulerx.WorkerPullTasksRequest{
		JobInstanceId: proto.Int64(jobInstanceId),
		PageSize:      proto.Int32(pageSize),
	}

	// Send to server by master
	actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}

	// Wait 30 seconds
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case resp := <-actorcomm.WorkerPullTasksRespMsgSender():
		if resp.GetSuccess() {
			taskMessages := resp.GetTaskMessage()
			for _, taskMessage := range taskMessages {
				taskInfos = append(taskInfos, rcvr.convert2TaskInfo(taskMessage))
			}
		} else {
			logger.Errorf("pull tasks of jobInstance=%d failed, respMsg=%s", jobInstanceId, resp.GetMessage())
		}
	case <-timer.C:
		logger.Errorf("ClearTasks of JobInstanceId=%d in ServerTaskPersistence timeout", jobInstanceId)
	}

	return taskInfos, nil
}

func (rcvr *ServerTaskPersistence) UpdateTaskStatues(taskStatusInfos []*schedulerx.ContainerReportTaskStatusRequest) error {
	if len(taskStatusInfos) == 0 {
		return fmt.Errorf("update task statues empty")
	}
	info := taskStatusInfos[0]
	status2WorkIdAddr2TaskIds := getTaskStatusMap(taskStatusInfos)
	var batchTaskStatuesReq schedulerx.WorkerBatchReportTaskStatuesRequest
	for status, workerAddr2TaskIds := range status2WorkIdAddr2TaskIds {
		for workerIdAddr, taskIds := range workerAddr2TaskIds {
			workerIdAddrParts := strings.Split(workerIdAddr, "@")
			batchTaskStatues := &schedulerx.BatchTaskStatues{
				Status:     proto.Int32(status),
				WorkerId:   proto.String(workerIdAddrParts[0]),
				WorkerAddr: proto.String(actorcomm.GetRealWorkerAddr(workerIdAddr)),
				TaskIds:    taskIds,
			}
			batchTaskStatuesReq.TaskStatues = append(batchTaskStatuesReq.TaskStatues, batchTaskStatues)
			batchTaskStatuesReq.GroupId = proto.String(rcvr.groupId)
		}
	}
	batchTaskStatuesReq.JobInstanceId = proto.Int64(info.GetJobInstanceId())

	actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: &batchTaskStatuesReq,
	}

	return nil
}

func (rcvr *ServerTaskPersistence) UpdateTaskStatus(jobInstanceId int64, taskIds []int64, status taskstatus.TaskStatus, workerId, workerAddr string) (int64, error) {
	var affectCnt int64
	if len(taskIds) == 0 {
		return 0, fmt.Errorf("update task statuses empty")
	}

	req := &schedulerx.WorkerBatchReportTaskStatuesRequest{
		JobInstanceId: proto.Int64(jobInstanceId),
		GroupId:       proto.String(rcvr.groupId),
	}

	batchTaskStatues := &schedulerx.BatchTaskStatues{
		Status:     proto.Int32(int32(status)),
		WorkerId:   proto.String(workerId),
		WorkerAddr: proto.String(workerAddr),
		TaskIds:    taskIds,
	}
	req.TaskStatues = append(req.TaskStatues, batchTaskStatues)

	actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}
	var resp *schedulerx.WorkerBatchReportTaskStatuesResponse
	if !resp.GetSuccess() {
		errMsg := fmt.Sprintf("batch update task Status of jobInstance=%d failed, err=%s", jobInstanceId, resp.GetMessage())
		logger.Errorf(errMsg)
		return 0, fmt.Errorf(errMsg)
	} else {
		affectCnt = int64(resp.GetAffectCnt())
	}

	return affectCnt, nil
}
