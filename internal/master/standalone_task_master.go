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
	"runtime/debug"
	"time"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/tracer"
)

var _ taskmaster.TaskMaster = &StandaloneTaskMaster{}

type StandaloneTaskMaster struct {
	*TaskMaster
	taskMasterPoolCleaner func(int64)
}

func NewStandaloneTaskMaster(jobInstanceInfo *common.JobInstanceInfo) *StandaloneTaskMaster {
	var (
		taskMaster            = NewTaskMaster(jobInstanceInfo)
		taskMasterPool        = masterpool.GetTaskMasterPool()
		taskMasterPoolCleaner = func(jobInstanceId int64) {
			if taskMaster := taskMasterPool.Get(jobInstanceId); taskMaster != nil {
				taskMaster.Stop()
				taskMasterPool.Remove(jobInstanceId)
			}
		}
	)
	return &StandaloneTaskMaster{
		TaskMaster:            taskMaster,
		taskMasterPoolCleaner: taskMasterPoolCleaner,
	}
}

func (m *StandaloneTaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	taskId := m.AcquireTaskId()
	uniqueId := utils.GetUniqueId(jobInstanceInfo.GetJobId(), jobInstanceInfo.GetJobInstanceId(), taskId)

	req, err := m.convert2StartContainerRequest(jobInstanceInfo, taskId, "", nil, false)
	if err != nil {
		logger.Errorf("SubmitInstance failed, jobInstanceInfo=%+v, err=%s.", jobInstanceInfo, err.Error())
		m.taskStatusMap.Store(uniqueId, common.TaskStatusFailed)
		return err
	}

	go m.handleTask(ctx, req)

	return nil
}

func (m *StandaloneTaskMaster) handleTask(ctx context.Context, req *schedulerx.MasterStartContainerRequest) {
	defer func() {
		// clean taskMasterPool
		m.taskMasterPoolCleaner(*req.JobInstanceId)

		if e := recover(); e != nil {
			logger.Infof("Process task panic, error=%v, stack=%s", e, debug.Stack())
		}
	}()

	jobCtx := convert2JobContext(req)
	jobName := gjson.Get(jobCtx.Content(), "jobName").String()
	// Compatible with the existing Java language configuration mechanism
	if jobCtx.JobType() == "java" {
		jobName = gjson.Get(jobCtx.Content(), "className").String()
	}
	task, ok := tasks.GetTaskMap().Find(jobName)
	if !ok {
		retMsg := fmt.Sprintf("jobName=%s not found, maybe forgot to register it by the client", jobName)
		if err := trans.SendReportTaskStatusReq(ctx, m.convert2ReportTaskStatusRequest(jobCtx, processor.InstanceStatusFailed, retMsg, "1.0", "")); err != nil {
			logger.Errorf("Report task status=%v failed, jobCtx=%+v, err=%s ", common.TaskStatusFailed, jobCtx, err.Error())
			return
		}
		logger.Errorf("Process task=%s failed, because it's unregistered. ", req.GetTaskName())
		return
	}

	if err := trans.SendReportTaskStatusReq(ctx, m.convert2ReportTaskStatusRequest(jobCtx, processor.InstanceStatusRunning, "", "0.5", "")); err != nil {
		logger.Errorf("Report task status=%v failed, jobCtx=%+v, err=%s ", common.TaskStatusRunning, jobCtx, err.Error())
		return
	}

	if t := tracer.GetTracer(); t != nil {
		jobCtx = t.Start(jobCtx)
	}

	ret, err := task.Process(jobCtx)

	if t := tracer.GetTracer(); t != nil {
		ret = t.End(jobCtx, ret)
	}
	if err != nil {
		if err := trans.SendReportTaskStatusReq(ctx, m.convert2ReportTaskStatusRequest(jobCtx, processor.InstanceStatusFailed, ret.GetResult(), "1.0", "")); err != nil {
			logger.Errorf("Report task status=%v failed, jobCtx=%+v, err=%s ", common.TaskStatusFailed, jobCtx, err.Error())
			return
		}
		logger.Errorf("Process task=%s failed, err=%s ", req.GetTaskName(), err.Error())
		return
	}

	if err := trans.SendReportTaskStatusReq(ctx, m.convert2ReportTaskStatusRequest(jobCtx, ret.GetStatus(), ret.GetResult(), "1.0", "")); err != nil {
		logger.Errorf("Report task status=%v failed, jobCtx=%+v, err=%s ", common.TaskStatusSucceed, jobCtx, err.Error())
		return
	}
	logger.Infof("Process task=%s succeed, ret=%+v", jobName, ret.String())
}

func convert2JobContext(req *schedulerx.MasterStartContainerRequest) *jobcontext.JobContext {
	jobCtx := new(jobcontext.JobContext)
	jobCtx.SetJobId(req.GetJobId())
	jobCtx.SetJobInstanceId(req.GetJobInstanceId())
	jobCtx.SetTaskId(req.GetTaskId())
	jobCtx.SetScheduleTime(time.UnixMilli(req.GetScheduleTime()))
	jobCtx.SetDataTime(time.UnixMilli(req.GetDataTime()))
	jobCtx.SetExecuteMode(req.GetExecuteMode())
	jobCtx.SetJobType(req.GetJobType())
	jobCtx.SetContent(req.GetContent())
	jobCtx.SetJobParameters(req.GetParameters())
	jobCtx.SetInstanceParameters(req.GetInstanceParameters())
	jobCtx.SetUser(req.GetUser())
	jobCtx.SetInstanceMasterActorPath(req.GetInstanceMasterAkkaPath())
	jobCtx.SetGroupId(req.GetGroupId())
	jobCtx.SetMaxAttempt(req.GetMaxAttempt())
	jobCtx.SetAttempt(req.GetAttempt())
	jobCtx.SetTaskName(req.GetTaskName())
	if req.GetTask() == nil {
		jobCtx.SetShardingId(req.GetTaskId())
	}
	jobCtx.SetTaskMaxAttempt(req.GetTaskMaxAttempt())
	jobCtx.SetTaskAttemptInterval(req.GetTaskAttemptInterval())

	var upstreamData []*common.JobInstanceData
	for _, data := range req.GetUpstreamData() {
		jobInstanceData := new(common.JobInstanceData)
		jobInstanceData.SetJobName(data.GetJobName())
		jobInstanceData.SetData(data.GetData())
		upstreamData = append(upstreamData, jobInstanceData)
	}
	jobCtx.SetUpstreamData(upstreamData)
	jobCtx.SetWfInstanceId(req.GetWfInstanceId())
	jobCtx.SetSerialNum(req.GetSerialNum())
	jobCtx.SetJobName(req.GetJobName())
	jobCtx.SetShardingNum(req.GetShardingNum())
	jobCtx.SetTimeType(req.GetTimeType())
	jobCtx.SetTimeExpression(req.GetTimeExpression())
	return jobCtx
}

func (m *StandaloneTaskMaster) convert2ReportTaskStatusRequest(jobCtx *jobcontext.JobContext, taskStatus processor.InstanceStatus, taskResult, progress, traceId string) *schedulerx.WorkerReportJobInstanceStatusRequest {
	return &schedulerx.WorkerReportJobInstanceStatusRequest{
		JobId:         proto.Int64(jobCtx.JobId()),
		JobInstanceId: proto.Int64(jobCtx.JobInstanceId()),
		Status:        proto.Int32(int32(taskStatus)),
		Result:        proto.String(taskResult),
		Progress:      proto.String(progress),
		TraceId:       proto.String(traceId),
		DeliveryId:    proto.Int64(utils.GetDeliveryId()),
		GroupId:       proto.String(jobCtx.GroupId()),
	}
}

func (m *StandaloneTaskMaster) selectWorker() string {
	workers := m.jobInstanceInfo.GetAllWorkers()
	workersCnt := len(workers)
	idx := 0

	if workersCnt == 0 {
		return ""
	} else if serialNum := m.serialNum.Load(); serialNum > int64(workersCnt) {
		idx = int(serialNum % int64(workersCnt))
	}

	return workers[idx]
}

func (m *StandaloneTaskMaster) KillInstance(reason string) error {
	err := m.TaskMaster.KillInstance(reason)
	if err != nil {
		return err
	}

	m.TaskMaster.SendKillContainerRequest(true, m.GetLocalWorkerIdAddr())
	_ = m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, reason)

	if !m.instanceStatus.IsFinished() {
		m.instanceStatus = processor.InstanceStatusFailed
	}

	return nil
}

func (m *StandaloneTaskMaster) convert2StartContainerRequest(jobInstanceInfo *common.JobInstanceInfo, taskId int64, taskName string, taskBody []byte, failover bool) (*schedulerx.MasterStartContainerRequest, error) {
	req := &schedulerx.MasterStartContainerRequest{
		JobId:              proto.Int64(jobInstanceInfo.GetJobId()),
		JobInstanceId:      proto.Int64(jobInstanceInfo.GetJobInstanceId()),
		TaskId:             proto.Int64(taskId),
		User:               proto.String(jobInstanceInfo.GetUser()),
		JobType:            proto.String(jobInstanceInfo.GetJobType()),
		Content:            proto.String(jobInstanceInfo.GetContent()),
		ScheduleTime:       proto.Int64(jobInstanceInfo.GetScheduleTime().Milliseconds()),
		DataTime:           proto.Int64(jobInstanceInfo.GetDataTime().Milliseconds()),
		Parameters:         proto.String(jobInstanceInfo.GetParameters()),
		InstanceParameters: proto.String(jobInstanceInfo.GetInstanceParameters()),
		GroupId:            proto.String(jobInstanceInfo.GetGroupId()),
		MaxAttempt:         proto.Int32(jobInstanceInfo.GetMaxAttempt()),
		Attempt:            proto.Int32(jobInstanceInfo.GetAttempt()),
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
