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

package actor

import (
	"context"
	"fmt"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/tidwall/gjson"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ actor.Actor = &jobInstanceActor{}

type jobInstanceActor struct {
	connpool       pool.ConnPool
	taskmasterPool *masterpool.TaskMasterPool
}

func newJobInstanceActor(actorSystem *actor.ActorSystem) *jobInstanceActor {
	jActor := &jobInstanceActor{
		connpool:       pool.GetConnPool(),
		taskmasterPool: masterpool.GetTaskMasterPool(),
	}

	resolver := func(pid *actor.PID) (actor.Process, bool) {
		if actorcomm.IsSchedulerxServer(pid) {
			return newJobInstanceProcessor(jActor.connpool), true
		}

		// If communicate with actors other than server, then use the default handler (return false)
		return nil, false
	}
	actorSystem.ProcessRegistry.RegisterAddressResolver(resolver)

	return jActor
}

func (a *jobInstanceActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *schedulerx.WorkerBatchReportTaskStatuesResponse:
		// send to atLeastOnceDeliveryRoutingActor
		//actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		//	Msg: msg,
		//}
		// FIXME atLeastOnceDelivery not yet implement, retry 3 times, interval 30s
		a.handleReportWorkerStatus(ctx, ctx.Message())
	case *schedulerx.WorkerReportJobInstanceStatusResponse:
		// send to atLeastOnceDeliveryRoutingActor
		//actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		//	Msg: msg,
		//}
		// FIXME atLeastOnceDelivery not yet implement, retry 3 times, interval 30s
		a.handleReportWorkerStatus(ctx, ctx.Message())
	case *actorcomm.SchedulerWrappedMsg:
		switch innerMsg := msg.Msg.(type) {
		case *schedulerx.ServerSubmitJobInstanceRequest:
			if err := a.handleSubmitJobInstance(ctx, msg); err != nil {
				logger.Errorf("handleSubmitJobInstanceRequest failed, err=%s", err.Error())
			}
		case *schedulerx.ServerKillJobInstanceRequest:
			if err := a.handleKillJobInstance(ctx, msg); err != nil {
				logger.Errorf("handleKillJobInstanceRequest failed, err=%s", err.Error())
			}
		case *schedulerx.ServerRetryTasksRequest:
			a.handleRetryTasks(ctx, msg)
		case *schedulerx.ServerKillTaskRequest:
			a.handleKillTask(ctx, msg)
		case *schedulerx.ServerCheckTaskMasterRequest:
			a.handCheckTaskMaster(ctx, msg)
		case *schedulerx.MasterNotifyWorkerPullRequest:
			a.handleInitPull(ctx, msg)
		case *schedulerx.WorkerReportJobInstanceStatusRequest:
			// forward to server
			ctx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), msg)
		case *schedulerx.WorkerReportJobInstanceStatusResponse, *schedulerx.WorkerBatchReportTaskStatuesResponse:
			// send to atLeastOnceDeliveryRoutingActor
			actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
				Msg:        innerMsg,
				SenderPath: msg.SenderPath,
			}
		default:
			logger.Errorf("Receive unknown message in jobInstanceActor, msg=%+v", msg)
		}
	}
}

func (a *jobInstanceActor) handleSubmitJobInstance(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) error {
	var (
		req            = msg.Msg.(*schedulerx.ServerSubmitJobInstanceRequest)
		taskMasterPool = masterpool.GetTaskMasterPool()
	)
	logger.Infof("handleSubmitJobInstance, jobInstanceId=%d, req=%+v", req.GetJobInstanceId(), req)
	if taskMasterPool.Contains(req.GetJobInstanceId()) {
		errMsg := fmt.Sprintf("jobInstanceId=%d is still running!", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerSubmitJobInstanceResponse{
			Success: proto.Bool(false),
			Message: proto.String(errMsg),
		}
		actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
	} else {
		resp := &schedulerx.ServerSubmitJobInstanceResponse{
			Success: proto.Bool(true),
		}
		actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
		jobInstanceInfo := convert2JobInstanceInfo(req)

		// check job is registered
		jobName := gjson.Get(jobInstanceInfo.GetContent(), "jobName").String()
		// Compatible with the existing Java language configuration mechanism
		if jobInstanceInfo.GetJobType() == "java" {
			jobName = gjson.Get(jobInstanceInfo.GetContent(), "className").String()
		}
		task, ok := masterpool.GetTaskMasterPool().Tasks().Find(jobName)
		if !ok || task == nil {
			fmt.Errorf("handleSubmitJobInstance error, jobName=%s is unregistered. ", jobName)

			// report job instance status with at-least-once-delivery
			req := &schedulerx.WorkerReportJobInstanceStatusRequest{
				JobId:         proto.Int64(jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(jobInstanceInfo.GetJobInstanceId()),
				Status:        proto.Int32(int32(processor.InstanceStatusFailed)),
				DeliveryId:    proto.Int64(utils.GetDeliveryId()),
				GroupId:       proto.String(jobInstanceInfo.GetGroupId()),
				Result:        proto.String(fmt.Sprintf("jobName=%s is unregistered", jobName)),
			}
			actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
				Msg: req,
			}
		} else {
			var taskMaster taskmaster.TaskMaster
			switch common.ExecuteMode(jobInstanceInfo.GetExecuteMode()) {
			case common.StandaloneExecuteMode:
				taskMaster = master.NewStandaloneTaskMaster(jobInstanceInfo, actorCtx)
			case common.BroadcastExecuteMode:
				taskMaster = master.NewBroadcastTaskMaster(jobInstanceInfo, actorCtx)
			case common.BatchExecuteMode:
				taskMaster = master.NewBatchTaskMaster(jobInstanceInfo, actorCtx)
			case common.ParallelExecuteMode:
				taskMaster = master.NewParallelTaskMaster(jobInstanceInfo, actorCtx)
			case common.GridExecuteMode:
				taskMaster = master.NewGridTaskMaster(jobInstanceInfo, actorCtx)
			case common.ShardingExecuteMode:
				taskMaster = master.NewShardingTaskMaster(jobInstanceInfo, actorCtx)
			default:
				logger.Errorf("Submit jobInstanceId=%d failed, unknown executeMode=%s", jobInstanceInfo.GetExecuteMode())
			}

			if taskMaster != nil {
				masterpool.GetTaskMasterPool().Put(jobInstanceInfo.GetJobInstanceId(), taskMaster)
				if err := taskMaster.SubmitInstance(msg.Ctx, jobInstanceInfo); err != nil {
					return err
				}
				logger.Infof("Submit jobInstanceId=%d succeed", req.GetJobInstanceId())
			}
		}
	}
	return nil
}

func convert2JobInstanceInfo(req *schedulerx.ServerSubmitJobInstanceRequest) *common.JobInstanceInfo {
	jobInstanceInfo := new(common.JobInstanceInfo)
	jobInstanceInfo.SetJobId(req.GetJobId())
	jobInstanceInfo.SetJobInstanceId(req.GetJobInstanceId())
	jobInstanceInfo.SetExecuteMode(req.GetExecuteMode())
	jobInstanceInfo.SetJobType(req.GetJobType())
	jobInstanceInfo.SetContent(req.GetContent())
	jobInstanceInfo.SetUser(req.GetUser())
	jobInstanceInfo.SetScheduleTime(time.Duration(req.GetScheduleTime()))
	jobInstanceInfo.SetDataTime(time.Duration(req.GetDataTime()))
	jobInstanceInfo.SetAllWorkers(utils.ShuffleStringSlice(req.GetWorkers()))
	jobInstanceInfo.SetJobConcurrency(req.GetJobConcurrency())
	jobInstanceInfo.SetRegionId(req.GetRegionId())
	jobInstanceInfo.SetAppGroupId(req.GetAppGroupId())
	jobInstanceInfo.SetTimeType(req.GetTimeType())
	jobInstanceInfo.SetTimeExpression(req.GetTimeExpression())
	jobInstanceInfo.SetGroupId(req.GetGroupId())
	jobInstanceInfo.SetTriggerType(req.GetTriggerType())
	jobInstanceInfo.SetParameters(req.GetParameters())
	jobInstanceInfo.SetXattrs(req.GetXattrs())
	jobInstanceInfo.SetInstanceParameters(req.GetInstanceParameters())
	jobInstanceInfo.SetUpstreamData(convert2JobInstanceData(req.GetUpstreamData()))
	jobInstanceInfo.SetMaxAttempt(req.GetMaxAttempt())
	jobInstanceInfo.SetAttempt(req.GetAttempt())
	jobInstanceInfo.SetWfInstanceId(req.GetWfInstanceId())
	jobInstanceInfo.SetJobName(req.GetJobName())
	return jobInstanceInfo
}

func convert2JobInstanceData(datas []*schedulerx.UpstreamData) []*common.JobInstanceData {
	var ret []*common.JobInstanceData
	for _, data := range datas {
		tmp := new(common.JobInstanceData)
		tmp.SetData(data.GetData())
		tmp.SetJobName(data.GetJobName())

		ret = append(ret, tmp)
	}
	return ret
}

func (a *jobInstanceActor) handleKillJobInstance(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) error {
	var (
		taskMasterPool = masterpool.GetTaskMasterPool()
		req            = msg.Msg.(*schedulerx.ServerKillJobInstanceRequest)
	)
	logger.Infof("handleKillJobInstance, jobInstanceId=%d ", req.GetJobInstanceId())
	if !taskMasterPool.Contains(req.GetJobInstanceId()) {
		errMsg := fmt.Sprintf("%d is not exist", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerKillJobInstanceResponse{
			Success: proto.Bool(true),
			Message: proto.String(errMsg),
		}
		actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
	} else {
		if taskMaster := masterpool.GetTaskMasterPool().Get(req.GetJobInstanceId()); taskMaster != nil {
			if err := taskMaster.KillInstance("killed from server"); err != nil {
				logger.Infof(fmt.Sprintf("%d killed from server failed, err=%s", req.GetJobInstanceId()), err.Error())
			}
		}
		errMsg := fmt.Sprintf("%d killed from server", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerKillJobInstanceResponse{
			Success: proto.Bool(false), // FIXME true or false
			Message: proto.String(errMsg),
		}
		actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
		logger.Infof("Kill jobInstanceId=%d succeed", req.GetJobInstanceId())
	}
	return nil
}

func (a *jobInstanceActor) handleRetryTasks(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) {
	req := msg.Msg.(*schedulerx.ServerRetryTasksRequest)
	logger.Infof("handleRetryTasks, jobInstanceId=%d", req.GetJobInstanceId())
	var (
		jobInstanceInfo = convertServerRetryTasksRequest2JobInstanceInfo(req)
	)
	if taskMaster := a.taskmasterPool.Get(jobInstanceInfo.GetJobInstanceId()); taskMaster != nil {
		if parallelTaskMaster, ok := taskMaster.(taskmaster.ParallelTaskMaster); ok {
			parallelTaskMaster.RetryTasks(req.GetRetryTaskEntity())
			resp := &schedulerx.ServerRetryTasksResponse{
				Success: proto.Bool(true),
			}
			actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
		}
	} else {
		var newTaskMaster taskmaster.TaskMaster
		switch common.ExecuteMode(jobInstanceInfo.GetExecuteMode()) {
		case common.StandaloneExecuteMode:
			newTaskMaster = master.NewStandaloneTaskMaster(jobInstanceInfo, actorCtx)
		case common.BroadcastExecuteMode:
			newTaskMaster = master.NewBroadcastTaskMaster(jobInstanceInfo, actorCtx)
		case common.BatchExecuteMode:
			newTaskMaster = master.NewBatchTaskMaster(jobInstanceInfo, actorCtx)
		case common.ParallelExecuteMode:
			newTaskMaster = master.NewParallelTaskMaster(jobInstanceInfo, actorCtx)
		case common.GridExecuteMode:
			newTaskMaster = master.NewGridTaskMaster(jobInstanceInfo, actorCtx)
		case common.ShardingExecuteMode:
			taskMaster = master.NewShardingTaskMaster(jobInstanceInfo, actorCtx)
		default:
			logger.Errorf("handleRetryTasks failed, jobInstanceId=%d, unknown executeMode=%s", jobInstanceInfo.GetExecuteMode())
		}
		if newTaskMaster != nil {
			masterpool.GetTaskMasterPool().Put(jobInstanceInfo.GetJobInstanceId(), newTaskMaster)
		}
	}
}

func convertServerRetryTasksRequest2JobInstanceInfo(req *schedulerx.ServerRetryTasksRequest) *common.JobInstanceInfo {
	jobInstanceInfo := new(common.JobInstanceInfo)
	jobInstanceInfo.SetJobId(req.GetJobId())
	jobInstanceInfo.SetJobInstanceId(req.GetJobInstanceId())
	jobInstanceInfo.SetExecuteMode(req.GetExecuteMode())
	jobInstanceInfo.SetJobType(req.GetJobType())
	jobInstanceInfo.SetContent(req.GetContent())
	jobInstanceInfo.SetUser(req.GetUser())
	jobInstanceInfo.SetScheduleTime(time.Duration(req.GetScheduleTime()))
	jobInstanceInfo.SetDataTime(time.Duration(req.GetDataTime()))
	jobInstanceInfo.SetAllWorkers(utils.ShuffleStringSlice(req.GetWorkers()))
	jobInstanceInfo.SetJobConcurrency(req.GetJobConcurrency())
	jobInstanceInfo.SetRegionId(req.GetRegionId())
	jobInstanceInfo.SetAppGroupId(req.GetAppGroupId())
	jobInstanceInfo.SetTimeType(req.GetTimeType())
	jobInstanceInfo.SetTimeExpression(req.GetTimeExpression())
	jobInstanceInfo.SetGroupId(req.GetGroupId())
	jobInstanceInfo.SetParameters(req.GetParameters())
	jobInstanceInfo.SetXattrs(req.GetXattrs())
	jobInstanceInfo.SetInstanceParameters(req.GetInstanceParameters())
	jobInstanceInfo.SetUpstreamData(convert2JobInstanceData(req.GetUpstreamData()))
	jobInstanceInfo.SetMaxAttempt(req.GetMaxAttempt())
	jobInstanceInfo.SetAttempt(req.GetAttempt())
	jobInstanceInfo.SetWfInstanceId(req.GetWfInstanceId())
	jobInstanceInfo.SetJobName(req.GetJobName())
	return jobInstanceInfo
}

func (a *jobInstanceActor) handleKillTask(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) {
	req := msg.Msg.(*schedulerx.ServerKillTaskRequest)
	logger.Infof("handleKillTask, jobInstanceId=%d", req.GetJobInstanceId())
	var (
		resp          = new(schedulerx.ServerKillTaskResponse)
		jobInstanceId = req.GetJobInstanceId()
	)
	if !a.taskmasterPool.Contains(jobInstanceId) {
		resp = &schedulerx.ServerKillTaskResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("jobInstanceId=%d is not existed", jobInstanceId)),
		}
	} else {
		uniqueId := utils.GetUniqueId(req.GetJobId(), req.GetJobInstanceId(), req.GetTaskId())
		if mapMaster, ok := a.taskmasterPool.Get(jobInstanceId).(taskmaster.MapTaskMaster); ok {
			mapMaster.KillTask(uniqueId, req.GetWorkerId(), req.GetWorkerAddr())
			resp = &schedulerx.ServerKillTaskResponse{
				Success: proto.Bool(true),
			}
		} else {
			logger.Warnf("taskmaster get form taskmasterPool is not mapTaskMaster, jobInstanceId=%d, taskmaster=%+v", jobInstanceId, mapMaster)
		}
	}

	actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
}

func (a *jobInstanceActor) handCheckTaskMaster(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) {
	req := msg.Msg.(*schedulerx.ServerCheckTaskMasterRequest)
	logger.Infof("handCheckTaskMaster, jobInstanceId=%d", req.GetJobInstanceId())
	var (
		resp          = new(schedulerx.ServerCheckTaskMasterResponse)
		jobInstanceId = req.GetJobInstanceId()
	)
	if !a.taskmasterPool.Contains(jobInstanceId) {
		resp = &schedulerx.ServerCheckTaskMasterResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("TaskMaster is not existed of jobInstance=%d", jobInstanceId)),
		}
	} else {
		resp = &schedulerx.ServerCheckTaskMasterResponse{
			Success: proto.Bool(true),
		}
	}
	actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
}

func (a *jobInstanceActor) handleInitPull(actorCtx actor.Context, msg *actorcomm.SchedulerWrappedMsg) {
	req := msg.Msg.(*schedulerx.MasterNotifyWorkerPullRequest)
	logger.Infof("handleInitPull, jobInstanceId=%d", req.GetJobInstanceId())
	// Pull 模式将要废弃，所以这里不处理直接返回
	resp := &schedulerx.MasterNotifyWorkerPullResponse{Success: proto.Bool(true)}
	actorCtx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), actorcomm.WrapSchedulerxMsg(msg.Ctx, resp, msg.SenderPath))
}

func (a *jobInstanceActor) handleReportWorkerStatus(actorCtx actor.Context, msg interface{}) {
	go func() {
		for i := 0; i < 3; i++ {
			f := actorCtx.RequestFuture(actorcomm.SchedulerxServerPid(context.Background()), msg, 5*time.Second)
			if err := f.Wait(); err != nil {
				logger.Warnf("handleReportWorkerStatus failed, retry times=%d, err=%s", i, err.Error())
				time.Sleep(30 * time.Second)
				continue
			}
			break
		}
	}()
}
