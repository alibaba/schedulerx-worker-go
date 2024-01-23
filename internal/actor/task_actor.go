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
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
)

var _ actor.Actor = &taskActor{}

// taskActor is the
type taskActor struct {
	connpool       pool.ConnPool
	taskMasterPool *masterpool.TaskMasterPool
}

func newTaskActor(actorSystem *actor.ActorSystem) *taskActor {
	tActor := &taskActor{
		connpool:       pool.GetConnPool(),
		taskMasterPool: masterpool.GetTaskMasterPool(),
	}

	resolver := func(pid *actor.PID) (actor.Process, bool) {
		if actorcomm.IsSchedulerxServer(pid) {
			return newTaskProcessor(tActor.connpool), true
		}

		// If communicate with actors other than server, then use the default handler (return false)
		return nil, false
	}
	actorSystem.ProcessRegistry.RegisterAddressResolver(resolver)

	return tActor
}

func (a *taskActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *schedulerx.ContainerReportTaskStatusRequest:
		if err := a.handleTaskStatus(msg); err != nil {
			logger.Errorf("handleTaskStatus failed, err=%s", err.Error())
		}
	case *schedulerx.ContainerBatchReportTaskStatuesRequest:
		// send to atLeastOnceDeliveryRoutingActor
		a.handleBatchTaskStatues(ctx, msg)
	case *schedulerx.WorkerMapTaskRequest:
		if err := a.handleMapTask(ctx, msg); err != nil {
			logger.Errorf("handleMapTask failed, err=%s", err.Error())
		}
	case *schedulerx.WorkerMapTaskResponse:
		actorcomm.WorkerMapTaskRespMsgSender() <- msg
	case *schedulerx.PullTaskFromMasterRequest:
		a.handlePullTasks(ctx, msg)
	case *actorcomm.SchedulerWrappedMsg:
		switch innerMsg := msg.Msg.(type) {
		case *schedulerx.ContainerBatchReportTaskStatuesRequest:
			// send to atLeastOnceDeliveryRoutingActor
			a.handleBatchTaskStatues(ctx, innerMsg)
		case *schedulerx.WorkerMapTaskResponse:
			actorcomm.WorkerMapTaskRespMsgSender() <- innerMsg
		case *schedulerx.PullTaskFromMasterRequest:
			a.handlePullTasks(ctx, innerMsg)
		case *schedulerx.WorkerReportJobInstanceProgressRequest:
			// forward to server
			ctx.Send(actorcomm.SchedulerxServerPid(msg.Ctx), msg)
		case *schedulerx.WorkerBatchUpdateTaskStatusRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, msg, 5*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerBatchUpdateTaskStatusRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender() <- result.(*schedulerx.WorkerBatchUpdateTaskStatusResponse)
			}
		case *schedulerx.WorkerQueryJobInstanceStatusRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, msg, 30*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerQueryJobInstanceStatusRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerQueryJobInstanceStatusRespMsgSender() <- result.(*schedulerx.WorkerQueryJobInstanceStatusResponse)
			}
		case *schedulerx.WorkerClearTasksRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, msg, 5*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerClearTasksRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerClearTasksRespMsgSender() <- result.(*schedulerx.WorkerClearTasksResponse)
			}
		case *schedulerx.WorkerBatchCreateTasksRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, msg, 90*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerBatchCreateTasksRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerBatchCreateTasksRespMsgSender() <- result.(*schedulerx.WorkerBatchCreateTasksResponse)
			}
		case *schedulerx.WorkerPullTasksRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, msg, 30*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerPullTasksRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerPullTasksRespMsgSender() <- result.(*schedulerx.WorkerPullTasksResponse)
			}
		case *schedulerx.WorkerBatchReportTaskStatuesRequest:
			// forward to server
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			ctx.Send(serverPid, msg)
		case *schedulerx.WorkerReportTaskListStatusRequest:
			serverPid := actorcomm.SchedulerxServerPid(context.Background())
			result, err := ctx.RequestFuture(serverPid, innerMsg, 30*time.Second).Result()
			if err != nil {
				logger.Errorf("Send WorkerReportTaskListStatusRequest timeout, jobInstanceId=%d, serverAddr=%s", innerMsg.JobInstanceId, serverPid.Address)
			} else {
				actorcomm.WorkerReportTaskListStatusRespMsgSender() <- result.(*schedulerx.WorkerReportTaskListStatusResponse)
			}
		default:
			logger.Errorf("Receive unknown message in taskActor, msg=%+v", msg)
		}
	}
}

func (a *taskActor) handleTaskStatus(req *schedulerx.ContainerReportTaskStatusRequest) error {
	if taskMaster := a.taskMasterPool.Get(req.GetJobInstanceId()); taskMaster != nil {
		if err := taskMaster.UpdateTaskStatus(req); err != nil {
			return fmt.Errorf("jobInstanceId=%v, taskId=%v", req.GetJobInstanceId(), req.GetTaskId())
		}
	}
	return nil
}

func (a *taskActor) handleBatchTaskStatues(actorCtx actor.Context, req *schedulerx.ContainerBatchReportTaskStatuesRequest) {
	logger.Debugf("[taskActor] handleBatchTaskStatues, jobInstanceId=%v, batch receive task status reqs, size:%v, taskMasterPool=%v",
		req.GetJobInstanceId(), len(req.GetTaskStatues()), masterpool.GetTaskMasterPool())
	if taskMaster := masterpool.GetTaskMasterPool().Get(req.GetJobInstanceId()); taskMaster != nil {
		if err := taskMaster.BatchUpdateTaskStatus(taskMaster, req); err != nil {
			logger.Warnf("[taskActor] TaskMaster BatchUpdateTaskStatus failed in handleBatchTaskStatues, req=%+v, err=%s", req, err.Error())
		}
	}
	response := &schedulerx.ContainerBatchReportTaskStatuesResponse{
		Success:    proto.Bool(true),
		DeliveryId: proto.Int64(req.GetDeliveryId()),
	}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Request(senderPid, response)
	} else {
		logger.Warnf("Cannot send ContainerBatchReportTaskStatuesResponse due to sender is unknown in handleBatchTaskStatues of taskActor, request=%+v", req)
	}
}

func (a *taskActor) handleMapTask(actorCtx actor.Context, req *schedulerx.WorkerMapTaskRequest) error {
	var (
		jobInstanceId = req.GetJobInstanceId()
		response      = new(schedulerx.WorkerMapTaskResponse)
	)
	if taskMaster := a.taskMasterPool.Get(jobInstanceId); taskMaster != nil {
		if mapTaskMaster, ok := taskMaster.(taskmaster.MapTaskMaster); ok {
			startTime := time.Now().UnixMilli()
			overload, err := mapTaskMaster.Map(nil, req.GetTaskBody(), req.GetTaskName())
			if err != nil {
				errMsg := fmt.Sprintf("handleMapTask failed, due to jobInstanceId=%v map error, err=%v", jobInstanceId, err.Error())
				logger.Errorf(errMsg)
				taskMaster.UpdateNewInstanceStatus(taskMaster.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
				return fmt.Errorf(errMsg)
			}
			logger.Debugf("jobInstanceId=%v map, cost=%vms", jobInstanceId, time.Now().UnixMilli()-startTime)
			response = &schedulerx.WorkerMapTaskResponse{
				Success:  proto.Bool(true),
				Overload: proto.Bool(overload),
			}
		} else {
			response = &schedulerx.WorkerMapTaskResponse{
				Success: proto.Bool(false),
				Message: proto.String("TaskMaster is not MapTaskMaster"),
			}
			taskMaster.UpdateNewInstanceStatus(taskMaster.GetSerialNum(), processor.InstanceStatusFailed, "TaskMaster is not MapTaskMaster")
		}
	} else {
		response = &schedulerx.WorkerMapTaskResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("can't found TaskMaster by jobInstanceId=%d", jobInstanceId)),
		}
	}

	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Send(senderPid, response)
	} else {
		logger.Warnf("Cannot send WorkerMapTaskResponse due to sender is unknown in handleMapTask of taskActor, request=%+v", req)
	}
	return nil
}

func (a *taskActor) handlePullTasks(actorCtx actor.Context, req *schedulerx.PullTaskFromMasterRequest) {
	var (
		jobInstanceId = req.GetJobInstanceId()
		response      = new(schedulerx.PullTaskFromMasterResponse)
	)
	if taskMaster := a.taskMasterPool.Get(jobInstanceId); taskMaster != nil {
		if mapTaskMaster, ok := taskMaster.(taskmaster.MapTaskMaster); ok {
			response = &schedulerx.PullTaskFromMasterResponse{
				Success: proto.Bool(true),
				Request: mapTaskMaster.SyncPullTasks(req.GetPageSize(), req.GetWorkerIdAddr()),
			}
		} else {
			response = &schedulerx.PullTaskFromMasterResponse{
				Success: proto.Bool(false),
				Message: proto.String(fmt.Sprintf("%v%v", "TaskMaster is not MapTaskMaster, jobInstanceId=", jobInstanceId)),
			}
		}
	} else {
		response = &schedulerx.PullTaskFromMasterResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("%v%v", "TaskMaster is null, jobInstanceId=", jobInstanceId)),
		}
	}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Send(senderPid, response)
	} else {
		logger.Warnf("Cannot send PullTaskFromMasterResponse due to sender is unknown in handlePullTasks of taskActor, request=%+v", req)
	}
}
