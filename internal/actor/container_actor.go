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
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/container"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ actor.Actor = &containerActor{}

type containerActor struct {
	enableShareContainerPool  bool
	containerPool             container.ContainerPool
	statusReqBatchHandlerPool *batch.ContainerStatusReqHandlerPool
	batchSize                 int32
	containerStarter          *ants.Pool
	lock                      sync.Mutex
}

func newContainerActor() *containerActor {
	gopool, _ := ants.NewPool(
		ants.DefaultAntsPoolSize,
		ants.WithPanicHandler(func(i interface{}) {
			if r := recover(); r != nil {
				logger.Errorf("Panic happened in containerStarter, %v\n%s", r, debug.Stack())
			}
		}))
	return &containerActor{
		enableShareContainerPool:  config.GetWorkerConfig().IsShareContainerPool(),
		batchSize:                 config.GetWorkerConfig().WorkerMapPageSize(),
		statusReqBatchHandlerPool: batch.GetContainerStatusReqHandlerPool(),
		containerPool:             container.GetThreadContainerPool(),
		containerStarter:          gopool,
		lock:                      sync.Mutex{},
	}
}

func (a *containerActor) Receive(actorCtx actor.Context) {
	switch msg := actorCtx.Message().(type) {
	case *schedulerx.MasterStartContainerRequest:
		a.handleStartContainer(actorCtx, msg)
	case *schedulerx.MasterBatchStartContainersRequest:
		a.handleBatchStartContainers(actorCtx, msg)
	case *schedulerx.MasterKillContainerRequest:
		a.handleKillContainer(actorCtx, msg)
	case *schedulerx.MasterDestroyContainerPoolRequest:
		a.handleDestroyContainerPool(actorCtx, msg)
	default:
		logger.Warnf("[containerActor] receive unknown message, msg=%+v", actorCtx.Message())
	}
}

func (a *containerActor) handleStartContainer(actorCtx actor.Context, req *schedulerx.MasterStartContainerRequest) {
	resp := new(schedulerx.MasterStartContainerResponse)
	uniqueId, err := a.startContainer(actorCtx, req)
	if err != nil {
		logger.Errorf("submit container to containerPool failed, err=%s, uniqueId=%v, cost=%vms", err.Error(), uniqueId, time.Now().UnixMilli()-req.GetScheduleTime())
		resp = &schedulerx.MasterStartContainerResponse{
			Success: proto.Bool(false),
			Message: proto.String(err.Error()),
		}
		if senderPid := actorCtx.Sender(); senderPid != nil {
			actorCtx.Send(senderPid, resp)
		} else {
			logger.Warnf("Cannot send MasterStartContainerResponse due to sender is unknown in handleStartContainer of containerActor, request=%+v", req)
		}
		return
	}

	logger.Debugf("submit container to containerPool, uniqueId=%v, cost=%vms", uniqueId, time.Now().UnixMilli()-req.GetScheduleTime())
	resp = &schedulerx.MasterStartContainerResponse{Success: proto.Bool(true)}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Send(senderPid, resp)
	} else {
		logger.Warnf("Cannot send MasterStartContainerResponse due to sender is unknown in handleStartContainer of containerActor, request=%+v", req)
	}
}

func (a *containerActor) handleBatchStartContainers(actorCtx actor.Context, req *schedulerx.MasterBatchStartContainersRequest) {
	err := a.containerStarter.Submit(func() {
		for _, startReq := range req.StartReqs {
			uniqueId, err := a.startContainer(actorCtx, startReq)
			if err != nil {
				// report task fail status to task master
				reportTaskStatusReq := &schedulerx.ContainerReportTaskStatusRequest{
					JobId:         proto.Int64(startReq.GetJobId()),
					JobInstanceId: proto.Int64(startReq.GetJobInstanceId()),
					TaskId:        proto.Int64(startReq.GetTaskId()),
					Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
					WorkerId:      proto.String(utils.GetWorkerId()),
					WorkerAddr:    proto.String(actorCtx.ActorSystem().Address()),
				}
				if startReq.GetTaskName() != "" {
					reportTaskStatusReq.TaskName = proto.String(startReq.GetTaskName())
				}
				if senderPid := actorCtx.Sender(); senderPid != nil {
					actorCtx.Send(senderPid, reportTaskStatusReq)
				} else {
					logger.Warnf("Cannot send ContainerReportTaskStatusRequest due to sender is unknown in handleBatchStartContainers of containerActor, request=%+v", req)
				}
			}
			logger.Debugf("submit container to containerPool, uniqueId=%v, cost=%vms", uniqueId, time.Now().UnixMilli()-startReq.GetScheduleTime())
		}
	})
	resp := new(schedulerx.MasterBatchStartContainersResponse)
	if err != nil {
		logger.Errorf("handleBatchStartContainers failed, err=%s", err.Error())
		resp = &schedulerx.MasterBatchStartContainersResponse{
			Success: proto.Bool(false),
			Message: proto.String(err.Error()),
		}
	} else {
		resp = &schedulerx.MasterBatchStartContainersResponse{
			Success: proto.Bool(true),
		}
	}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Request(senderPid, resp)
	} else {
		logger.Warnf("Cannot send MasterBatchStartContainersResponse due to sender is unknown in handleBatchStartContainers of containerActor, request=%+v", req)
	}
}

func (a *containerActor) convert2ContainerReportTaskStatusRequest(actorCtx actor.Context, req *schedulerx.MasterStartContainerRequest,
	status taskstatus.TaskStatus, result string) *schedulerx.ContainerReportTaskStatusRequest {
	return &schedulerx.ContainerReportTaskStatusRequest{
		JobId:                   proto.Int64(req.GetJobId()),
		JobInstanceId:           proto.Int64(req.GetJobInstanceId()),
		TaskId:                  proto.Int64(req.GetTaskId()),
		TaskName:                proto.String(req.GetTaskName()),
		Status:                  proto.Int32(int32(status)),
		WorkerAddr:              proto.String(actorCtx.ActorSystem().Address()),
		Result:                  proto.String(result),
		WorkerId:                proto.String(utils.GetWorkerId()),
		InstanceMasterActorPath: proto.String(req.GetInstanceMasterAkkaPath()),
		TimeType:                proto.Int32(req.GetTimeType()),
		SerialNum:               proto.Int64(req.GetSerialNum()),
	}
}

func (a *containerActor) handleKillContainer(actorCtx actor.Context, req *schedulerx.MasterKillContainerRequest) {
	var (
		jobId         = req.GetJobId()
		jobInstanceId = req.GetJobInstanceId()
		taskId        = req.GetTaskId()
		uniqueId      = utils.GetUniqueId(jobId, jobInstanceId, taskId)
	)

	if req.GetTaskId() != 0 {
		// kill task container
		if a.containerPool.Contain(uniqueId) {
			a.containerPool.Get(uniqueId).Kill()
		}
		logger.Infof("kill task container success, uniqueId=%v", uniqueId)
	} else {
		uniqueId = utils.GetUniqueIdWithoutTaskId(jobId, jobInstanceId)
		// kill instance container pool
		a.killInstance(jobId, jobInstanceId)
		logger.Infof("kill instance success, uniqueId:%v", uniqueId)
	}
	resp := &schedulerx.MasterKillContainerResponse{
		Success: proto.Bool(true),
	}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Request(senderPid, resp)
	} else {
		logger.Warnf("Cannot send MasterKillContainerResponse due to sender is unknown in handleKillContainer of containerActor, request=%+v", req)
	}
}

func (a *containerActor) handleDestroyContainerPool(actorCtx actor.Context, req *schedulerx.MasterDestroyContainerPoolRequest) {
	if !a.enableShareContainerPool {
		handler, ok := a.statusReqBatchHandlerPool.GetHandlers().Load(req.GetJobInstanceId())
		if ok {
			a.lock.Lock()
			defer a.lock.Unlock()
			if h, ok := handler.(*batch.ContainerStatusReqHandler); ok {
				if latestRequest := h.GetLatestRequest(); latestRequest != nil {
					reportTaskStatusRequest, ok := latestRequest.(*schedulerx.ContainerReportTaskStatusRequest)
					if ok {
						if reportTaskStatusRequest.GetSerialNum() != req.GetSerialNum() {
							logger.Infof("skip handleDestroyContainerPool cycleId=%v_%v, handler serialNum=%v.", req.GetJobInstanceId(), req.GetSerialNum(), reportTaskStatusRequest.GetSerialNum())
							return
						}
					} else {
						logger.Infof("handleDestroyContainerPool from cycleId=%v_%v, handler serialNum=%v.", req.GetJobInstanceId(), req.GetSerialNum(), reportTaskStatusRequest.GetSerialNum())
						a.containerPool.DestroyByInstance(req.GetJobInstanceId())
						a.statusReqBatchHandlerPool.Stop(req.GetJobInstanceId())
					}
				}
			}
		}
	}
	response := &schedulerx.MasterDestroyContainerPoolResponse{
		Success:    proto.Bool(true),
		DeliveryId: proto.Int64(req.GetDeliveryId()),
	}
	if senderPid := actorCtx.Sender(); senderPid != nil {
		actorCtx.Send(senderPid, response)
	} else {
		logger.Warnf("Cannot send MasterKillContainerResponse due to sender is unknown in handleDestroyContainerPool of containerActor, request=%+v", req)
	}

	a.containerPool.ReleaseInstanceLock(req.GetJobInstanceId())
}

func (a *containerActor) killInstance(jobId, jobInstanceId int64) {
	containerMap := a.containerPool.GetContainerMap()
	prefixKey := fmt.Sprintf("%d%s%d", jobId, utils.SplitterToken, jobInstanceId)
	a.containerPool.GetContainerMap().Range(func(key, value any) bool {
		var (
			uniqueId  = key.(string)
			container = value.(container.Container)
		)
		if strings.HasPrefix(uniqueId, prefixKey) {
			container.Kill()
			containerMap.Delete(uniqueId)
		}
		return true
	})

	if !a.enableShareContainerPool {
		a.containerPool.DestroyByInstance(jobInstanceId)
	}
}

func (a *containerActor) startContainer(actorCtx actor.Context, req *schedulerx.MasterStartContainerRequest) (string, error) {
	uniqueId := utils.GetUniqueId(req.GetJobId(), req.GetJobInstanceId(), req.GetTaskId())
	logger.Debugf("startContainer, uniqueId=%v, req=%+v, cost=%vms", uniqueId, req, time.Now().UnixMilli()-req.GetScheduleTime())

	jobCtx, err := convertMasterStartContainerRequest2JobContext(req)
	if err != nil {
		return "", err
	}
	container, err := container.NewThreadContainer(jobCtx, actorCtx, container.GetThreadContainerPool())
	if err != nil {
		return "", err
	}
	if container != nil {
		a.lock.Lock()
		defer a.lock.Unlock()
		a.containerPool.Put(uniqueId, container)
		// Whether to share containerPool. If shared, statusReqBatchHandlerPool has only one handler with key=0.
		statusReqBatchHandlerKey := int64(0)
		if !a.enableShareContainerPool {
			statusReqBatchHandlerKey = req.GetJobInstanceId()
		}
		if !a.statusReqBatchHandlerPool.Contains(statusReqBatchHandlerKey) {
			// support 1.5 million requests
			reqQueue := batch.NewReqQueue(config.GetWorkerConfig().QueueSize())
			a.statusReqBatchHandlerPool.Start(
				statusReqBatchHandlerKey,
				batch.NewContainerStatusReqHandler(statusReqBatchHandlerKey, 1, 1, a.batchSize, reqQueue, req.GetInstanceMasterAkkaPath()))
		}
		consumerNum := int32(constants.ConsumerNumDefault)
		if req.GetConsumerNum() > 0 {
			consumerNum = req.GetConsumerNum()
		}
		if err = a.containerPool.Submit(req.GetJobId(), req.GetJobInstanceId(), req.GetTaskId(), container, consumerNum); err != nil {
			return "", err
		}
	} else {
		logger.Warnf("Container is null, uniqueId=%d", uniqueId)
	}
	return uniqueId, nil
}

func convertMasterStartContainerRequest2JobContext(req *schedulerx.MasterStartContainerRequest) (*jobcontext.JobContext, error) {
	jobCtx := new(jobcontext.JobContext)
	jobCtx.SetJobId(req.GetJobId())
	jobCtx.SetJobInstanceId(req.GetJobInstanceId())
	jobCtx.SetTaskId(req.GetTaskId())
	jobCtx.SetScheduleTime(time.Duration(req.GetScheduleTime()))
	jobCtx.SetDataTime(time.Duration(req.GetDataTime()))
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
	if req.GetTask() != nil {
		jobCtx.SetTask(req.GetTask())

		// If it's a sharding task, set the sharding id and sharding parameters.
		shardingTask := new(common.ShardingTask)
		if err := json.Unmarshal(req.GetTask(), shardingTask); err == nil {
			jobCtx.SetShardingId(shardingTask.GetId())
			jobCtx.SetShardingParameter(shardingTask.GetParameter())
		}
	} else {
		jobCtx.SetShardingId(req.GetTaskId())
	}
	jobCtx.SetTaskMaxAttempt(req.GetTaskMaxAttempt())
	jobCtx.SetTaskAttemptInterval(req.GetTaskAttemptInterval())

	upstreamData := make([]*common.JobInstanceData, 0, len(req.GetUpstreamData()))
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
	jobCtx.Context = context.Background()
	return jobCtx, nil
}
