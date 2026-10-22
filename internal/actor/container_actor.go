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
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
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

var _ actor.Actor = (*containerActor)(nil)

var defaultActorPool, _ = ants.NewPool(
	math.MaxInt32,
	ants.WithPanicHandler(func(i interface{}) {
		if r := recover(); r != nil {
			logger.Errorf("Panic happened in containerStarter, %v\n%s", r, debug.Stack())
		}
	}))

type containerActor struct {
	containerPool             container.Pool
	statusReqBatchHandlerPool *batch.ContainerStatusReqHandlerPool
	batchSize                 int32
	containerStarter          *ants.Pool
	*containerAdmissionState
}

type containerAdmissionState struct {
	admissionMu          sync.Mutex
	instances            map[int64]*instanceAdmission
	admissionExpirations list.List
}

func newContainerActor() *containerActor {
	return newContainerActorWithState(new(containerAdmissionState))
}

func newContainerActorWithState(state *containerAdmissionState) *containerActor {
	return &containerActor{
		containerAdmissionState:   state,
		batchSize:                 config.GetWorkerConfig().WorkerMapPageSize(),
		statusReqBatchHandlerPool: batch.GetContainerStatusReqHandlerPool(),
		containerPool:             container.GetThreadContainerPool(),
		containerStarter:          defaultActorPool,
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
	case actor.SystemMessage:
		return
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
	sender := actorCtx.Sender()
	err := a.containerStarter.Submit(func() {
		for _, startReq := range req.StartReqs {
			uniqueId, err := a.startContainer(actorCtx, startReq)
			if err != nil {
				// The batch Sender is a short-lived RequestFuture, not the master.
				// Preserve the existing envelope ACK and report rejected tasks to the
				// actual master router, with the original serial and failure reason.
				report := a.convert2ContainerReportTaskStatusRequest(actorCtx, startReq, taskstatus.TaskStatusFailed, err.Error())
				if startReq.GetInstanceMasterAkkaPath() != "" {
					workerAddr := actorcomm.GetRealWorkerAddr(startReq.GetInstanceMasterAkkaPath())
					actorCtx.Send(actorcomm.GetMapMasterPid(workerAddr), report)
				} else {
					logger.Errorf("cannot route rejected task status: missing master path, jobInstanceId=%d taskId=%d err=%v", startReq.GetJobInstanceId(), startReq.GetTaskId(), err)
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
	if senderPid := sender; senderPid != nil {
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

// A close fence contains only IDs, never containers or task payloads. Do not
// evict unexpired fences on capacity pressure: reject new admission instead of
// silently allowing a late message to reopen a closed cycle.
const (
	closedInstanceRetention = time.Hour
	maxInstanceAdmissions   = 65536
)

type instanceAdmission struct {
	instanceID int64
	jobID      int64
	serial     int64
	attempt    int32
	closed     bool
	expires    time.Time
	element    *list.Element
}

// Both close fences and inactive open records have a bounded retention.
// A missed Master Destroy must not permanently consume admission capacity.
func (a *containerActor) refreshAdmissionExpiry(state *instanceAdmission, now time.Time) {
	if state.element != nil {
		a.admissionExpirations.Remove(state.element)
	}
	state.expires = now.Add(closedInstanceRetention)
	state.element = a.admissionExpirations.PushBack(state)
}

func (a *containerActor) pruneInstanceAdmissions(now time.Time) {
	for element := a.admissionExpirations.Front(); element != nil; element = a.admissionExpirations.Front() {
		state := element.Value.(*instanceAdmission)
		if now.Before(state.expires) {
			break
		}
		if !state.closed {
			handler := a.statusReqBatchHandlerPool.Get(state.instanceID)
			if a.containerPool.IsInstanceActive(state.instanceID) || handler != nil && handler.IsActive() {
				a.refreshAdmissionExpiry(state, now)
				continue
			}
			a.statusReqBatchHandlerPool.Stop(state.instanceID)
		}
		delete(a.instances, state.instanceID)
		a.admissionExpirations.Remove(element)
		state.element = nil
	}
}

// Optional epoch fields preserve legacy callers. New workers always send both
// fields, so delayed cleanup from an older retry cannot close a newer attempt.
func lifecycleEpoch(state *instanceAdmission, jobID int64, serial *int64, attempt *int32) (int64, int32, bool, error) {
	var targetSerial int64
	var targetAttempt int32
	if state != nil {
		if jobID != 0 && state.jobID != 0 && state.jobID != jobID {
			return 0, 0, false, fmt.Errorf("container instance belongs to another job")
		}
		targetSerial, targetAttempt = state.serial, state.attempt
	}
	if attempt != nil {
		targetAttempt = *attempt
	}
	if serial != nil && !(state != nil && attempt == nil && *serial == 0) {
		targetSerial = *serial
	}
	stale := state != nil && (targetAttempt < state.attempt || targetAttempt == state.attempt && targetSerial < state.serial)
	return targetSerial, targetAttempt, stale, nil
}

func (a *containerActor) closeInstance(instanceID, jobID, serial int64) error {
	return a.closeInstanceEpoch(instanceID, jobID, &serial, nil)
}

func (a *containerActor) closeInstanceEpoch(instanceID, jobID int64, serial *int64, attempt *int32) error {
	now := time.Now()
	a.pruneInstanceAdmissions(now)
	if a.instances == nil {
		a.instances = make(map[int64]*instanceAdmission)
	}
	state := a.instances[instanceID]
	targetSerial, targetAttempt, stale, err := lifecycleEpoch(state, jobID, serial, attempt)
	if err != nil || stale {
		return err
	}
	if state == nil {
		if len(a.instances) >= maxInstanceAdmissions {
			return fmt.Errorf("container instance admission history is full")
		}
		state = &instanceAdmission{instanceID: instanceID, jobID: jobID}
		a.instances[instanceID] = state
	}
	if state.closed && targetSerial == state.serial && targetAttempt == state.attempt {
		return nil
	}
	if state.jobID == 0 {
		state.jobID = jobID
	}
	state.serial, state.attempt = targetSerial, targetAttempt
	state.closed = true
	a.refreshAdmissionExpiry(state, now)
	return nil
}

func (a *containerActor) handleKillContainer(actorCtx actor.Context, req *schedulerx.MasterKillContainerRequest) {
	jobID, instanceID, taskID := req.GetJobId(), req.GetJobInstanceId(), req.GetTaskId()
	a.admissionMu.Lock()
	var toKill []container.Container
	state := a.instances[instanceID]
	serial, attempt, stale, err := lifecycleEpoch(state, jobID, req.SerialNum, req.Attempt)
	if err == nil && !stale {
		if taskID != 0 {
			// A task-level kill applies only to the matching execution generation.
			if state != nil && serial == state.serial && attempt == state.attempt {
				if jobID == 0 {
					jobID = state.jobID
				}
				uniqueID := utils.GetUniqueId(jobID, instanceID, taskID)
				if c := a.containerPool.Get(uniqueID); c != nil {
					a.containerPool.Cancel(uniqueID, c)
					toKill = append(toKill, c)
				}
			}
		} else {
			err = a.closeInstanceEpoch(instanceID, jobID, req.SerialNum, req.Attempt)
			if err == nil {
				if jobID == 0 && state != nil {
					jobID = state.jobID
				}
				// Match the separator too: instance 1001 must not match 10010.
				prefix := utils.GetUniqueIdWithoutTaskId(jobID, instanceID) + utils.SplitterToken
				a.containerPool.GetContainerMap().Range(func(key, value any) bool {
					if strings.HasPrefix(key.(string), prefix) {
						toKill = append(toKill, value.(container.Container))
					}
					return true
				})
				a.containerPool.DestroyByInstance(instanceID)
				a.statusReqBatchHandlerPool.Stop(instanceID)
			}
		}
	}
	a.admissionMu.Unlock()
	// User Kill callbacks may block or call back into the SDK. Never run them
	// under admission/queue locks.
	for _, c := range toKill {
		c.Kill()
	}
	response := &schedulerx.MasterKillContainerResponse{Success: proto.Bool(err == nil)}
	if err != nil {
		response.Message = proto.String(err.Error())
	}
	if sender := actorCtx.Sender(); sender != nil {
		actorCtx.Request(sender, response)
	}
}

func (a *containerActor) handleDestroyContainerPool(actorCtx actor.Context, req *schedulerx.MasterDestroyContainerPoolRequest) {
	a.admissionMu.Lock()
	_, _, stale, err := lifecycleEpoch(a.instances[req.GetJobInstanceId()], req.GetJobId(), req.SerialNum, req.Attempt)
	if err == nil && !stale {
		err = a.closeInstanceEpoch(req.GetJobInstanceId(), req.GetJobId(), req.SerialNum, req.Attempt)
		if err == nil {
			a.containerPool.DestroyByInstance(req.GetJobInstanceId())
			a.statusReqBatchHandlerPool.Stop(req.GetJobInstanceId())
		}
	}
	a.admissionMu.Unlock()
	response := &schedulerx.MasterDestroyContainerPoolResponse{Success: proto.Bool(err == nil), DeliveryId: proto.Int64(req.GetDeliveryId())}
	if err != nil {
		response.Message = proto.String(err.Error())
	}
	if sender := actorCtx.Sender(); sender != nil {
		actorCtx.Send(sender, response)
	}
}

func (a *containerActor) startContainer(actorCtx actor.Context, req *schedulerx.MasterStartContainerRequest) (string, error) {
	uniqueID := utils.GetUniqueId(req.GetJobId(), req.GetJobInstanceId(), req.GetTaskId())
	jobCtx, err := convertMasterStartContainerRequest2JobContext(req)
	if err != nil {
		return "", err
	}
	tc, err := container.NewThreadContainer(jobCtx, actorCtx)
	if err != nil {
		return "", err
	}
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	a.pruneInstanceAdmissions(time.Now())
	if a.instances == nil {
		a.instances = make(map[int64]*instanceAdmission)
	}
	state := a.instances[req.GetJobInstanceId()]
	if state != nil {
		if state.jobID != 0 && state.jobID != req.GetJobId() {
			return "", fmt.Errorf("container instance belongs to another job")
		}
		newer := req.GetAttempt() > state.attempt || req.GetAttempt() == state.attempt && req.GetSerialNum() > state.serial
		if !newer && (state.closed || req.GetAttempt() < state.attempt || req.GetSerialNum() < state.serial) {
			return uniqueID, nil
		}
		if state.jobID == 0 {
			state.jobID = req.GetJobId()
		}
		if newer {
			// Keep the prior expiry node until Submit succeeds; a submission
			// failure must not leave an untracked open admission behind.
			a.containerPool.DestroyByInstance(req.GetJobInstanceId())
			a.statusReqBatchHandlerPool.Stop(req.GetJobInstanceId())
			state.serial, state.attempt, state.closed = req.GetSerialNum(), req.GetAttempt(), false
		}
	} else {
		if len(a.instances) >= maxInstanceAdmissions {
			return "", fmt.Errorf("container instance admission history is full")
		}
	}
	// Only suppress duplicate deliveries that are queued or still executing.
	// Completion removes the entry, retaining explicit same-cycle task retries.
	if a.containerPool.Contain(uniqueID) {
		return uniqueID, nil
	}
	var consumerNum int32
	switch common.ExecuteMode(req.GetExecuteMode()) {
	case common.ParallelExecuteMode, common.GridExecuteMode, common.BatchExecuteMode:
		consumerNum = req.GetConsumerNum()
		if consumerNum <= 0 {
			consumerNum = constants.DefaultXAttrsConsumerSize
		}
	}
	key := req.GetJobInstanceId()
	handler := a.statusReqBatchHandlerPool.Get(key)
	created := handler == nil
	if created {
		queue := batch.NewReqQueue(config.GetWorkerConfig().QueueSize())
		handler = batch.NewContainerStatusReqHandler(key, 1, 1, a.batchSize, queue, req.GetInstanceMasterAkkaPath())
		a.statusReqBatchHandlerPool.Start(key, handler)
	}
	tc.SetStatusHandler(handler)
	if err = a.containerPool.Submit(req.GetJobId(), key, req.GetTaskId(), tc, consumerNum); err != nil {
		if created {
			a.statusReqBatchHandlerPool.Stop(key)
		}
		return "", err
	}
	if state == nil {
		state = &instanceAdmission{instanceID: key, jobID: req.GetJobId(), serial: req.GetSerialNum(), attempt: req.GetAttempt()}
		a.instances[key] = state
	}
	a.refreshAdmissionExpiry(state, time.Now())
	return uniqueID, nil
}

func convertMasterStartContainerRequest2JobContext(req *schedulerx.MasterStartContainerRequest) (*jobcontext.JobContext, error) {
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
	jobCtx.SetTriggerType(req.GetTriggerType())
	jobCtx.Context = context.Background()
	return jobCtx, nil
}
