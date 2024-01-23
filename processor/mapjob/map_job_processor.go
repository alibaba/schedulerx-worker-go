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

package mapjob

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob/bizsubtask"
)

var _ processor.MapJobProcessor = &MapJobProcessor{}

const maxRetryCount = 3

type MapJobProcessor struct {
	processor.Processor
	taskMasterPool *masterpool.TaskMasterPool
	actorSystem    *actor.ActorSystem
}

func NewMapJobProcessor() *MapJobProcessor {
	return &MapJobProcessor{
		taskMasterPool: masterpool.GetTaskMasterPool(),
		actorSystem:    actorcomm.GetActorSystem(),
	}
}

// checkTaskObject check subtask object information
func (rcvr *MapJobProcessor) checkTaskObject(jobCtx *jobcontext.JobContext, taskObject interface{}) error {
	// FIXME
	isAdvancedVersion := false
	//context := ContainerFactory.getContainerPool().getContext()
	//isAdvancedVersion := GroupManager.INSTANCE.isAdvancedVersion(context.getGroupId())

	if bizSubTask, ok := taskObject.(bizsubtask.BizSubTask); isAdvancedVersion && ok {
		labelMap := bizSubTask.LabelMap()
		if len(labelMap) > 3 {
			return fmt.Errorf("label map size can't beyond 3")
		}
		for key, val := range labelMap {
			if len(key) > 60 || len(val) > 180 {
				logger.Errorf("Job instance=%d label map<%s, %s> content can't beyond max size(60,180).", jobCtx.JobInstanceId(), key, val)
				return fmt.Errorf("label map content can't beyond max size(60,180)")
			}
		}
	}
	return nil
}

func (rcvr *MapJobProcessor) handleMapTask(jobCtx *jobcontext.JobContext, taskMaster taskmaster.TaskMaster, request *schedulerx.WorkerMapTaskRequest) (*schedulerx.WorkerMapTaskResponse, error) {
	var (
		resp          *schedulerx.WorkerMapTaskResponse
		jobInstanceId = request.GetJobInstanceId()
	)

	if taskMaster != nil {
		if mapTaskMaster, ok := taskMaster.(taskmaster.MapTaskMaster); !ok {
			resp = &schedulerx.WorkerMapTaskResponse{
				Success: proto.Bool(false),
				Message: proto.String("TaskMaster is not MapTaskMaster"),
			}
			if err := taskMaster.UpdateNewInstanceStatus(taskMaster.GetSerialNum(), processor.InstanceStatusFailed, "TaskMaster is not MapTaskMaster"); err != nil {
				errMsg := fmt.Sprintf("jobInstanceId=%d, UpdateNewInstanceStatus failed, err=%s", jobInstanceId, err.Error())
				logger.Errorf(errMsg)
				return &schedulerx.WorkerMapTaskResponse{
					Success: proto.Bool(false),
					Message: proto.String(errMsg),
				}, nil
			}
		} else {
			startTime := time.Now()
			overload, err := mapTaskMaster.Map(jobCtx, request.GetTaskBody(), request.GetTaskName())
			if err != nil {
				logger.Errorf("jobInstanceId=%s map failed, err=%s", err.Error())
				if err := taskMaster.UpdateNewInstanceStatus(taskMaster.GetSerialNum(), processor.InstanceStatusFailed, err.Error()); err != nil {
					errMsg := fmt.Sprintf("jobInstanceId=%d, UpdateNewInstanceStatus failed, err=%s", jobInstanceId, err.Error())
					logger.Errorf(errMsg)
					return &schedulerx.WorkerMapTaskResponse{
						Success: proto.Bool(false),
						Message: proto.String(errMsg),
					}, nil
				}
				return nil, err
			}
			logger.Debugf("jobInstanceId=%d map, cost=%sms", jobInstanceId, time.Since(startTime).Milliseconds())
			resp = &schedulerx.WorkerMapTaskResponse{
				Success:  proto.Bool(true),
				Overload: proto.Bool(overload),
			}
		}
	} else {
		resp = &schedulerx.WorkerMapTaskResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("can't found TaskMaster by jobInstanceId=%d", jobInstanceId)),
		}
	}

	return resp, nil
}

func (rcvr *MapJobProcessor) IsRootTask(jobCtx *jobcontext.JobContext) bool {
	return jobCtx.TaskName() == constants.MapTaskRootName
}

// Map distribute tasks to all workers.
// Every element in taskList shouldn't beyond 64KB.
func (rcvr *MapJobProcessor) Map(jobCtx *jobcontext.JobContext, taskList []interface{}, taskName string) (*processor.ProcessResult, error) {
	var (
		result = processor.NewProcessResult(processor.WithFailed())
	)

	if len(taskList) == 0 {
		result.SetResult("task list is empty")
		return result, nil
	}

	workerAddr := actorcomm.GetRealWorkerAddr(jobCtx.InstanceMasterActorPath())
	mapMasterPid := actorcomm.GetMapMasterPid(workerAddr)
	if mapMasterPid == nil {
		errMsg := fmt.Sprintf("%v%v", "get taskMaster akka path error, path=", jobCtx.InstanceMasterActorPath())
		logger.Errorf(errMsg)
		result.SetResult(errMsg)
		return result, nil
	}

	batchSize := int(config.GetWorkerConfig().WorkerMapPageSize())
	size := len(taskList)
	quotient := size / batchSize
	remainder := size % batchSize
	// map taskList in #batchNumber batch, every batch has no more than 3000 tasks;
	// int batchNumber = remainder > 0 ? quotient + 1 : quotient;
	batchNumber := quotient
	if remainder > 0 {
		batchNumber = quotient + 1
	}
	logger.Infof("map task list, jobInstanceId=%d, taskName=%s, size=%d, batchSize=%d, batchNumber=%d",
		jobCtx.JobInstanceId(), taskName, size, batchSize, batchNumber)

	reqs := make([]*schedulerx.WorkerMapTaskRequest, 0, batchNumber)
	for i := 0; i < batchNumber; i++ {
		reqs = append(reqs, new(schedulerx.WorkerMapTaskRequest))
	}

	position := 0
	maxTaskBodySize := int(config.GetWorkerConfig().TaskBodySizeMax())
	for _, task := range taskList {
		rcvr.checkTaskObject(jobCtx, task)
		batchIdx := position / batchSize
		position++

		taskBody, err := json.Marshal(task)
		if err != nil {
			return nil, fmt.Errorf("json marshal task=%+v failed, err=%s", task, err.Error())
		}
		if len(taskBody) > maxTaskBodySize {
			return nil, fmt.Errorf("taskBody size more than %dB", maxTaskBodySize)
		}
		if reqs[batchIdx].TaskBody == nil {
			reqs[batchIdx].TaskBody = [][]byte{taskBody}
		} else {
			reqs[batchIdx].TaskBody = append(reqs[batchIdx].TaskBody, taskBody)
		}
	}

	position = 0
	for _, req := range reqs {
		req.JobId = proto.Int64(jobCtx.JobId())
		req.JobInstanceId = proto.Int64(jobCtx.JobInstanceId())
		req.TaskId = proto.Int64(jobCtx.TaskId())
		req.TaskName = proto.String(taskName)

		var (
			resp *schedulerx.WorkerMapTaskResponse
			err  error
			ret  interface{}

			retryCount = 0
			ok         = false
		)

		taskMaster := rcvr.taskMasterPool.Get(req.GetJobInstanceId())
		if isMapTaskMaster(taskMaster) {
			// current worker is master worker
			resp, err = rcvr.handleMapTask(jobCtx, taskMaster, req)
		} else {
			// current worker isn't master worker, forward request to master worker
			ret, err = rcvr.actorSystem.Root.RequestFuture(mapMasterPid, req, 30*time.Second).Result()
			if errors.Is(err, actor.ErrTimeout) {
				logger.Warnf("JobInstanceId=%d WorkerMapTaskRequest dispatch failed, due to send request=%+v to taskMaster timeout.", req.GetJobInstanceId(), req)
				if retryCount < maxRetryCount {
					time.Sleep(10 * time.Millisecond)
					ret, err = rcvr.actorSystem.Root.RequestFuture(mapMasterPid, req, 30*time.Second).Result()
					retryCount++
				} else {
					return nil, fmt.Errorf("JobInstanceId=%d WorkerMapTaskRequest dispatch failed, due to send request=%+v to taskMaster timeout after retry exceed %d times, err=%s ", req.GetJobInstanceId(), req, retryCount, err.Error())
				}
			}
			if err == nil {
				resp, ok = ret.(*schedulerx.WorkerMapTaskResponse)
				if !ok {
					err = fmt.Errorf("Response send request=%+v to taskMaster is not WorkerMapTaskResponse, response=%+v ", req, ret)
				}
			}
		}

		if err != nil {
			return nil, fmt.Errorf("JobInstanceId=%d WorkerMapTaskRequest dispatch error, due to send request=%+v to taskMaster failed, err=%s ", req.GetJobInstanceId(), req, err.Error())
		}

		if !resp.GetSuccess() {
			logger.Errorf(resp.GetMessage())
			result.SetResult(resp.GetMessage())
			return result, nil
		}

		reqs[position] = nil
		position++

		if resp.GetOverload() {
			logger.Warnf("Task Master is busy, sleeping a while 10s...")
			time.Sleep(10 * time.Second)
		}
	}
	result.SetSucceed()
	return result, nil
}

func (rcvr *MapJobProcessor) Kill(jobCtx *jobcontext.JobContext) error {
	//TODO implement me
	panic("implement me")
}

func isMapTaskMaster(taskMaster taskmaster.TaskMaster) bool {
	if taskMaster == nil {
		return false
	}
	_, ok := taskMaster.(taskmaster.MapTaskMaster)
	return ok
}
