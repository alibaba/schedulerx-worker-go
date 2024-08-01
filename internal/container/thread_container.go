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

package container

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ Container = &ThreadContainer{}

type ThreadContainer struct {
	jobCtx        *jobcontext.JobContext
	actorCtx      actor.Context
	jobProcessor  processor.Processor
	containerPool ContainerPool
	masterPid     *actor.PID
}

func NewThreadContainer(jobCtx *jobcontext.JobContext, actorCtx actor.Context, containerPool ContainerPool) (*ThreadContainer, error) {
	if jobCtx.InstanceMasterActorPath() == "" {
		return nil, fmt.Errorf("get taskMaster akka path error, path=" + jobCtx.InstanceMasterActorPath())
	}
	workerAddr := actorcomm.GetRealWorkerAddr(jobCtx.InstanceMasterActorPath())
	return &ThreadContainer{
		jobCtx:        jobCtx,
		actorCtx:      actorCtx,
		containerPool: containerPool,
		masterPid:     actorcomm.GetMapMasterPid(workerAddr),
	}, nil
}

func (c *ThreadContainer) Run() {
	c.Start()
}

func (c *ThreadContainer) Start() {
	taskMasterPool := masterpool.GetTaskMasterPool()
	uniqueId := utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId())
	c.containerPool.SetContext(c.jobCtx)

	startTime := time.Now().UnixMilli()
	logger.Debugf("start run container, uniqueId=%v, cost=%vms, jobContext=%+v", uniqueId, startTime-c.jobCtx.ScheduleTime().Milliseconds(), c.jobCtx)

	defer func() {
		// clean containerPool
		c.containerPool.Remove(uniqueId)
		c.containerPool.RemoveContext()

		if e := recover(); e != nil {
			logger.Infof("Start run container panic, error=%v, stack=%s", e, debug.Stack())
		}
	}()

	result := processor.NewProcessResult(processor.WithFailed())
	workerAddr := c.actorCtx.ActorSystem().Address()
	var err error
	if c.jobCtx.TaskAttempt() == 0 {
		c.reportTaskStatus(processor.NewProcessResult(processor.WithStatus(processor.InstanceStatusRunning)), workerAddr)
	}

	jobName := gjson.Get(c.jobCtx.Content(), "jobName").String()
	// Compatible with the existing Java language configuration mechanism
	if c.jobCtx.JobType() == "java" {
		jobName = gjson.Get(c.jobCtx.Content(), "className").String()
	}
	task, ok := taskMasterPool.Tasks().Find(jobName)
	if !ok {
		retMsg := fmt.Sprintf("jobName=%s not found, maybe forgot to register it by the client", c.jobCtx.JobName())
		result = processor.NewProcessResult(processor.WithFailed(), processor.WithResult(retMsg))
		c.reportTaskStatus(result, workerAddr)
		logger.Errorf("Process task=%s failed, because it's unregistered. ", jobName)
		return
	}

	result, err = task.Process(c.jobCtx)
	if err != nil {
		fixedErrMsg := err.Error()
		if errMsg := err.Error(); len(errMsg) > constants.InstanceResultSizeMax {
			fixedErrMsg = errMsg[:constants.InstanceResultSizeMax]
		}
		result = processor.NewProcessResult(processor.WithFailed(), processor.WithResult(fixedErrMsg))
		c.reportTaskStatus(result, workerAddr)
		logger.Errorf("Process task=%s failed, uniqueId=%v, serialNum=%v, err=%s ", c.jobCtx.TaskName(), uniqueId, c.jobCtx.SerialNum(), err.Error())
		return
	}

	endTime := time.Now().UnixMilli()
	logger.Debugf("container run finished, uniqueId=%v, cost=%dms", uniqueId, endTime-startTime)

	if result == nil {
		result = processor.NewProcessResult(processor.WithFailed(), processor.WithResult("result can't be null"))
	}

	// If the execution of the map model subtask (non-root task) fails, would be retried
	if c.jobCtx.MaxAttempt() > 0 && c.jobCtx.TaskId() > 0 && result.Status() == processor.InstanceStatusFailed {
		if taskAttempt := c.jobCtx.TaskAttempt(); taskAttempt < c.jobCtx.MaxAttempt() {
			taskAttempt++
			time.Sleep(time.Duration(c.jobCtx.TaskAttemptInterval()) * time.Second)
			c.jobCtx.SetTaskAttempt(taskAttempt)
			c.Start()

			// No need to return the current result status when retrying
			return
		}
	}

	c.reportTaskStatus(result, workerAddr)
}

func (c *ThreadContainer) Kill() {
	logger.Infof("kill container, jobInstanceId=%v, content=%s", c.jobCtx.JobInstanceId(), c.jobCtx.Content())
	jobName := gjson.Get(c.jobCtx.Content(), "jobName").String()
	if jobName != "" {
		taskMasterPool := masterpool.GetTaskMasterPool()
		task, ok := taskMasterPool.Tasks().Find(jobName)
		if !ok {
			logger.Warnf("Kill task=%s failed, because it's not found. ", jobName)
		}
		task.Kill(c.jobCtx)
	}

	workerAddr := c.actorCtx.ActorSystem().Address()
	req := &schedulerx.ContainerReportTaskStatusRequest{
		JobId:         proto.Int64(c.jobCtx.JobId()),
		JobInstanceId: proto.Int64(c.jobCtx.JobInstanceId()),
		TaskId:        proto.Int64(c.jobCtx.TaskId()),
		TaskName:      proto.String(c.jobCtx.TaskName()),
		Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
		WorkerId:      proto.String(utils.GetWorkerId()),
		WorkerAddr:    proto.String(workerAddr),
		Result:        proto.String("killed"),
	}
	c.actorCtx.Send(c.masterPid, req)

	uniqueId := utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId())
	c.containerPool.Remove(uniqueId)
}

func (c *ThreadContainer) reportTaskStatus(result *processor.ProcessResult, workerAddr string) {
	req := &schedulerx.ContainerReportTaskStatusRequest{
		JobId:                   proto.Int64(c.jobCtx.JobId()),
		JobInstanceId:           proto.Int64(c.jobCtx.JobInstanceId()),
		TaskId:                  proto.Int64(c.jobCtx.TaskId()),
		Status:                  proto.Int32(int32(result.Status())),
		WorkerAddr:              proto.String(workerAddr),
		WorkerId:                proto.String(utils.GetWorkerId()),
		SerialNum:               proto.Int64(c.jobCtx.SerialNum()),
		InstanceMasterActorPath: proto.String(c.jobCtx.InstanceMasterActorPath()),
	}
	if c.jobCtx.TaskName() != "" {
		req.TaskName = proto.String(c.jobCtx.TaskName())
	}
	if result.Result() != "" {
		req.Result = proto.String(result.Result())
	}

	submitResult := false
	if config.GetWorkerConfig().IsShareContainerPool() {
		submitResult = batch.GetContainerStatusReqHandlerPool().SubmitReq(0, req)
	} else {
		submitResult = batch.GetContainerStatusReqHandlerPool().SubmitReq(c.jobCtx.JobInstanceId(), req)
	}
	logger.Debugf("reportTaskStatus instanceId=%v submitResult=%v, processResult=%v", utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId()), submitResult, result)
	if !submitResult {
		c.actorCtx.Request(c.masterPid, req)
	}
}

func (c *ThreadContainer) GetContext() *jobcontext.JobContext {
	return c.jobCtx
}

func (c *ThreadContainer) SetContext(context *jobcontext.JobContext) {
	c.jobCtx = context
}

func (c *ThreadContainer) Stop() {
	//TODO implement me
	panic("implement me")
}
