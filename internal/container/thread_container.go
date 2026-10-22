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
	"sync"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/tracer"
)

var _ Container = (*ThreadContainer)(nil)

type ThreadContainer struct {
	jobCtx        *jobcontext.JobContext
	actorCtx      actor.Context
	jobProcessor  processor.Processor
	containerPool Pool
	masterPid     *actor.PID
	statusHandler *batch.ContainerStatusReqHandler
	killed        atomic.Bool
	statusMu      sync.Mutex
	finished      bool
}

func NewThreadContainer(jobCtx *jobcontext.JobContext, actorCtx actor.Context) (*ThreadContainer, error) {
	if jobCtx.InstanceMasterActorPath() == "" {
		return nil, fmt.Errorf("get taskMaster akka path error, path=" + jobCtx.InstanceMasterActorPath())
	}
	workerAddr := actorcomm.GetRealWorkerAddr(jobCtx.InstanceMasterActorPath())
	return &ThreadContainer{
		jobCtx:        jobCtx,
		actorCtx:      actorCtx,
		containerPool: GetThreadContainerPool(),
		masterPid:     actorcomm.GetMapMasterPid(workerAddr),
	}, nil
}

// SetStatusHandler binds the execution generation before the container is queued.
func (c *ThreadContainer) SetStatusHandler(handler *batch.ContainerStatusReqHandler) {
	c.statusHandler = handler
}

func (c *ThreadContainer) Run() {
	c.Start()
}

func (c *ThreadContainer) Start() {
	uniqueID := utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId())
	returned := false
	defer func() {
		defer c.finish(uniqueID)
		if returned {
			return
		}
		// Goexit runs defers but cannot be recovered. Report a terminal task
		// status as well as letting the pool replace the exited consumer.
		message := "task goroutine exited before returning"
		if r := recover(); r != nil {
			message = fmt.Sprintf("task initialization panic: %v", r)
		}
		c.reportTaskStatus(processor.NewProcessResult(processor.WithFailed(), processor.WithResult(message)), c.actorCtx.ActorSystem().Address())
	}()
	if !c.killed.Load() {
		c.start()
	}
	returned = true
}

// Serialize terminal cancellation with completion/removal, so a delayed user
// Kill callback cannot report against a same-ID replacement in the same cycle.
func (c *ThreadContainer) finish(uniqueID string) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.finished = true
	c.containerPool.Remove(uniqueID, c)
	c.containerPool.RemoveContext()
}

func (c *ThreadContainer) start() {
	taskMasterPool := masterpool.GetTaskMasterPool()
	uniqueId := utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId())
	c.containerPool.SetContext(c.jobCtx)

	// 统一挂任务级 trace：Start 会把携带 span 的 ctx 写回 jobCtx，
	// 框架日志、业务日志与下游调用（DB/HTTP 等）自动共用同一 traceId。
	tr := tracer.GetTracerOrDefault()
	if c.killed.Load() {
		return
	}
	c.jobCtx = tr.Start(c.jobCtx)

	startTime := time.Now().UnixMilli()
	logger.DebugfCtx(c.jobCtx, "start run container, uniqueId=%v, cost=%vms, jobContext=%+v", uniqueId, startTime-c.jobCtx.ScheduleTime().UnixMilli(), c.jobCtx)

	defer func() {
		if e := recover(); e != nil {
			logger.ErrorfCtx(c.jobCtx, "Start run container panic, error=%v, stack=%s", e, debug.Stack())
			errMsg := fmt.Sprintf("Process task panic, error=%v, stack=%s", e, debug.Stack())
			result := processor.NewProcessResult(processor.WithFailed(), processor.WithResult(errMsg))
			workerAddr := c.actorCtx.ActorSystem().Address()
			c.reportTaskStatus(result, workerAddr)
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
		logger.ErrorfCtx(c.jobCtx, "Process task=%s failed, because it's unregistered. ", jobName)
		return
	}

	defer func() {
		result = tr.End(c.jobCtx, result)
	}()

	if c.killed.Load() {
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
		logger.ErrorfCtx(c.jobCtx, "Process task=%s failed, uniqueId=%v, serialNum=%v, err=%s ", c.jobCtx.TaskName(), uniqueId, c.jobCtx.SerialNum(), err.Error())
		return
	}

	endTime := time.Now().UnixMilli()
	logger.DebugfCtx(c.jobCtx, "container run finished, uniqueId=%v, cost=%dms", uniqueId, endTime-startTime)

	if result == nil {
		result = processor.NewProcessResult(processor.WithFailed(), processor.WithResult("result can't be null"))
	}

	// If the execution of the map model subtask (non-root task) fails, would be retried
	if c.jobCtx.MaxAttempt() > 0 && c.jobCtx.TaskId() > 0 && result.Status() == processor.InstanceStatusFailed {
		if taskAttempt := c.jobCtx.TaskAttempt(); taskAttempt < c.jobCtx.MaxAttempt() {
			taskAttempt++
			time.Sleep(time.Duration(c.jobCtx.TaskAttemptInterval()) * time.Second)
			c.jobCtx.SetTaskAttempt(taskAttempt)
			c.start()

			// No need to return the current result status when retrying
			return
		}
	}

	c.reportTaskStatus(result, workerAddr)
}

func (c *ThreadContainer) cancelExecution() bool {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if c.finished || c.killed.Load() {
		return false
	}
	c.killed.Store(true)
	c.sendTaskStatus(processor.NewProcessResult(processor.WithFailed(), processor.WithResult("killed")), c.actorCtx.ActorSystem().Address())
	uniqueID := utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId())
	if !c.containerPool.Cancel(uniqueID, c) {
		c.finished = true
		c.containerPool.Remove(uniqueID, c)
	}
	return true
}

func (c *ThreadContainer) Kill() {
	if !c.cancelExecution() {
		return
	}

	logger.InfofCtx(c.jobCtx, "kill container, jobInstanceId=%v, content=%s", c.jobCtx.JobInstanceId(), c.jobCtx.Content())
	jobName := gjson.Get(c.jobCtx.Content(), "jobName").String()
	if jobName != "" {
		taskMasterPool := masterpool.GetTaskMasterPool()
		task, ok := taskMasterPool.Tasks().Find(jobName)
		if !ok {
			logger.WarnfCtx(c.jobCtx, "Kill task=%s failed, because it's not found. ", jobName)
		} else {
			kt, ok := task.(processor.KillProcessor)
			if ok {
				kt.Kill(c.jobCtx)
			}
		}
	}
}

func (c *ThreadContainer) reportTaskStatus(result *processor.ProcessResult, workerAddr string) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if c.killed.Load() {
		return
	}
	c.sendTaskStatus(result, workerAddr)
}

func (c *ThreadContainer) sendTaskStatus(result *processor.ProcessResult, workerAddr string) {
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

	if c.statusHandler != nil {
		// A stopped/replaced handler belongs to an old generation; do not fall back
		// to the new handler or send an unscoped status directly to its master.
		batch.GetContainerStatusReqHandlerPool().SubmitReqTo(c.jobCtx.JobInstanceId(), c.statusHandler, req)
		return
	}
	submitResult := batch.GetContainerStatusReqHandlerPool().SubmitReq(c.jobCtx.JobInstanceId(), req)
	logger.DebugfCtx(c.jobCtx, "reportTaskStatus instanceId=%v submitResult=%v, processResult=%v", utils.GetUniqueId(c.jobCtx.JobId(), c.jobCtx.JobInstanceId(), c.jobCtx.TaskId()), submitResult, result)
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
