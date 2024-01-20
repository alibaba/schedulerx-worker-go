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
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ taskmaster.MapTaskMaster = &ShardingTaskMaster{}

type ShardingTaskMaster struct {
	*GridTaskMaster
	actorCtx              actor.Context
	parameters            []string
	shardingTaskStatusMap *sync.Map // Map<Long, ShardingTaskStatus>
}

func NewShardingTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) *ShardingTaskMaster {
	return &ShardingTaskMaster{
		GridTaskMaster:        NewGridTaskMaster(jobInstanceInfo, actorCtx),
		actorCtx:              actorCtx,
		shardingTaskStatusMap: new(sync.Map),
	}
}

func (m *ShardingTaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	if err := m.parseShardingParameters(jobInstanceInfo); err != nil {
		m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, err.Error())
		return err
	}
	shardingNum := len(m.parameters)
	startContainerRequests := make([]*schedulerx.MasterStartContainerRequest, 0, shardingNum)
	for _, param := range m.parameters {
		tokens := strings.Split(param, "=")
		if len(tokens) != 2 {
			errMsg := fmt.Sprintf("invalid sharding parameters, should be like 0=a,1=b,2=c")
			m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
			return fmt.Errorf(errMsg)
		}
		shardingId, err := strconv.Atoi(tokens[0])
		if err != nil {
			errMsg := fmt.Sprintf("invalid sharding parameters, shardingId is not digit, shardingId=%s", tokens[0])
			m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
			return fmt.Errorf(errMsg)
		}
		taskName := tokens[0] // taskName == shardingId
		shardingParameter := tokens[1]

		if _, ok := m.taskProgressMap.Load(taskName); ok {
			errMsg := fmt.Sprintf("shardingId=%s is duplicated", taskName)
			m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
			return fmt.Errorf(errMsg)
		}

		task := common.NewShardingTask(int64(shardingId), shardingParameter)
		taskObj, err := json.Marshal(task)
		if err != nil {
			errMsg := fmt.Sprintf("json marshal task failed, err=%s, task=%+v", err.Error(), task)
			m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
			return fmt.Errorf(errMsg)
		}
		req, err := m.convert2StartContainerRequest(jobInstanceInfo, int64(shardingId), taskName, taskObj, false)
		if err != nil {
			errMsg := fmt.Sprintf("convert2StartContainerRequest failed, err=%s, jobInstanceInfo=%+v", err.Error(), jobInstanceInfo)
			m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, errMsg)
			return fmt.Errorf(errMsg)
		}
		req.ShardingNum = proto.Int32(int32(shardingNum))

		startContainerRequests = append(startContainerRequests, req)
	}

	m.startBatchHandler()
	m.BatchDispatchTasks(startContainerRequests, "")
	m.init()

	return nil
}

func (m *ShardingTaskMaster) parseShardingParameters(jobInstanceInfo *common.JobInstanceInfo) error {
	// For sharding tasks in the workflow, the sharding parameters are those configured in the original task.
	shardingParameters := jobInstanceInfo.GetParameters()
	if jobInstanceInfo.GetWfInstanceId() == 0 && jobInstanceInfo.GetInstanceParameters() != "" {
		shardingParameters = jobInstanceInfo.GetInstanceParameters()
	}

	if shardingParameters == "" {
		return fmt.Errorf("sharding parameters is empty")
	}
	reg, err := regexp.Compile(",|\n|\r")
	if err != nil {
		return err
	}
	m.parameters = reg.Split(shardingParameters, -1)
	return nil
}

func (m *ShardingTaskMaster) BatchUpdateTaskStatues(requests []*schedulerx.ContainerReportTaskStatusRequest) {
	m.GridTaskMaster.BatchUpdateTaskStatues(requests)
	for _, req := range requests {
		var (
			taskId     = req.GetTaskId()
			taskStatus = req.GetStatus()
			workerAddr = req.GetWorkerAddr()
		)
		if existedShardingTaskStatus, ok := m.shardingTaskStatusMap.Load(taskId); ok {
			existedShardingTaskStatus.(*taskstatus.ShardingTaskStatus).SetStatus(taskStatus)
		} else {
			m.shardingTaskStatusMap.Store(taskId, taskstatus.NewShardingTaskStatus(taskId, workerAddr, taskStatus))
		}
	}
}

func (m *ShardingTaskMaster) BatchHandlePulledProgress(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest,
	remoteWorker string) (map[string][]*schedulerx.MasterStartContainerRequest, map[string][]*schedulerx.MasterStartContainerRequest) {
	var (
		worker2ReqsWithNormal   = make(map[string][]*schedulerx.MasterStartContainerRequest)
		worker2ReqsWithFailover = make(map[string][]*schedulerx.MasterStartContainerRequest)
	)
	for _, request := range masterStartContainerRequests {
		workerIdAddr := remoteWorker
		if workerIdAddr == "" {
			workerIdAddr = m.selectWorker()
		}
		if workerIdAddr == "" {
			m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, "all worker is down!")
			break
		}

		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		if request.GetFailover() {
			if _, ok := worker2ReqsWithFailover[workerIdAddr]; !ok {
				worker2ReqsWithFailover[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithFailover[workerIdAddr] = append(worker2ReqsWithFailover[workerIdAddr], request)
			}
		} else {
			if _, ok := worker2ReqsWithNormal[workerIdAddr]; !ok {
				worker2ReqsWithNormal[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithNormal[workerIdAddr] = append(worker2ReqsWithFailover[workerIdAddr], request)
			}

			if counter, ok := m.taskProgressMap.Load(request.GetTaskName()); ok {
				counter.(*common.TaskProgressCounter).IncrementOnePulled()
			}
		}

		if _, ok := m.workerProgressMap.Load(workerAddr); workerAddr != "" && !ok {
			m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		}
		if val, ok := m.workerProgressMap.Load(workerAddr); ok {
			val.(*common.WorkerProgressCounter).IncrementTotal()
			val.(*common.WorkerProgressCounter).IncrementPulled()
		}

		m.shardingTaskStatusMap.Store(request.GetTaskId(), taskstatus.NewShardingTaskStatus(request.GetTaskId(), workerAddr, int32(taskstatus.TaskStatusInit)))
	}
	return worker2ReqsWithNormal, worker2ReqsWithFailover
}

func (m *ShardingTaskMaster) GetJobInstanceProgress() (string, error) {
	shardingTaskStatusList := make([]*taskstatus.ShardingTaskStatus, 0, utils.SyncMapLen(m.shardingTaskStatusMap))
	m.shardingTaskStatusMap.Range(func(shardingId, shardingTaskStatus any) bool {
		shardingTaskStatusList = append(shardingTaskStatusList, shardingTaskStatus.(*taskstatus.ShardingTaskStatus))
		return true
	})
	detail := taskstatus.NewShardingTaskProgress(shardingTaskStatusList)
	data, err := json.Marshal(detail)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (m *ShardingTaskMaster) CheckProcessor() {
	// nothing to do
}

func (m *ShardingTaskMaster) PostFinish(jobInstanceId int64) *processor.ProcessResult {
	if err := m.taskPersistence.ClearTasks(jobInstanceId); err != nil {
		return nil
	}
	return processor.NewProcessResult(processor.WithSucceed())
}

func (m *ShardingTaskMaster) Clear(taskMaster taskmaster.TaskMaster) {
	m.GridTaskMaster.Clear(taskMaster)
	if m.shardingTaskStatusMap != nil {
		m.shardingTaskStatusMap = nil
	}
}
