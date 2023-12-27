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

package taskmaster

import (
	"context"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

type TaskMaster interface {
	Stop()
	IsInited() bool
	IsKilled() bool
	DestroyContainerPool()
	Clear(taskMaster TaskMaster)
	GetSerialNum() int64
	AcquireSerialNum() int64
	ExistInvalidWorker() bool
	GetCurrentSelection() string
	KillInstance(reason string) error
	GetInstanceStatus() processor.InstanceStatus
	GetJobInstanceProgress() (string, error)
	GetAliveCheckWorkerSet() *utils.ConcurrentSet
	GetJobInstanceInfo() *common.JobInstanceInfo
	RestJobInstanceWorkerList(freeWorkers *utils.Set)
	SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error
	BatchUpdateTaskStatus(taskMaster TaskMaster, req *schedulerx.ContainerBatchReportTaskStatuesRequest) error
	UpdateTaskStatus(req *schedulerx.ContainerReportTaskStatusRequest) error
	SetInstanceStatus(instanceStatus processor.InstanceStatus)
	UpdateNewInstanceStatus(serialNum int64, newStatus processor.InstanceStatus, result string) error
	PostFinish(jobInstanceId int64) *processor.ProcessResult
}

type MapTaskMaster interface {
	TaskMaster
	Map(jobCtx *jobcontext.JobContext, taskList [][]byte, taskName string) (bool, error)
	KillTask(uniqueId string, workerId string, workerAddr string)
	BatchUpdateTaskStatues(requests []*schedulerx.ContainerReportTaskStatusRequest)
	SyncPullTasks(pageSize int32, workerIdAddr string) []*schedulerx.MasterStartContainerRequest
	BatchPullTasks(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest, workerIdAddr string)
	BatchDispatchTasks(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest, remoteWorker string)
	BatchHandlePulledProgress(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest,
		remoteWorker string) (map[string][]*schedulerx.MasterStartContainerRequest, map[string][]*schedulerx.MasterStartContainerRequest)
}

type ParallelTaskMaster interface {
	MapTaskMaster
	RetryTasks(taskEntities []*schedulerx.RetryTaskEntity)
}
