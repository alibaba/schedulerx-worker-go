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

package persistence

import (
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

type TaskPersistence interface {
	// InitTable init task table
	InitTable()

	// UpdateTaskStatus update tasks Status and Worker info
	UpdateTaskStatus(jobInstanceId int64, taskIds []int64, status taskstatus.TaskStatus, workerId, workerAddr string) (int64, error)

	// UpdateTaskStatues update task statues accord to list of TaskStatusInfo
	UpdateTaskStatues(taskStatusInfos []*schedulerx.ContainerReportTaskStatusRequest) error

	// ClearTasks clear all tasks belong to specific job instance
	ClearTasks(jobInstanceId int64) error

	// CreateTask create task
	CreateTask(jobId, jobInstanceId, taskId int64, taskName string, taskBody []byte) error

	// CreateTasks batch create container infos
	CreateTasks(containers []*schedulerx.MasterStartContainerRequest, workerId, workerAddr string) error

	// BatchUpdateTaskStatus update tasks Status using condition
	BatchUpdateTaskStatus(jobInstanceId int64, status taskstatus.TaskStatus, workerId string, workerAddr string) int64

	// CheckInstanceStatus check job instance current Status
	CheckInstanceStatus(jobInstanceId int64) processor.InstanceStatus

	// Pull pull init tasks for failover retry
	Pull(jobInstanceId int64, pageSize int32) ([]*common.TaskInfo, error)
}
