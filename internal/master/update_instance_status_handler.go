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
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/processor"
)

var _ UpdateInstanceStatusHandler = &baseUpdateInstanceStatusHandler{}

type UpdateInstanceStatusHandler interface {
	Handle(serialNum int64, newStatus processor.InstanceStatus, result string) error
}

type baseUpdateInstanceStatusHandler struct {
	jobInstanceInfo *common.JobInstanceInfo
	taskMaster      taskmaster.TaskMaster
	masterPool      *masterpool.TaskMasterPool
}

func NewBaseUpdateInstanceStatusHandler(jobInstanceInfo *common.JobInstanceInfo, taskMaster taskmaster.TaskMaster) *baseUpdateInstanceStatusHandler {
	return &baseUpdateInstanceStatusHandler{
		jobInstanceInfo: jobInstanceInfo,
		taskMaster:      taskMaster,
		masterPool:      masterpool.GetTaskMasterPool(),
	}
}

func (h *baseUpdateInstanceStatusHandler) Handle(serialNum int64, newStatus processor.InstanceStatus, result string) error {
	return nil
}
