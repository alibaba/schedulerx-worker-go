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
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"google.golang.org/protobuf/proto"
	"strings"
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

func (h *baseUpdateInstanceStatusHandler) Handle(serialNum int64, instanceStatus processor.InstanceStatus, result string) error {
	uniqueId := utils.GetUniqueIdWithoutTaskId(h.jobInstanceInfo.GetJobId(), h.jobInstanceInfo.GetJobInstanceId())
	if h.taskMaster.GetInstanceStatus() != instanceStatus {
		h.taskMaster.SetInstanceStatus(instanceStatus)
		if instanceStatus.IsFinished() {
			postResult := h.taskMaster.PostFinish(h.jobInstanceInfo.GetJobInstanceId())
			if postResult != nil {
				if instanceStatus == processor.InstanceStatusSucceed && postResult.GetStatus() == processor.InstanceStatusFailed {
					instanceStatus = processor.InstanceStatusFailed
				}
				if postResult.GetResult() != "" && !strings.Contains(result, "Worker master shutdown") {
					result = postResult.GetResult()
				}
			}

			if !strings.Contains(result, "killed from server") {
				err := h.reportJobInstanceStatus(instanceStatus, result)
				if err != nil {
					return err
				}
			}

			h.taskMaster.DestroyContainerPool()
			h.masterPool.Get(h.jobInstanceInfo.GetJobInstanceId()).Stop()
			h.masterPool.Remove(h.jobInstanceInfo.GetJobInstanceId())
			logger.Infof("uniqueId: %s is finished, remove from MasterPool.", uniqueId)
		}
	}

	progress, _ := h.taskMaster.GetJobInstanceProgress()
	_, ok := h.taskMaster.(*StandaloneTaskMaster)
	if ok && !instanceStatus.IsFinished() && progress != "" {
		err := h.reportJobInstanceStatus(instanceStatus, result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *baseUpdateInstanceStatusHandler) reportJobInstanceStatus(instanceStatus processor.InstanceStatus, result string) error {
	//report job instance status with at-least-once-delivery
	request := &schedulerx.WorkerReportJobInstanceStatusRequest{
		JobId:         proto.Int64(h.jobInstanceInfo.GetJobId()),
		JobInstanceId: proto.Int64(h.jobInstanceInfo.GetJobInstanceId()),
		Status:        proto.Int32(int32(instanceStatus)),
		GroupId:       proto.String(h.jobInstanceInfo.GetGroupId()),
	}
	if result != "" {
		request.Result = proto.String(result)
	}

	progress, _ := h.taskMaster.GetJobInstanceProgress()
	if progress != "" {
		request.Progress = proto.String(progress)
	}

	err := trans.SendReportTaskStatusReq(context.Background(), request)
	if err != nil {
		return err
	}
	logger.Infof("report jobInstance=%d, status=%d to AtLeastDeliveryRoutingActor",
		h.jobInstanceInfo.GetJobInstanceId(), instanceStatus)
	return nil
}
