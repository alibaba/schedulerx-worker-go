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
	"strings"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
)

var _ UpdateInstanceStatusHandler = (*commonUpdateInstanceStatusHandler)(nil)

type commonUpdateInstanceStatusHandler struct {
	*baseUpdateInstanceStatusHandler
	actorContext actor.Context
}

func NewCommonUpdateInstanceStatusHandler(actorContext actor.Context, taskMaster taskmaster.TaskMaster, jobInstanceInfo *common.JobInstanceInfo) (rcvr UpdateInstanceStatusHandler) {
	return &commonUpdateInstanceStatusHandler{
		actorContext:                    actorContext,
		baseUpdateInstanceStatusHandler: NewBaseUpdateInstanceStatusHandler(jobInstanceInfo, taskMaster),
	}
}

func (h *commonUpdateInstanceStatusHandler) Handle(serialNum int64, instanceStatus processor.InstanceStatus, result string) error {
	jobInstanceId := h.jobInstanceInfo.GetJobInstanceId()

	if h.taskMaster.GetInstanceStatus() != instanceStatus {
		h.taskMaster.SetInstanceStatus(instanceStatus)
		if instanceStatus.IsFinished() {
			postResult := h.taskMaster.PostFinish(jobInstanceId)
			if postResult != nil {
				if instanceStatus == processor.InstanceStatusSucceed && postResult.Status() == processor.InstanceStatusFailed {
					instanceStatus = processor.InstanceStatusFailed
				}
				if postResult.Result() != "" && !strings.Contains(result, "Worker master shutdown") {
					result = postResult.Result()
				}
			}

			if result != "killed from server" {
				// 对服务端强制停止操作不做状态反馈
				// report job instance status with at-least-once-delivery
				req := &schedulerx.WorkerReportJobInstanceStatusRequest{
					JobId:         proto.Int64(h.jobInstanceInfo.GetJobId()),
					JobInstanceId: proto.Int64(jobInstanceId),
					Status:        proto.Int32(int32(instanceStatus)),
					DeliveryId:    proto.Int64(utils.GetDeliveryId()),
					GroupId:       proto.String(h.jobInstanceInfo.GetGroupId()),
				}
				if result != "" {
					req.Result = proto.String(result)
				}
				progress, err := h.taskMaster.GetJobInstanceProgress()
				if err == nil {
					req.Progress = proto.String(progress)
				} else {
					logger.Warnf("report job instance status with at-least-once-delivery failed, due to GetJobInstanceProgress is empty")
				}

				actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
					Msg: req,
				}
				logger.Infof("report jobInstance=%d, status=%d to AtLeastDeliveryRoutingActor", jobInstanceId, instanceStatus)
			}

			// destroy containers and taskMaster
			h.taskMaster.DestroyContainerPool()
			if taskMaster := h.masterPool.Get(jobInstanceId); taskMaster != nil {
				taskMaster.Stop()
				h.masterPool.Remove(jobInstanceId)
			}
			uniqueId := utils.GetUniqueIdWithoutTaskId(h.jobInstanceInfo.GetJobId(), jobInstanceId)
			logger.Infof("uniqueId: %s is finished, remove from MasterPool.", uniqueId)
		}
	}
	progress, err := h.taskMaster.GetJobInstanceProgress()
	if err != nil {
		logger.Warnf("report job instance status with at-least-once-delivery failed, due to GetJobInstanceProgress is empty")
	}

	_, ok := h.taskMaster.(*StandaloneTaskMaster)
	if ok && !instanceStatus.IsFinished() && progress != "" {
		// report job instance status with at-least-once-delivery
		reportStatusReq := &schedulerx.WorkerReportJobInstanceStatusRequest{
			JobId:         proto.Int64(h.jobInstanceInfo.GetJobId()),
			JobInstanceId: proto.Int64(jobInstanceId),
			Status:        proto.Int32(int32(instanceStatus)),
			Progress:      proto.String(progress),
			DeliveryId:    proto.Int64(utils.GetDeliveryId()),
			GroupId:       proto.String(h.jobInstanceInfo.GetGroupId()),
		}
		if result != "" {
			reportStatusReq.Result = proto.String(result)
		}
		actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: reportStatusReq,
		}
		logger.Infof("report jobInstance=%d, status=%s to AtLeastDeliveryRoutingActor", jobInstanceId, instanceStatus.Descriptor())
	}
	return nil
}
