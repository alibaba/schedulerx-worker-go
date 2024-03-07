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
	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

var _ UpdateInstanceStatusHandler = &commonUpdateInstanceStatusHandler{}

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

func (rcvr *commonUpdateInstanceStatusHandler) Handle(serialNum int64, instanceStatus processor.InstanceStatus, result string) error {
	jobInstanceId := rcvr.jobInstanceInfo.GetJobInstanceId()
	uniqueId := utils.GetUniqueIdWithoutTaskId(rcvr.jobInstanceInfo.GetJobId(), jobInstanceId)

	if rcvr.taskMaster.GetInstanceStatus() != instanceStatus {
		rcvr.taskMaster.SetInstanceStatus(instanceStatus)
		if instanceStatus.IsFinished() {
			postResult := rcvr.taskMaster.PostFinish(jobInstanceId)
			if postResult != nil {
				if instanceStatus == processor.InstanceStatusSucceed && postResult.Status() == processor.InstanceStatusFailed {
					instanceStatus = processor.InstanceStatusFailed
				}
				if postResult.Result() != "" {
					result = postResult.Result()
				}
			}

			// report job instance status with at-least-once-delivery
			req := &schedulerx.WorkerReportJobInstanceStatusRequest{
				JobId:         proto.Int64(rcvr.jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(jobInstanceId),
				Status:        proto.Int32(int32(instanceStatus)),
				DeliveryId:    proto.Int64(utils.GetDeliveryId()),
				GroupId:       proto.String(rcvr.jobInstanceInfo.GetGroupId()),
			}
			if result != "" {
				req.Result = proto.String(result)
			}
			progress, err := rcvr.taskMaster.GetJobInstanceProgress()
			if err == nil {
				req.Progress = proto.String(progress)
			} else {
				logger.Warnf("report job instance status with at-least-once-delivery failed, due to GetJobInstanceProgress is empty")
			}

			actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
				Msg: req,
			}
			logger.Infof("report jobInstance=%d, status=%d to AtLeastDeliveryRoutingActor", jobInstanceId, instanceStatus)

			// destroy containers and taskMaster
			if !config.GetWorkerConfig().IsShareContainerPool() {
				rcvr.taskMaster.DestroyContainerPool()
			}
			if taskMaster := rcvr.masterPool.Get(jobInstanceId); taskMaster != nil {
				taskMaster.Stop()
				rcvr.masterPool.Remove(jobInstanceId)
			}

			logger.Infof("uniqueId: %d is finished, remove from MasterPool.", uniqueId)
		}
	}
	progress, err := rcvr.taskMaster.GetJobInstanceProgress()
	if err != nil {
		logger.Warnf("report job instance status with at-least-once-delivery failed, due to GetJobInstanceProgress is empty")
	}

	_, ok := rcvr.taskMaster.(*StandaloneTaskMaster)
	if ok && !instanceStatus.IsFinished() && progress != "" {
		// report job instance status with at-least-once-delivery
		reportStatusReq := &schedulerx.WorkerReportJobInstanceStatusRequest{
			JobId:         proto.Int64(rcvr.jobInstanceInfo.GetJobId()),
			JobInstanceId: proto.Int64(jobInstanceId),
			Status:        proto.Int32(int32(instanceStatus)),
			Progress:      proto.String(progress),
			DeliveryId:    proto.Int64(utils.GetDeliveryId()),
			GroupId:       proto.String(rcvr.jobInstanceInfo.GetGroupId()),
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
