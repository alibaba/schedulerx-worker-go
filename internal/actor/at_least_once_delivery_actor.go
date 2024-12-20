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

package actor

import (
	"github.com/asynkron/protoactor-go/actor"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

type atLeastOnceDeliveryRoutingActor struct{}

func newAtLeastOnceDeliveryRoutingActor() *atLeastOnceDeliveryRoutingActor {
	return &atLeastOnceDeliveryRoutingActor{}
}

func (a *atLeastOnceDeliveryRoutingActor) Receive(actorCtx actor.Context) {
	msg := actorCtx.Message()
	if wrappedMsg, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
		msg = wrappedMsg.Msg
	}
	switch innerMsg := msg.(type) {
	case *schedulerx.WorkerReportJobInstanceStatusRequest:
		a.handleReportInstanceStatusEvent(innerMsg)
	case *schedulerx.WorkerBatchReportTaskStatuesRequest:
		a.handleBatchReportTaskStatues(innerMsg)
	case *schedulerx.ContainerBatchReportTaskStatuesRequest:
		a.handleContainerBatchStatus(actorCtx, innerMsg)
	case *schedulerx.MasterDestroyContainerPoolRequest:
		a.handleDestroyContainerPool(actorCtx, innerMsg)
	case *schedulerx.WorkerReportJobInstanceStatusResponse:
		logger.Infof("Receive WorkerReportJobInstanceStatusResponse, resp=%+v", innerMsg)
	case *schedulerx.WorkerBatchReportTaskStatuesResponse:
		logger.Infof("Receive WorkerBatchReportTaskStatuesResponse, resp=%+v", innerMsg)
	case *schedulerx.ContainerBatchReportTaskStatuesResponse:
		logger.Infof("Receive ContainerBatchReportTaskStatuesResponse, resp=%+v", innerMsg)
	case *schedulerx.MasterDestroyContainerPoolResponse:
		logger.Infof("Receive MasterDestroyContainerPoolResponse, resp=%+v", innerMsg)
	default:
		logger.Errorf("Receive unknown message in atLeastOnceDeliveryRoutingActor, msg=%+v", msg)
	}
}

func (a *atLeastOnceDeliveryRoutingActor) handleReportInstanceStatusEvent(req *schedulerx.WorkerReportJobInstanceStatusRequest) {
	actorcomm.SxMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}
}

func (a *atLeastOnceDeliveryRoutingActor) handleBatchReportTaskStatues(req *schedulerx.WorkerBatchReportTaskStatuesRequest) {
	actorcomm.SxMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
		Msg: req,
	}
}

func (a *atLeastOnceDeliveryRoutingActor) handleContainerBatchStatus(actorCtx actor.Context, msg *schedulerx.ContainerBatchReportTaskStatuesRequest) {
	workerAddr := actorcomm.GetRealWorkerAddr(msg.GetTaskMasterAkkaPath())
	actorCtx.Request(actorcomm.GetMapMasterPid(workerAddr), msg)
}

func (a *atLeastOnceDeliveryRoutingActor) handleDestroyContainerPool(actorCtx actor.Context, msg *schedulerx.MasterDestroyContainerPoolRequest) {
	workerAddr := actorcomm.GetRealWorkerAddr(msg.GetWorkerIdAddr())
	actorCtx.Request(actorcomm.GetContainerRouterPid(workerAddr), msg)
}
