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
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/codec"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ actor.Process = &jobInstanceProcessor{}

type jobInstanceProcessor struct {
	connpool pool.ConnPool
}

func newJobInstanceProcessor(connpool pool.ConnPool) actor.Process {
	return &jobInstanceProcessor{
		connpool: connpool,
	}
}

func (p *jobInstanceProcessor) SendUserMessage(pid *actor.PID, message interface{}) {
	if actorcomm.IsSchedulerxServer(pid) {
		var (
			akkaMsg *akka.AkkaProtocolMessage
			err     error
		)
		wrappedMsg, ok := message.(*actorcomm.SchedulerWrappedMsg)
		if !ok {
			logger.Errorf("Get unknown message, msg=%+v", wrappedMsg)
			return
		}
		conn, err := p.connpool.Get(wrappedMsg.Ctx)
		if err != nil {
			logger.Errorf("Get conn from pool failed, err=%s", err.Error())
			return
		}
		switch msg := wrappedMsg.Msg.(type) {
		case *schedulerx.ServerSubmitJobInstanceResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$ServerSubmitJobInstanceResponse")
		case *schedulerx.ServerKillJobInstanceResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$ServerKillJobInstanceResponse")
		case *schedulerx.MasterKillContainerResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$MasterKillContainerResponse")
		case *schedulerx.ServerKillTaskResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$ServerKillTaskResponse")
		case *schedulerx.WorkerReportJobInstanceStatusRequest:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				fmt.Sprintf("akka.tcp://server@%s/", conn.RemoteAddr().String()),
				fmt.Sprintf("akka.tcp://%s@%s/user/at_least_once_delivery_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Worker$WorkerReportJobInstanceStatusRequest",
				codec.WithMessageContainerSerializer(),
				codec.WithSelectionEnvelopePattern([]*akka.Selection{
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("user"),
					},
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("instance_status_router"),
					},
				}))
		case *schedulerx.WorkerBatchReportTaskStatuesRequest:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				fmt.Sprintf("akka.tcp://server@%s/", conn.RemoteAddr().String()),
				fmt.Sprintf("akka.tcp://%s@%s/user/at_least_once_delivery_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Worker$WorkerBatchReportTaskStatuesRequest",
				codec.WithMessageContainerSerializer(),
				codec.WithSelectionEnvelopePattern([]*akka.Selection{
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("user"),
					},
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("instance_status_router"),
					},
				}))
		case *schedulerx.WorkerReportJobInstanceProgressRequest:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				fmt.Sprintf("akka.tcp://server@%s/", conn.RemoteAddr().String()),
				"",
				"com.alibaba.schedulerx.protocol.Worker$WorkerReportJobInstanceProgressRequest",
				codec.WithMessageContainerSerializer(),
				codec.WithSelectionEnvelopePattern([]*akka.Selection{
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("user"),
					},
					{
						Type:    akka.PatternType_CHILD_NAME.Enum(),
						Matcher: proto.String("map_master_router"),
					},
				}))
		case *schedulerx.ServerRetryTasksResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$ServerRetryTasksResponse")
		case *schedulerx.ServerCheckTaskMasterResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$ServerCheckTaskMasterResponse")
		case *schedulerx.MasterNotifyWorkerPullResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$MasterNotifyWorkerPullResponse")
		case *schedulerx.WorkerReportTaskListStatusResponse:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				wrappedMsg.SenderPath,
				fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
				"com.alibaba.schedulerx.protocol.Server$WorkerReportTaskListStatusResponse")
		default:
			logger.Errorf("Unknown akka message type=%+v", msg)
			return
		}
		if err != nil {
			logger.Errorf("Encode akka message failed, err=%s", err.Error())
			return
		}
		if err := trans.WriteAkkaMsg(akkaMsg, conn); err != nil {
			logger.Errorf("Write akka message failed, err=%s", err.Error())
			return
		}
	}
}

func (p *jobInstanceProcessor) SendSystemMessage(pid *actor.PID, message interface{}) {
	switch msg := message.(type) {
	default:
		logger.Errorf("Unknown akka message type=%+v", msg)
		return
	}
}

func (p *jobInstanceProcessor) Stop(pid *actor.PID) {
	// do nothing
}
