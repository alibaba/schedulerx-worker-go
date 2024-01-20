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
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ actor.Process = &taskProcessor{}

type taskProcessor struct {
	connpool pool.ConnPool
}

func newTaskProcessor(connpool pool.ConnPool) actor.Process {
	return &taskProcessor{
		connpool: connpool,
	}
}

func (p *taskProcessor) SendUserMessage(pid *actor.PID, message interface{}) {
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
		case *schedulerx.WorkerBatchUpdateTaskStatusRequest:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				fmt.Sprintf("akka.tcp://server@%s/user/map_master_router", conn.RemoteAddr().String()),
				"",
				"com.alibaba.schedulerx.protocol.Worker$WorkerBatchUpdateTaskStatusRequest",
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
		case *schedulerx.WorkerQueryJobInstanceStatusRequest:
			akkaMsg, err = codec.EncodeAkkaMessage(
				msg,
				fmt.Sprintf("akka.tcp://server@%s/user/map_master_router", conn.RemoteAddr().String()),
				"",
				"com.alibaba.schedulerx.protocol.Worker$WorkerQueryJobInstanceStatusRequest",
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

func (p *taskProcessor) SendSystemMessage(pid *actor.PID, message interface{}) {
	switch msg := message.(type) {
	default:
		logger.Errorf("Unknown akka message type=%+v", msg)
		return
	}
}

func (p *taskProcessor) Stop(pid *actor.PID) {
	// do nothing
}
