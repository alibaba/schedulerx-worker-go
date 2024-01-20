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

package remoting

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/codec"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

func OnMsgReceived(ctx context.Context) {
	connpool := pool.GetConnPool()

	var dataLen uint32
	hdrBuf := make([]byte, constants.TransportHeaderSize)
	for {
		conn, err := connpool.Get(ctx)
		if err != nil {
			logger.Errorf("OnMsgReceived get conn from pool failed, err=%s", err.Error())
			time.Sleep(100 * time.Millisecond) // maybe network is broken, just wait a moment
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, err := io.ReadFull(conn, hdrBuf)
		if err == io.EOF {
			time.Sleep(100 * time.Millisecond) // maybe network is broken, just wait a moment
			continue
		}
		if errors.Is(err, os.ErrDeadlineExceeded) { // timeout, read no data
			continue
		}
		if err != nil { // broke pipe
			// EADDRNOTAVAIL
			connpool.ReconnectTrigger() <- struct{}{}
			logger.Errorf("OnMsgReceived broke pipe, err=%s", err.Error())
			continue
		}
		if n < constants.TransportHeaderSize {
			logger.Errorf("Read header from connection failed, read bytes=%d but expect bytes=%d", n, constants.TransportHeaderSize)
			continue
		}

		dataLen = binary.BigEndian.Uint32(hdrBuf)
		dataBuf := make([]byte, dataLen)
		n, err = io.ReadFull(conn, dataBuf)
		if err == io.EOF {
			continue
		}
		if n < int(dataLen) {
			logger.Errorf("Read payload from connection failed, read bytes=%d but expect bytes=%d", n, dataLen)
			continue
		}

		akkaMsg, err := trans.ReadAkkaMsg(dataBuf)
		if err != nil {
			logger.Errorf("Read raw buffer data failed, err=%s", err.Error())
			continue
		}
		msg, senderPath, err := codec.DecodeAkkaMessage(akkaMsg)
		if err != nil {
			logger.Errorf("Read akka message failed, err=%s", err.Error())
			continue
		}

		switch msg := msg.(type) {
		case *akka.AkkaControlMessage:
			if int32(msg.GetCommandType()) == int32(akka.CommandType_HEARTBEAT) {
				logger.Debugf("Receive heartbeat from server, heartbeat=%+v", msg)
				continue
			} else {
				logger.Warnf("Receive unexpect control message from server, message=%+v", msg)
				continue
			}
		case *schedulerx.ServerSubmitJobInstanceRequest:
			actorcomm.SxMsgReceiver() <- actorcomm.WrapSchedulerxMsg(ctx, msg, senderPath)
		case *schedulerx.ServerKillJobInstanceRequest:
			actorcomm.SxMsgReceiver() <- actorcomm.WrapSchedulerxMsg(ctx, msg, senderPath)
		case *schedulerx.ServerKillTaskRequest:
			actorcomm.SxMsgReceiver() <- actorcomm.WrapSchedulerxMsg(ctx, msg, senderPath)
		case *schedulerx.ServerRetryTasksRequest:
			actorcomm.SxMsgReceiver() <- actorcomm.WrapSchedulerxMsg(ctx, msg, senderPath)
		case *schedulerx.WorkerReportJobInstanceStatusResponse:
			logger.Debugf("Receive WorkerReportJobInstanceStatusResponse from server, resp=%+v", msg)
			continue
		case *schedulerx.WorkerHeartBeatResponse:
			logger.Debugf("Receive heartbeat from server, heartbeat=%+v", msg)
			continue
		default:
			logger.Errorf("Unknown msg type, msg=%+v", msg)
			continue
		}
	}
}
