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
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

func Handshake(ctx context.Context, conn net.Conn) error {
	if err := sendHandshake(ctx, conn); err != nil {
		return fmt.Errorf("Write handshake to remote failed, err=%s. ", err.Error())
	}

	waitRespTimeout := 5 * time.Second
	waitTimeout := time.After(waitRespTimeout)
	for {
		select {
		case <-waitTimeout:
			return fmt.Errorf("Wait handshake response timeout, timeout=%s ", waitRespTimeout.String())
		default:
			var dataLen uint32
			hdrBuf := make([]byte, constants.TransportHeaderSize)
			n, err := io.ReadFull(conn, hdrBuf)
			if err == io.EOF {
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

			msg, err := trans.ReadAkkaMsg(dataBuf)
			if err != nil {
				return fmt.Errorf("handshake read akka msg err=%+v ", err)
			}
			if controlMsg := msg.Instruction; controlMsg != nil && controlMsg.CommandType != nil {
				if int32(*controlMsg.CommandType) == int32(akka.CommandType_ASSOCIATE) {
					logger.Infof("Receive handshake msg, msg=%+v ", controlMsg)
					return nil
				}
			} else {
				return fmt.Errorf("Receive unknown msg type when wait handshake response, msg=%+v ", msg)
			}
		}
	}
}

func sendHandshake(ctx context.Context, conn net.Conn) error {
	host, port, err := utils.ParseIPAddr(conn.LocalAddr().String())
	if err != nil {
		return err
	}
	akkaMsg := &akka.AkkaProtocolMessage{
		Instruction: &akka.AkkaControlMessage{
			CommandType: akka.CommandType_ASSOCIATE.Enum(),
			HandshakeInfo: &akka.AkkaHandshakeInfo{
				Origin: &akka.AddressData{
					System:   proto.String(utils.GetWorkerId()),
					Hostname: proto.String(host),
					Port:     proto.Uint32(uint32(port)),
					Protocol: proto.String("tcp"),
				},
				Uid: proto.Uint64(utils.GetHandshakeUid()),
			},
		},
	}
	return trans.WriteAkkaMsg(akkaMsg, conn)
}
