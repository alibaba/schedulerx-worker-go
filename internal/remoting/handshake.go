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

// Handshake bounds the entire exchange, including blocking socket reads and writes.
func Handshake(ctx context.Context, conn net.Conn) (result error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set handshake deadline: %w", err)
	}

	// Context cancellation has to interrupt the in-flight syscall. Join this
	// watcher before clearing deadlines so it cannot poison a reused connection.
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()
	defer func() {
		close(stop)
		<-stopped
		if err := ctx.Err(); err != nil {
			result = err
		} else if !time.Now().Before(deadline) {
			result = context.DeadlineExceeded
		}
		if err := conn.SetDeadline(time.Time{}); err != nil && result == nil {
			result = fmt.Errorf("clear handshake deadline: %w", err)
		}
	}()

	// The common encoder must not overwrite the installed exchange deadline
	// or a cancellation deadline which the watcher has already installed.
	if err := sendHandshake(ctx, handshakeDeadlineConn{Conn: conn}); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var hdr [constants.TransportHeaderSize]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return fmt.Errorf("read handshake header: %w", err)
		}
		data := make([]byte, binary.BigEndian.Uint32(hdr[:]))
		if _, err := io.ReadFull(conn, data); err != nil {
			return fmt.Errorf("read handshake payload: %w", err)
		}
		msg, err := trans.ReadAkkaMsg(data)
		if err != nil {
			return fmt.Errorf("decode handshake: %w", err)
		}
		if msg.Instruction == nil || msg.Instruction.CommandType == nil {
			return fmt.Errorf("unexpected handshake response: %v", msg)
		}
		if *msg.Instruction.CommandType == akka.CommandType_ASSOCIATE {
			logger.Infof("Receive handshake msg, msg=%+v ", msg.Instruction)
			return nil
		}
		// Preserve handling of other control frames while waiting for ASSOCIATE.
	}

}

type handshakeDeadlineConn struct{ net.Conn }

// The exchange owns both deadlines until its cancellation watcher has joined.
func (c handshakeDeadlineConn) SetWriteDeadline(time.Time) error { return nil }

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
