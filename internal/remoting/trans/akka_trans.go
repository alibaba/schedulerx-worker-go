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

package trans

import (
	"bytes"
	"encoding/binary"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
)

func WriteAkkaMsg(msg *akka.AkkaProtocolMessage, conn net.Conn) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, data); err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write(buf.Bytes())
	return err
}

func ReadAkkaMsg(data []byte) (*akka.AkkaProtocolMessage, error) {
	buf := bytes.NewBuffer(data)
	rawData := make([]byte, len(data))
	if err := binary.Read(buf, binary.BigEndian, &rawData); err != nil {
		return nil, err
	}
	msg := &akka.AkkaProtocolMessage{}
	if err := proto.Unmarshal(rawData, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
