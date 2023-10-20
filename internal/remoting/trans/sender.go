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
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/codec"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/connpool"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

func SendHeartbeatReq(ctx context.Context, req *schedulerx.WorkerHeartBeatRequest) error {
	conn, err := connpool.GetConnPool().Get(ctx)
	if err != nil {
		return err
	}
	akkaMsg, err := codec.EncodeAkkaMessage(
		req,
		fmt.Sprintf("akka.tcp://server@%s/", conn.RemoteAddr().String()),
		fmt.Sprintf("akka.tcp://%s@%s/temp/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
		"com.alibaba.schedulerx.protocol.Worker$WorkerHeartBeatRequest",
		codec.WithMessageContainerSerializer(),
		codec.WithSelectionEnvelopePattern([]*akka.Selection{
			{
				Type:    akka.PatternType_CHILD_NAME.Enum(),
				Matcher: proto.String("user"),
			},
			{
				Type:    akka.PatternType_CHILD_NAME.Enum(),
				Matcher: proto.String("heartbeat"),
			},
		}))
	if err != nil {
		return err
	}
	return WriteAkkaMsg(akkaMsg, conn)
}

func SendSubmitJobInstanceResp(ctx context.Context, resp *schedulerx.ServerSubmitJobInstanceResponse, senderPath string) error {
	conn, err := connpool.GetConnPool().Get(ctx)
	if err != nil {
		return err
	}
	akkaMsg, err := codec.EncodeAkkaMessage(
		resp,
		senderPath,
		fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
		"com.alibaba.schedulerx.protocol.Server$ServerSubmitJobInstanceResponse")
	if err != nil {
		return err
	}
	return WriteAkkaMsg(akkaMsg, conn)
}

func SendReportTaskStatusReq(ctx context.Context, req *schedulerx.WorkerReportJobInstanceStatusRequest) error {
	conn, err := connpool.GetConnPool().Get(ctx)
	if err != nil {
		return err
	}
	akkaMsg, err := codec.EncodeAkkaMessage(
		req,
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
	if err != nil {
		return err
	}
	return WriteAkkaMsg(akkaMsg, conn)
}

func SendServerKillJobInstanceResp(ctx context.Context, resp *schedulerx.ServerKillJobInstanceResponse, senderPath string) error {
	conn, err := connpool.GetConnPool().Get(ctx)
	if err != nil {
		return err
	}
	akkaMsg, err := codec.EncodeAkkaMessage(
		resp,
		senderPath,
		fmt.Sprintf("akka.tcp://%s@%s/user/job_instance_routing/%s", utils.GetWorkerId(), conn.LocalAddr().String(), utils.GenPathTpl()),
		"com.alibaba.schedulerx.protocol.Server$ServerKillJobInstanceResponse")
	if err != nil {
		return err
	}
	return WriteAkkaMsg(akkaMsg, conn)
}

func SendKillContainerReq(ctx context.Context, req *schedulerx.MasterKillContainerRequest, workIdAddr string) error {
	conn, err := connpool.GetConnPool().Get(ctx)
	if err != nil {
		return err
	}

	akkaMsg, err := codec.EncodeAkkaMessage(
		req,
		fmt.Sprintf("akka.tcp://server@%s/", conn.RemoteAddr().String()),
		fmt.Sprintf("akka.tcp://%s/user/container_routing/%s", workIdAddr, utils.GenPathTpl()),
		"com.alibaba.schedulerx.protocol.Worker$MasterKillContainerRequest",
		codec.WithMessageContainerSerializer(),
		codec.WithSelectionEnvelopePattern([]*akka.Selection{
			{
				Type:    akka.PatternType_CHILD_NAME.Enum(),
				Matcher: proto.String("user"),
			},
			{
				Type:    akka.PatternType_CHILD_NAME.Enum(),
				Matcher: proto.String("container_router"),
			},
		}))
	if err != nil {
		return err
	}

	return WriteAkkaMsg(akkaMsg, conn)
}
