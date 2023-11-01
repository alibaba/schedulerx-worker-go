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
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/codec"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/connpool"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

func OnMsgReceived(ctx context.Context) {
	pool := connpool.GetConnPool()

	var dataLen uint32
	hdrBuf := make([]byte, constants.TransportHeaderSize)
	for {
		conn, err := pool.Get(ctx)
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
			pool.ReconnectTrigger() <- struct{}{}
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
			logger.Infof("Read akka message failed, reason=%s", err.Error())
			continue
		}

		switch msg := msg.(type) {
		case *akka.AkkaControlMessage:
			if int32(msg.GetCommandType()) == int32(akka.CommandType_HEARTBEAT) {
				logger.Debugf("Receive heartbeat from server, heartbeat=%+v", msg)
				continue
			} else {
				logger.Errorf("Receive unexpect control message from server, message=%+v", msg)
				continue
			}
		case *schedulerx.ServerSubmitJobInstanceRequest:
			if err := handleSubmitJobInstance(ctx, msg, senderPath); err != nil {
				logger.Errorf("handleSubmitJobInstanceRequest failed, err=%s", err.Error())
				continue
			}
		case *schedulerx.ServerKillJobInstanceRequest:
			if err := handleKillJobInstance(ctx, msg, senderPath); err != nil {
				logger.Errorf("handleKillJobInstanceRequest failed, err=%s", err.Error())
				continue
			}
		case *schedulerx.WorkerHeartBeatResponse:
			logger.Debugf("Receive heartbeat from server, heartbeat=%+v", msg)
			continue
		default:
			logger.Debugf("Unknown msg type, msg=%+v", msg)
			continue
		}
	}
}

func handleSubmitJobInstance(ctx context.Context, req *schedulerx.ServerSubmitJobInstanceRequest, senderPath string) error {
	logger.Infof("handleSubmitJobInstance, jobInstanceId=%d", req.GetJobInstanceId())
	if masterpool.GetTaskMasterPool().Contains(req.GetJobInstanceId()) {
		errMsg := fmt.Sprintf("jobInstanceId=%d is still running!", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerSubmitJobInstanceResponse{
			Success: proto.Bool(false),
			Message: proto.String(errMsg),
		}
		return trans.SendSubmitJobInstanceResp(ctx, resp, senderPath)
	} else {
		resp := &schedulerx.ServerSubmitJobInstanceResponse{
			Success: proto.Bool(true),
		}
		if err := trans.SendSubmitJobInstanceResp(ctx, resp, senderPath); err != nil {
			return err
		}
		jobInstanceInfo := convert2JobInstanceInfo(req)
		taskMaster := master.NewStandaloneTaskMaster(jobInstanceInfo)
		masterpool.GetTaskMasterPool().Put(jobInstanceInfo.GetJobInstanceId(), taskMaster)
		if err := taskMaster.SubmitInstance(ctx, jobInstanceInfo); err != nil {
			return err
		}
		logger.Infof("Submit jobInstanceId=%d succeed", req.GetJobInstanceId())
		return nil
	}
}

func convert2JobInstanceInfo(req *schedulerx.ServerSubmitJobInstanceRequest) *common.JobInstanceInfo {
	jobInstanceInfo := new(common.JobInstanceInfo)
	jobInstanceInfo.SetJobId(req.GetJobId())
	jobInstanceInfo.SetJobInstanceId(req.GetJobInstanceId())
	jobInstanceInfo.SetExecuteMode(req.GetExecuteMode())
	jobInstanceInfo.SetJobType(req.GetJobType())
	jobInstanceInfo.SetContent(req.GetContent())
	jobInstanceInfo.SetUser(req.GetUser())
	jobInstanceInfo.SetScheduleTime(time.Duration(req.GetScheduleTime()))
	jobInstanceInfo.SetDataTime(time.Duration(req.GetDataTime()))
	jobInstanceInfo.SetAllWorkers(utils.ShuffleStringSlice(req.GetWorkers()))
	jobInstanceInfo.SetJobConcurrency(req.GetJobConcurrency())
	jobInstanceInfo.SetRegionId(req.GetRegionId())
	jobInstanceInfo.SetAppGroupId(req.GetAppGroupId())
	jobInstanceInfo.SetTimeType(req.GetTimeType())
	jobInstanceInfo.SetTimeExpression(req.GetTimeExpression())
	jobInstanceInfo.SetGroupId(req.GetGroupId())
	jobInstanceInfo.SetParameters(req.GetParameters())
	jobInstanceInfo.SetXattrs(req.GetXattrs())
	jobInstanceInfo.SetInstanceParameters(req.GetInstanceParameters())
	jobInstanceInfo.SetUpstreamData(convert2JobInstanceData(req.GetUpstreamData()))
	jobInstanceInfo.SetMaxAttempt(req.GetMaxAttempt())
	jobInstanceInfo.SetAttempt(req.GetAttempt())
	jobInstanceInfo.SetWfInstanceId(req.GetWfInstanceId())
	jobInstanceInfo.SetJobName(req.GetJobName())

	return jobInstanceInfo
}

func convertKillJobInstanceReq2JobInstanceInfo(req *schedulerx.ServerKillJobInstanceRequest) *common.JobInstanceInfo {
	jobInstanceInfo := new(common.JobInstanceInfo)
	jobInstanceInfo.SetJobId(req.GetJobId())
	jobInstanceInfo.SetJobInstanceId(req.GetJobInstanceId())
	jobInstanceInfo.SetUser(req.GetUser())
	jobInstanceInfo.SetJobType(req.GetJobType())
	jobInstanceInfo.SetContent(req.GetContent())
	jobInstanceInfo.SetAppGroupId(req.GetAppGroupId())
	jobInstanceInfo.SetXattrs(req.GetXattrs())
	jobInstanceInfo.SetExecuteMode(req.GetExecuteMode())
	jobInstanceInfo.SetAppGroupId(req.GetAppGroupId())
	return jobInstanceInfo
}

func convert2JobInstanceData(datas []*schedulerx.UpstreamData) []*common.JobInstanceData {
	var ret []*common.JobInstanceData
	for _, data := range datas {
		tmp := new(common.JobInstanceData)
		tmp.SetData(data.GetData())
		tmp.SetJobName(data.GetJobName())

		ret = append(ret, tmp)
	}

	return ret
}

func handleKillJobInstance(ctx context.Context, req *schedulerx.ServerKillJobInstanceRequest, senderPath string) error {
	logger.Infof("handleKillJobInstance, jobInstanceId=%d ", req.GetJobInstanceId())
	uniqueId := utils.GetUniqueIdWithoutTaskId(req.GetJobId(), req.GetJobInstanceId())
	if !masterpool.GetTaskMasterPool().Contains(req.GetJobInstanceId()) {
		errMsg := fmt.Sprintf("%d is not exist", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerKillJobInstanceResponse{
			Success: proto.Bool(false),
			Message: proto.String(errMsg),
		}
		return trans.SendServerKillJobInstanceResp(ctx, resp, senderPath)
	} else {
		errMsg := fmt.Sprintf("%d killed from server", req.GetJobInstanceId())
		logger.Infof(errMsg)
		resp := &schedulerx.ServerKillJobInstanceResponse{
			Success: proto.Bool(true),
		}

		jobInstanceInfo := convertKillJobInstanceReq2JobInstanceInfo(req)
		taskMaster := masterpool.GetTaskMasterPool().Get(jobInstanceInfo.GetJobInstanceId())
		if err := taskMaster.KillInstance("killed from server"); err != nil {
			resp = &schedulerx.ServerKillJobInstanceResponse{
				Success: proto.Bool(false),
				Message: proto.String(err.Error()),
			}
			logger.Errorf("[JobInstanceActor]handleKillJobInstance error, uniqueId:%s", uniqueId)
		}

		if err := trans.SendServerKillJobInstanceResp(ctx, resp, senderPath); err != nil {
			return err
		}

		logger.Infof("Kill jobInstanceId=%d succeed", req.GetJobInstanceId())
		return nil
	}
}
