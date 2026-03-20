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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/codec"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/internal/version"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	heartbeatInterval        = 5 * time.Second
	waitHeartbeatRespTimeout = 5 * time.Second
)

func KeepHeartbeat(ctx context.Context, actorSystem *actor.ActorSystem, stopChan chan os.Signal) {
	var (
		taskMasterPool = masterpool.GetTaskMasterPool()
		groupManager   = discovery.GetGroupManager()
	)

	heartbeat := func(online bool) {
		_, actorSystemPort, err := actorSystem.GetHostPort()
		if err != nil {
			logger.Errorf("Write heartbeat to remote failed due to get actorSystem port failed, err=%s", err.Error())
			return
		}
		for groupId, appGroupId := range groupManager.GroupId2AppGroupIdMap() {
			appKey := groupManager.GetAppKeyByGroupId(groupId)
			jobInstanceIds := taskMasterPool.GetInstanceIds(appGroupId)
			heartbeatReq := genHeartBeatRequest(groupId, appGroupId, jobInstanceIds, actorSystemPort, online, appKey)
			if err := sendHeartbeat(ctx, groupId, heartbeatReq); err != nil {
				logger.Warnf("Write heartbeat to server failed, groupId=%s, reason=%s", groupId, err.Error())
				continue
			}
			utils.GetHealthTimeHolder().ResetServerHeartbeatTime()
			logger.Debugf("Write heartbeat to remote succeed, groupId=%s", groupId)
		}
	}
	heartbeat(true)

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopChan:
			heartbeat(false)
			logger.Infof("Write shutdown heartbeat to remote succeed.")
			return
		case <-ticker.C:
			heartbeat(true)
		}
	}
}

func sendHeartbeat(ctx context.Context, groupId string, req *schedulerx.WorkerHeartBeatRequest) error {
	connPool := pool.GetConnPoolManager().GetOrCreate(groupId)
	conn, err := connPool.Get(ctx)
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

	if writeErr := trans.WriteAkkaMsg(akkaMsg, conn); writeErr != nil {
		if errors.Is(writeErr, syscall.EPIPE) || errors.Is(writeErr, os.ErrDeadlineExceeded) {
			connPool.ReconnectTrigger() <- struct{}{}
		}
		return writeErr
	}
	return nil
}

func metricsJsonStr() string {
	loadAvg, err := load.Avg()
	if err != nil {
		logger.Warnf("Failed to get system load average:" + err.Error())
		return "{}"
	}

	cpus, _ := strconv.Atoi(os.Getenv("SIGMA_MAX_PROCESSORS_LIMIT"))
	if cpus <= 0 {
		cpus = runtime.NumCPU()
	}

	ms, err := mem.VirtualMemory()
	if err != nil {
		logger.Warnf("Failed to get system mem info:" + err.Error())
		return "{}"
	}

	metricsJson := map[string]float64{
		"cpuLoad1":      loadAvg.Load1,
		"cpuLoad5":      loadAvg.Load5,
		"cpuProcessors": float64(cpus),
		"heap1Usage":    ms.UsedPercent / 100,
		"heap1Used":     float64(ms.Used) / 1024 / 1024,
		"heap5Usage":    ms.UsedPercent / 100,
		"heapMax":       float64(ms.Available+ms.Used) / 1024 / 1024,
	}
	diskStat, err := disk.Usage("/")
	if err != nil {
		fmt.Println("Failed to get system disk usage info:" + err.Error())
	} else {
		diskUsed := diskStat.Used / 1024 / 1024
		diskFree := diskStat.Free / 1024 / 1024
		diskMax := float64(diskUsed + diskFree)
		metricsJson["diskUsed"] = float64(diskUsed)
		metricsJson["diskMax"] = diskMax
		metricsJson["diskUsage"] = float64(diskUsed) / diskMax
	}
	ret, err := json.Marshal(metricsJson)
	if err != nil {
		logger.Warnf("Get metric json failed for heartbeat, err=%s", err.Error())
		return "{}"
	}
	return string(ret)
}

func genHeartBeatRequest(groupId string, appGroupId int64, jobInstanceIds []int64, actorSystemPort int, online bool, appKey string) *schedulerx.WorkerHeartBeatRequest {
	return &schedulerx.WorkerHeartBeatRequest{
		GroupId:       proto.String(groupId),
		WorkerId:      proto.String(utils.GetWorkerId()),
		Version:       proto.String(version.Version()),
		MetricsJson:   proto.String(metricsJsonStr()),
		JobInstanceId: jobInstanceIds,
		Starter:       proto.String("go"),
		AppGroupId:    proto.Int64(appGroupId),
		Source:        proto.String("unknown"),
		Label:         proto.String(config.GetWorkerConfig().Label()),
		Online:        proto.Bool(online),
		AppKey:        proto.String(appKey),
		RpcPort:       proto.Int32(int32(actorSystemPort)),
	}
}
