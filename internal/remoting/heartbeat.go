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
	"math"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/shirou/gopsutil/load"
	"google.golang.org/protobuf/proto"

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

func KeepHeartbeat(ctx context.Context, actorSystem *actor.ActorSystem) {
	var (
		taskMasterPool = masterpool.GetTaskMasterPool()
		connpool       = pool.GetConnPool()
		groupManager   = discovery.GetGroupManager()
	)

	heartbeat := func() {
		_, actorSystemPort, err := actorSystem.GetHostPort()
		if err != nil {
			logger.Errorf("Write heartbeat to remote failed due to get actorSystem port failed, err=%s", err.Error())
			return
		}
		for groupId, appGroupId := range groupManager.GroupId2AppGroupIdMap() {
			jobInstanceIds := taskMasterPool.GetInstanceIds(int64(appGroupId))
			heartbeatReq := genHeartBeatRequest(groupId, int64(appGroupId), jobInstanceIds, actorSystemPort)
			if err := sendHeartbeat(ctx, heartbeatReq); err != nil {
				if errors.Is(err, syscall.EPIPE) || errors.Is(err, os.ErrDeadlineExceeded) {
					connpool.ReconnectTrigger() <- struct{}{}
				}
				logger.Warnf("Write heartbeat to server failed, had already re-connect with server, reason=%s", err.Error())
				continue
			}
			logger.Debugf("Write heartbeat to remote succeed.")
		}
	}
	heartbeat()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		heartbeat()
	}
}

func sendHeartbeat(ctx context.Context, req *schedulerx.WorkerHeartBeatRequest) error {
	conn, err := pool.GetConnPool().Get(ctx)
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
	return trans.WriteAkkaMsg(akkaMsg, conn)
}

func getLoadAvg() ([]float64, error) {
	avg, err := load.Avg()
	if err != nil {
		return nil, err
	}
	return []float64{avg.Load1, avg.Load5, avg.Load15}, nil
}

func metricsJsonStr() string {
	loadAvg, err := getLoadAvg()
	if err != nil {
		logger.Warnf("Failed to get system load average:" + err.Error())
		return "{}"
	}
	cpus := runtime.NumCPU()
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)

	metricsJson := map[string]float64{
		"cpuLoad1":      loadAvg[0],
		"cpuLoad5":      loadAvg[1],
		"cpuProcessors": float64(cpus),
		"heap1Usage":    float64(memstats.HeapInuse) / float64(memstats.HeapSys),
		"heap1Used":     float64(memstats.HeapInuse) / 1024 / 1024,
		"heap5Usage":    float64(memstats.HeapInuse) / math.Max(float64(memstats.HeapSys), 1),
		"heapMax":       float64(memstats.HeapSys) / 1024 / 1024,
	}
	ret, err := json.Marshal(metricsJson)
	if err != nil {
		logger.Warnf("Get metric json failed for heartbeat, err=%s", err.Error())
		return "{}"
	}
	return string(ret)
}

func genHeartBeatRequest(groupId string, appGroupId int64, jobInstanceIds []int64, actorSystemPort int) *schedulerx.WorkerHeartBeatRequest {
	return &schedulerx.WorkerHeartBeatRequest{
		GroupId:       proto.String(groupId),
		WorkerId:      proto.String(utils.GetWorkerId()),
		Version:       proto.String(version.Version()),
		MetricsJson:   proto.String(metricsJsonStr()),
		JobInstanceId: jobInstanceIds,
		Starter:       proto.String("go"),
		AppGroupId:    proto.Int64(appGroupId),
		Source:        proto.String("unknown"),
		RpcPort:       proto.Int32(int32(actorSystemPort)),
	}
}
