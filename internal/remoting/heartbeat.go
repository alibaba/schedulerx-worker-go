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
	"math"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/load"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/connpool"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/trans"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/internal/version"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	heartbeatInterval        = 5 * time.Second
	waitHeartbeatRespTimeout = 5 * time.Second
)

func KeepHeartbeat(ctx context.Context) {
	var (
		taskMasterPool = masterpool.GetTaskMasterPool()
		connpool       = connpool.GetConnPool()
		groupManager   = discovery.GetGroupManager()
	)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	heartbeat := func() {
		for groupId, appGroupId := range groupManager.GroupId2AppGroupIdMap() {
			appKey := groupManager.GetAppKeyByGroupId(groupId)
			jobInstanceIds := taskMasterPool.GetInstanceIds(int64(appGroupId))
			heartbeatReq := genHeartBeatRequest(groupId, appKey, int64(appGroupId), jobInstanceIds)
			err := trans.SendHeartbeatReq(ctx, heartbeatReq)
			if err != nil {
				if errors.Is(err, unix.EPIPE) || errors.Is(err, os.ErrDeadlineExceeded) {
					connpool.ReconnectTrigger() <- struct{}{}
				}
				logger.Errorf("Write heartbeat to remote failed, err=%s", err.Error())
				continue
			}
			logger.Debugf("Write heartbeat=[%+v] to remote succeed", heartbeatReq)
		}
	}

	heartbeat()
	for range ticker.C {
		heartbeat()
	}
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

func genHeartBeatRequest(groupId, appKey string, appGroupId int64, jobInstanceIds []int64) *schedulerx.WorkerHeartBeatRequest {
	return &schedulerx.WorkerHeartBeatRequest{
		GroupId:       proto.String(groupId),
		WorkerId:      proto.String(utils.GetWorkerId()),
		Version:       proto.String(version.Version()),
		MetricsJson:   proto.String(metricsJsonStr()),
		JobInstanceId: jobInstanceIds,
		Starter:       proto.String("go"),
		AppGroupId:    proto.Int64(appGroupId),
		Source:        proto.String("unknown"),
		AppKey:        proto.String(appKey),
	}
}
