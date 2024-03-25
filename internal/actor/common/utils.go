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

package actorcomm

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/asynkron/protoactor-go/actor"

	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
)

const (
	workerAkkaPathPrefix           = "akka.tcp://"
	workerAkkaContainerRouting     = "container_routing"
	workerAkkaContainerRoutingPath = "/user/" + workerAkkaContainerRouting

	schedulerxServerPidId    = "schedulerx"
	ContainerRouterPidId     = "user_container_routing"
	JobInstancePidId         = "job_instance_routing"
	MapMasterPidId           = "map_master_router"
	AtLeastOnceDeliveryPidId = "at_least_once_delivery_routing"
	HeartbeatPidId           = "heartbeat_routing"
)

// GetContainerRouterPid get remote PID of container router
func GetContainerRouterPid(workerIdAddr string) *actor.PID {
	return actor.NewPID(workerIdAddr, ContainerRouterPidId)
}

// GetMapMasterPid get remote PID of mapMaster router
func GetMapMasterPid(workerIdAddr string) *actor.PID {
	return actor.NewPID(workerIdAddr, MapMasterPidId)
}

// GetHeartbeatActorPid get remote PID of heartbeat actor
func GetHeartbeatActorPid(workerIdAddr string) *actor.PID {
	return actor.NewPID(workerIdAddr, HeartbeatPidId)
}

func IsSchedulerxServer(pid *actor.PID) bool {
	return pid.GetId() == schedulerxServerPidId
}

func SchedulerxServerPid(ctx context.Context) *actor.PID {
	conn, err := pool.GetConnPool().Get(ctx)
	if err != nil {
		return &actor.PID{}
	}
	return actor.NewPID(conn.RemoteAddr().String(), schedulerxServerPidId)
}

// GetRealWorkerAddr get the real workerAddr, which is the address of the remote worker's ActorSystem
// The workerAddr issued by the server is the address reported by the heartbeat.
// It is the connection address obtained from the connection pool, not the ActorSystem address, so it needs to be converted.
func GetRealWorkerAddr(workerIdAddr string) string {
	parts := strings.Split(workerIdAddr, "@")
	workerAddr := parts[1]
	addrParts := strings.Split(workerAddr, ":")

	var (
		host, port string
	)

	// Note: akka_port is used for akka communication between master and server, rpc_port is used for actorSystem communication between master and worker.
	// Old format (compatible): host:akka_port
	// New format: host:akka_port:rpc_port
	if !(len(addrParts) == 2 || len(addrParts) == 3) {
		panic(fmt.Sprintf("invalid worker addr: %s", workerAddr))
	}

	host = addrParts[0]

	if len(addrParts) == 2 {
		port = addrParts[1]
	} else if len(addrParts) == 3 {
		port = addrParts[2]
	}

	return net.JoinHostPort(host, port)
}
