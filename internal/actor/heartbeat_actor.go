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

package actor

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ actor.Actor = &heartbeatActor{}

// heartbeatActor is the
type heartbeatActor struct {
	connpool       pool.ConnPool
	taskMasterPool *masterpool.TaskMasterPool
}

func newHeartbeatActor(actorSystem *actor.ActorSystem) *heartbeatActor {
	tActor := &heartbeatActor{
		connpool:       pool.GetConnPool(),
		taskMasterPool: masterpool.GetTaskMasterPool(),
	}

	resolver := func(pid *actor.PID) (actor.Process, bool) {
		if actorcomm.IsSchedulerxServer(pid) {
			return newHeartbeatProcessor(tActor.connpool), true
		}

		// If communicate with actors other than server, then use the default handler (return false)
		return nil, false
	}
	actorSystem.ProcessRegistry.RegisterAddressResolver(resolver)

	return tActor
}

func (a *heartbeatActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *schedulerx.MasterCheckWorkerAliveRequest:
		a.handleCheckWorkerAlive(ctx, msg)
	case *schedulerx.ContainerCheckZombieRequest:
		a.handleCheckZombie(ctx, msg)
	default:
		logger.Warnf("[heartbeatActor] receive unknown message, msg=%+v", ctx.Message())
	}
}

func (a *heartbeatActor) handleCheckWorkerAlive(ctx actor.Context, req *schedulerx.MasterCheckWorkerAliveRequest) {
	resp := &schedulerx.MasterCheckWorkerAliveResponse{
		Success: proto.Bool(true),
	}
	if req.GetDispatchMode() == string(common.TaskDispatchModePull) {
		resp = &schedulerx.MasterCheckWorkerAliveResponse{
			Success: proto.Bool(false),
			Message: proto.String(fmt.Sprintf("%d is crashed in PullMananger", req.GetJobInstanceId())),
		}
	} else {
		// FIXME implement metric monitor
		resp = &schedulerx.MasterCheckWorkerAliveResponse{
			Success: proto.Bool(true),
		}
	}
	if senderPid := ctx.Sender(); senderPid != nil {
		ctx.Send(senderPid, resp)
	} else {
		logger.Warnf("Cannot send MasterCheckWorkerAliveRequest due to sender is unknown in handleCheckWorkerAlive of heartbeatActor, request=%+v", req)
	}
}

func (a *heartbeatActor) handleCheckZombie(ctx actor.Context, req *schedulerx.ContainerCheckZombieRequest) {
	zombieJobInstanceIds := make([]int64, 0, len(req.GetJobInstanceId()))
	for _, jobInstanceId := range req.GetJobInstanceId() {
		if !a.taskMasterPool.Contains(jobInstanceId) {
			zombieJobInstanceIds = append(zombieJobInstanceIds, jobInstanceId)
		}
	}
	resp := &schedulerx.ContainerCheckZombieResponse{
		ZombieJobInstanceId: zombieJobInstanceIds,
	}
	if senderPid := ctx.Sender(); senderPid != nil {
		ctx.Send(senderPid, resp)
	} else {
		logger.Warnf("Cannot send ContainerCheckZombieRequest due to sender is unknown in handleCheckZombie of heartbeatActor, request=%+v", req)
	}
}
