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
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

func InitActors(actorSystem *actor.ActorSystem) error {
	// init containerActor
	containerRouterPid, err := actorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newContainerActor()
	}), actorcomm.ContainerRouterPidId)
	if err != nil {
		return err
	}

	// init jobInstanceActor
	jobInstancePid, err := actorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newJobInstanceActor(actorSystem)
	}), actorcomm.JobInstancePidId)
	if err != nil {
		return err
	}

	// init atLeastOnceDeliveryActor
	atLeastOnceDeliveryPid, err := actorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newAtLeastOnceDeliveryRoutingActor()
	}), actorcomm.AtLeastOnceDeliveryPidId)
	if err != nil {
		return err
	}

	// init taskActor
	taskActorPid, err := actorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newTaskActor(actorSystem)
	}), actorcomm.MapMasterPidId)
	if err != nil {
		return err
	}

	// init heartbeatActor
	heartbeatActorPid, err := actorSystem.Root.SpawnNamed(actor.PropsFromProducer(func() actor.Actor {
		return newHeartbeatActor(actorSystem)
	}), actorcomm.HeartbeatPidId)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case sxMsg := <-actorcomm.SxMsgReceiver():
				actorSystem.Root.Send(jobInstancePid, sxMsg)
			case taskMasterMsg := <-actorcomm.TaskMasterMsgReceiver():
				actorSystem.Root.Send(taskActorPid, taskMasterMsg)
			case containerRouterMsg := <-actorcomm.ContainerRouterMsgReceiver():
				actorSystem.Root.Send(containerRouterPid, containerRouterMsg)
			case atLeastOnceDeliveryMsg := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
				actorSystem.Root.Send(atLeastOnceDeliveryPid, atLeastOnceDeliveryMsg)
			case heartbeatMsg := <-actorcomm.HeartbeatMsgReceiver():
				actorSystem.Root.Send(heartbeatActorPid, heartbeatMsg)
			}
		}
	}()

	var (
		host = "0.0.0.0"
		port = 0 // random port
	)
	if grpcPort := config.GetWorkerConfig().GrpcPort(); grpcPort != 0 {
		port = int(grpcPort)
	}

	if config.GetWorkerConfig().Iface() != "" {
		localHost, err := utils.GetIpv4AddrByIface(config.GetWorkerConfig().Iface())
		if err != nil {
			panic(err)
		}
		host = localHost
	} else {
		localHost, err := utils.GetIpv4AddrHost()
		if err != nil {
			panic(err)
		}
		host = localHost
	}

	// The maximum limit for a subtask is 64kb, and a maximum of 1000 batches can be sent together, which is 64MB,
	// plus about 200MB for serialization and request headers.
	remoteConfig := remote.Configure(host, port,
		remote.WithDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(200*1024*1024),
				grpc.MaxCallSendMsgSize(200*1024*1024),
			)),
		remote.WithServerOptions(
			grpc.MaxRecvMsgSize(200*1024*1024),
			grpc.MaxSendMsgSize(200*1024*1024),
		))

	remoting := remote.NewRemote(actorSystem, remoteConfig)
	// Warning: must start remoting last, because it would register a default address resolver,
	// will it overwrite what we have registered ourselves.

	remoting.Register(actorcomm.ContainerRouterPidId, actor.PropsFromProducer(func() actor.Actor {
		return newContainerActor()
	}))
	remoting.Start()

	return nil
}
