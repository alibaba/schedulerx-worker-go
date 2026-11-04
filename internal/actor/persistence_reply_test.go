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
	"context"
	"errors"
	"net"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	pa "github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

type persistenceReplyPool struct{ pool.ConnPool }

func (persistenceReplyPool) Get(context.Context) (net.Conn, error) {
	return nil, errors.New("test transport uses a local actor")
}

type persistenceReplyContext struct {
	pa.Context
	system    *pa.ActorSystem
	responder *pa.PID
	message   interface{}
}

func (c persistenceReplyContext) Message() interface{} { return c.message }
func (c persistenceReplyContext) RequestFuture(_ *pa.PID, msg interface{}, timeout time.Duration) *pa.Future {
	return c.system.Root.RequestFuture(c.responder, msg, timeout)
}

func TestPersistenceActorUsesRequestScopedReply(t *testing.T) {
	for _, kind := range []string{"pull", "batch update", "create"} {
		for _, scoped := range []bool{true, false} {
			t.Run(kind+map[bool]string{true: "/scoped-late", false: "/legacy"}[scoped], func(t *testing.T) {
				system := pa.NewActorSystem()
				defer system.Shutdown()
				var req, response interface{}
				var takeLegacy func() interface{}
				switch kind {
				case "pull":
					req = &schedulerx.WorkerPullTasksRequest{JobInstanceId: proto.Int64(1001)}
					response = &schedulerx.WorkerPullTasksResponse{Success: proto.Bool(true)}
					takeLegacy = func() interface{} {
						select {
						case v := <-actorcomm.WorkerPullTasksRespMsgSender():
							return v
						default:
							return nil
						}
					}
				case "batch update":
					req = &schedulerx.WorkerBatchUpdateTaskStatusRequest{JobInstanceId: proto.Int64(1001)}
					response = &schedulerx.WorkerBatchUpdateTaskStatusResponse{Success: proto.Bool(true), AffectCnt: proto.Int32(3)}
					takeLegacy = func() interface{} {
						select {
						case v := <-actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender():
							return v
						default:
							return nil
						}
					}
				case "create":
					req = &schedulerx.WorkerBatchCreateTasksRequest{JobInstanceId: proto.Int64(1001)}
					response = &schedulerx.WorkerBatchCreateTasksResponse{Success: proto.Bool(true)}
					takeLegacy = func() interface{} {
						select {
						case v := <-actorcomm.WorkerBatchCreateTasksRespMsgSender():
							return v
						default:
							return nil
						}
					}
				}
				for takeLegacy() != nil {
				}
				responder := system.Root.Spawn(pa.PropsFromFunc(func(ctx pa.Context) {
					if _, ok := ctx.Message().(*actorcomm.SchedulerWrappedMsg); ok {
						ctx.Respond(response)
					}
				}))
				defer system.Root.Stop(responder)
				wrapped := &actorcomm.SchedulerWrappedMsg{
					Ctx: pool.WithConnPool(context.Background(), persistenceReplyPool{}), Msg: req,
				}
				if scoped {
					wrapped.Reply = make(chan interface{}, 1)
				}
				ctx := persistenceReplyContext{system: system, responder: responder, message: wrapped}
				done := make(chan struct{})
				go func() { defer close(done); new(taskActor).Receive(ctx) }()
				// No caller receives until the actor has finished: a late response
				// must fit the private buffer even when its waiter has gone away.
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("late persistence response blocked actor")
				}
				if scoped {
					select {
					case got := <-wrapped.Reply:
						if got != response {
							t.Fatalf("wrong scoped reply: %v", got)
						}
					default:
						t.Fatal("response was not routed to its request")
					}
					if takeLegacy() != nil {
						t.Fatal("scoped response leaked to global channel")
					}
				} else if takeLegacy() != response {
					t.Fatal("legacy response route changed")
				}
			})
		}
	}
}
