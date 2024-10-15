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

package schedulerx

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"

	"github.com/alibaba/schedulerx-worker-go/config"
	sxactor "github.com/alibaba/schedulerx-worker-go/internal/actor"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/tracer"
)

var (
	client *Client
	err    error
	once   sync.Once
)

type Client struct {
	cfg            *Config
	opts           *Options
	connpool       pool.ConnPool
	tasks          *tasks.TaskMap
	actorSystem    *actor.ActorSystem
	taskMasterPool *masterpool.TaskMasterPool
}

type Config struct {
	DomainName string `json:"DomainName"`
	Endpoint   string `json:"Endpoint"`
	Namespace  string `json:"Namespace"`
	// GroupId may be exited multiple, separated by comma, such as "group1,group2"
	GroupId string `json:"GroupId"`
	// AppKey may be exited multiple, separated by comma, such as "appKey1,appKey2",
	// appKey and groupId are in one-to-one correspondence
	AppKey string `json:"AppKey"`
}

func (c *Config) IsValid() bool {
	if c == nil {
		return false
	}
	if c.Namespace == "" || c.GroupId == "" || c.AppKey == "" {
		return false
	}
	if c.Endpoint == "" && c.DomainName == "" {
		return false
	}
	return true
}

type Options struct{}

type Option func(*Options)

func WithWorkerConfig(cfg *config.WorkerConfig) Option {
	return func(opt *Options) {
		config.InitWorkerConfig(cfg)
	}
}

func WithTracer(t tracer.Tracer) Option {
	return func(opt *Options) {
		tracer.InitTracer(t)
	}
}

func GetClient(cfg *Config, opts ...Option) (*Client, error) {
	once.Do(func() {
		client, err = newClient(cfg, opts...)
	})
	return client, err
}

func newClient(cfg *Config, opts ...Option) (*Client, error) {
	if !cfg.IsValid() {
		return nil, fmt.Errorf("invalid console config, cfg=%+v", cfg)
	}

	ctx := context.Background()
	options := new(Options)
	for _, opt := range opts {
		opt(options)
	}

	// Init discovery
	openAPIClient := openapi.NewClient(
		openapi.WithHTTPClient(http.Client{Timeout: time.Second * 3}),
		openapi.WithNamespace(cfg.Namespace),
		openapi.WithGroupId(cfg.GroupId),
		openapi.WithOpenAPIDomain(cfg.DomainName),
		openapi.WithOpenAPIEndpoint(cfg.Endpoint),
		openapi.WithAppKey(cfg.AppKey),
	)
	openapi.InitOpenAPIClient(openAPIClient)
	discovery.GetGroupManager().StartServerDiscovery(cfg.GroupId, cfg.AppKey)
	serverDiscover := discovery.GetDiscovery(cfg.GroupId)
	getActiveServer := func() string {
		return serverDiscover.ActiveServer()
	}

	// Init connection pool
	dialer := func() (net.Conn, error) {
		logger.Infof("SchedulerX discovery active server addr=%s", getActiveServer())
		return net.DialTimeout("tcp", getActiveServer(), time.Millisecond*500)
	}
	singleConnPool := pool.NewSingleConnPool(ctx, dialer,
		pool.WithPostDialer(remoting.Handshake),
		pool.WithAddrChangedSignalCh(serverDiscover.ResultChangedCh()))
	pool.InitConnPool(singleConnPool)
	if conn, err := singleConnPool.Get(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect schedulerx server, maybe network was broken, err=%s", err.Error())
	} else {
		logger.Infof("SchedulerX server connected, remoteAddr=%s, localAddr=%s", conn.RemoteAddr(), conn.LocalAddr().String())
	}

	taskMap := tasks.GetTaskMap()
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(taskMap))

	// Init actors
	actorSystem := actor.NewActorSystem()
	actorcomm.InitActorSystem(actorSystem)
	if err := sxactor.InitActors(actorSystem); err != nil {
		return nil, fmt.Errorf("Init actors faild, err=%s. ", err.Error())
	}

	// Keep heartbeat, and receive message
	// KeepHeartbeat must after init actors, so that can get actorSystemPort from actorSystem
	go remoting.KeepHeartbeat(ctx, actorSystem, cfg.AppKey)
	go remoting.OnMsgReceived(ctx)
	// send worker offline heartbeat when shutdown
	go remoting.Shutdown(ctx, actorSystem, cfg.AppKey)

	return &Client{
		cfg:         cfg,
		opts:        options,
		tasks:       taskMap,
		actorSystem: actorSystem,
	}, nil
}

func (c *Client) RegisterTask(name string, task processor.Processor) {
	c.tasks.Register(name, task)
}
