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

package config

import (
	"sync"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/constants"
)

var (
	workerConfig *WorkerConfig
	once         sync.Once
)

func InitWorkerConfig(cfg *WorkerConfig) {
	once.Do(func() {
		workerConfig = cfg
	})
}

func GetWorkerConfig() *WorkerConfig {
	if workerConfig != nil {
		return workerConfig
	}
	return defaultWorkerConfig()
}

type Option func(*WorkerConfig)

func WithEnableShareContainerPool() Option {
	return func(config *WorkerConfig) {
		config.isShareContainerPool = true
	}
}

func WithDisableMapMasterFailover() Option {
	return func(config *WorkerConfig) {
		config.isMapMasterFailover = false
	}
}

func WithEnableSecondDelayIntervalMS() Option {
	return func(config *WorkerConfig) {
		config.isSecondDelayIntervalMS = true
	}
}

func WithEnableDispatchSecondDelayStandalone() Option {
	return func(config *WorkerConfig) {
		config.isDispatchSecondDelayStandalone = true
	}
}

func WithDisableBroadcastMasterExec() Option {
	return func(config *WorkerConfig) {
		config.broadcastMasterExecEnable = false
	}
}

func WithBroadcastDispatchRetryTimes(broadcastDispatchRetryTimes int32) Option {
	return func(config *WorkerConfig) {
		config.broadcastDispatchRetryTimes = broadcastDispatchRetryTimes
	}
}

func WithMapMasterPageSize(mapMasterPageSize int32) Option {
	return func(config *WorkerConfig) {
		config.mapMasterPageSize = mapMasterPageSize
	}
}

func WithMapMasterQueueSize(mapMasterQueueSize int32) Option {
	return func(config *WorkerConfig) {
		config.mapMasterQueueSize = mapMasterQueueSize
	}
}

func WithMapMasterDispatcherSize(mapMasterDispatcherSize int32) Option {
	return func(config *WorkerConfig) {
		config.mapMasterDispatcherSize = mapMasterDispatcherSize
	}
}

func WithMapMasterStatusCheckInterval(mapMasterStatusCheckInterval time.Duration) Option {
	return func(config *WorkerConfig) {
		config.mapMasterStatusCheckInterval = mapMasterStatusCheckInterval
	}
}

func WithSharePoolSize(sharePoolSize int32) Option {
	return func(config *WorkerConfig) {
		config.sharePoolSize = sharePoolSize
	}
}

func WithWorkerParallelTaskMaxSize(workerParallelTaskMaxSize int32) Option {
	return func(config *WorkerConfig) {
		config.workerParallelTaskMaxSize = workerParallelTaskMaxSize
	}
}

func WithWorkerMapPageSize(workerMapPageSize int32) Option {
	return func(config *WorkerConfig) {
		config.workerMapPageSize = workerMapPageSize
	}
}

func WithTaskBodySizeMax(taskBodySizeMax int32) Option {
	return func(config *WorkerConfig) {
		config.taskBodySizeMax = taskBodySizeMax
	}
}

func WithActorSystemPort(port int32) Option {
	return func(config *WorkerConfig) {
		config.actorSystemPort = port
	}
}

func NewWorkerConfig(opts ...Option) *WorkerConfig {
	once.Do(func() {
		workerConfig = defaultWorkerConfig()
		for _, opt := range opts {
			opt(workerConfig)
		}
	})
	return workerConfig
}

type WorkerConfig struct {
	isShareContainerPool            bool
	isMapMasterFailover             bool
	isSecondDelayIntervalMS         bool
	isDispatchSecondDelayStandalone bool
	broadcastMasterExecEnable       bool
	broadcastDispatchRetryTimes     int32
	mapMasterPageSize               int32
	mapMasterQueueSize              int32
	mapMasterDispatcherSize         int32
	mapMasterStatusCheckInterval    time.Duration
	sharePoolSize                   int32
	workerParallelTaskMaxSize       int32
	workerMapPageSize               int32
	taskBodySizeMax                 int32
	actorSystemPort                 int32
}

func (w *WorkerConfig) IsShareContainerPool() bool {
	return w.isShareContainerPool
}

func (w *WorkerConfig) IsMapMasterFailover() bool {
	return w.isMapMasterFailover
}

func (w *WorkerConfig) IsSecondDelayIntervalMS() bool {
	return w.isSecondDelayIntervalMS
}

func (w *WorkerConfig) IsDispatchSecondDelayStandalone() bool {
	return w.isDispatchSecondDelayStandalone
}

func (w *WorkerConfig) BroadcastMasterExecEnable() bool {
	return w.broadcastMasterExecEnable
}

func (w *WorkerConfig) BroadcastDispatchRetryTimes() int32 {
	return w.broadcastDispatchRetryTimes
}

func (w *WorkerConfig) MapMasterPageSize() int32 {
	return w.mapMasterPageSize
}

func (w *WorkerConfig) MapMasterQueueSize() int32 {
	return w.mapMasterQueueSize
}

func (w *WorkerConfig) MapMasterDispatcherSize() int32 {
	return w.mapMasterDispatcherSize
}

func (w *WorkerConfig) MapMasterStatusCheckInterval() time.Duration {
	return w.mapMasterStatusCheckInterval
}

func (w *WorkerConfig) SharePoolSize() int32 {
	return w.sharePoolSize
}

func (w *WorkerConfig) WorkerParallelTaskMaxSize() int32 {
	return w.workerParallelTaskMaxSize
}

func (w *WorkerConfig) WorkerMapPageSize() int32 {
	return w.workerMapPageSize
}

func (w *WorkerConfig) TaskBodySizeMax() int32 {
	return w.taskBodySizeMax
}

func (w *WorkerConfig) ActorSystemPort() int32 {
	return w.actorSystemPort
}

func defaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		isSecondDelayIntervalMS:         false,
		isShareContainerPool:            false,
		isDispatchSecondDelayStandalone: false,
		isMapMasterFailover:             true,
		broadcastMasterExecEnable:       true,
		broadcastDispatchRetryTimes:     constants.BroadcastDispatchRetryTimesDefault,
		mapMasterPageSize:               constants.MapMasterPageSizeDefault,
		mapMasterQueueSize:              constants.MapMasterQueueSizeDefault,
		mapMasterDispatcherSize:         constants.MapMasterDispatcherSizeDefault,
		mapMasterStatusCheckInterval:    constants.MapMasterStatusCheckIntervalDefault,
		sharePoolSize:                   constants.SharedPoolSizeDefault,
		workerParallelTaskMaxSize:       constants.ParallelTaskListSizeMaxDefault,
		workerMapPageSize:               constants.WorkerMapPageSizeDefault,
		taskBodySizeMax:                 constants.TaskBodySizeMaxDefault,
	}
}
