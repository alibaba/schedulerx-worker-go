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

func WithDebugMode() Option {
	return func(config *WorkerConfig) {
		config.isDebugMode = true
	}
}

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

func WithWorkerLabel(workerLabel string) Option {
	return func(config *WorkerConfig) {
		config.workerLabel = workerLabel
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
	isDebugMode                     bool
	isShareContainerPool            bool
	isMapMasterFailover             bool
	isSecondDelayIntervalMS         bool
	isDispatchSecondDelayStandalone bool
	mapMasterPageSize               int32
	mapMasterQueueSize              int32
	mapMasterDispatcherSize         int32
	sharePoolSize                   int32
	workerParallelTaskMaxSize       int32
	workerMapPageSize               int32
	taskBodySizeMax                 int32
	workerLabel                     string
}

func (w *WorkerConfig) IsDebugMode() bool {
	return w.isDebugMode
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

func (w *WorkerConfig) MapMasterPageSize() int32 {
	return w.mapMasterPageSize
}

func (w *WorkerConfig) MapMasterQueueSize() int32 {
	return w.mapMasterQueueSize
}

func (w *WorkerConfig) MapMasterDispatcherSize() int32 {
	return w.mapMasterDispatcherSize
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

func (w *WorkerConfig) WorkerLabel() string {
	return w.workerLabel
}

func defaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		isDebugMode:                     false,
		isSecondDelayIntervalMS:         false,
		isShareContainerPool:            false,
		isDispatchSecondDelayStandalone: false,
		isMapMasterFailover:             true,
		mapMasterPageSize:               constants.MapMasterPageSizeDefault,
		mapMasterQueueSize:              constants.MapMasterQueueSizeDefault,
		mapMasterDispatcherSize:         constants.MapMasterDispatcherSizeDefault,
		sharePoolSize:                   constants.SharedPoolSizeDefault,
		workerParallelTaskMaxSize:       constants.ParallelTaskListSizeMax,
		workerMapPageSize:               constants.WorkerMapPageSizeDefault,
		taskBodySizeMax:                 constants.TaskBodySizeMaxDefault,
	}
}
