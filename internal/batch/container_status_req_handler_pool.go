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

package batch

import (
	"sync"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
)

var (
	containerStatusReqHandlerPool *ContainerStatusReqHandlerPool
	once                          sync.Once
)

func GetContainerStatusReqHandlerPool() *ContainerStatusReqHandlerPool {
	once.Do(func() {
		containerStatusReqHandlerPool = NewContainerStatusReqHandlerPool()
	})
	return containerStatusReqHandlerPool
}

// ContainerStatusReqHandlerPool a reqs handler per jobInstance
type ContainerStatusReqHandlerPool struct {
	handlers *sync.Map // Map<Long, ContainerStatusReqHandler<ContainerReportTaskStatusRequest>>
}

func NewContainerStatusReqHandlerPool() *ContainerStatusReqHandlerPool {
	return &ContainerStatusReqHandlerPool{
		handlers: new(sync.Map),
	}
}

func (p *ContainerStatusReqHandlerPool) Start(jobInstanceId int64, reqHandler *ContainerStatusReqHandler) {
	// only process init phase;
	// make sure no other already create mapping during sync blocking time range.
	handler, ok := p.handlers.LoadOrStore(jobInstanceId, reqHandler)
	if !ok {
		if statusReqHandler, ok := handler.(*ContainerStatusReqHandler); ok {
			statusReqHandler.Start(statusReqHandler)
		}
	}
}

func (p *ContainerStatusReqHandlerPool) Stop(jobInstanceId int64) {
	handler, ok := p.handlers.LoadAndDelete(jobInstanceId)
	if ok {
		handler.(*ContainerStatusReqHandler).Stop()
		handler = nil
	}
}

func (p *ContainerStatusReqHandlerPool) Contains(jobInstanceId int64) bool {
	_, ok := p.handlers.Load(jobInstanceId)
	return ok
}

func (p *ContainerStatusReqHandlerPool) SubmitReq(jobInstanceId int64, req *schedulerx.ContainerReportTaskStatusRequest) bool {
	success := false
	handler, ok := p.handlers.Load(jobInstanceId)
	if ok {
		success = true
		handler.(*ContainerStatusReqHandler).SubmitRequest(req)
	}
	return success
}

func (p *ContainerStatusReqHandlerPool) GetHandlers() *sync.Map {
	return p.handlers
}
