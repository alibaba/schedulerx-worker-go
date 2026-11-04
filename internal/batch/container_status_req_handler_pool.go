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
	handlers sync.Map // Map<Long, ContainerStatusReqHandler<ContainerReportTaskStatusRequest>>
}

func NewContainerStatusReqHandlerPool() *ContainerStatusReqHandlerPool {
	return &ContainerStatusReqHandlerPool{
		handlers: sync.Map{},
	}
}

func (p *ContainerStatusReqHandlerPool) Start(jobInstanceId int64, reqHandler *ContainerStatusReqHandler) {
	// only process init phase;
	// make sure no other already create mapping during sync blocking time range.
	_, loaded := p.handlers.LoadOrStore(jobInstanceId, reqHandler)
	if !loaded {
		reqHandler.Start(reqHandler)
	}
}

func (p *ContainerStatusReqHandlerPool) Stop(jobInstanceId int64) {
	handler, ok := p.handlers.LoadAndDelete(jobInstanceId)
	if ok {
		handler.(*ContainerStatusReqHandler).Stop()
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
	return &p.handlers
}

// Get returns the current generation's handler. Containers bind this identity
// before execution instead of looking up a possibly newer handler at completion.
func (p *ContainerStatusReqHandlerPool) Get(jobInstanceID int64) *ContainerStatusReqHandler {
	handler, ok := p.handlers.Load(jobInstanceID)
	if !ok {
		return nil
	}
	return handler.(*ContainerStatusReqHandler)
}

func (p *ContainerStatusReqHandlerPool) SubmitReqTo(jobInstanceID int64, expected *ContainerStatusReqHandler, req *schedulerx.ContainerReportTaskStatusRequest) bool {
	if expected == nil || p.Get(jobInstanceID) != expected {
		return false
	}
	expected.stopChLock.Lock()
	stopped := expected.stopBatchRetrieveCh
	expected.stopChLock.Unlock()
	if stopped == nil {
		return false
	}
	// Stop may race with this send. Never strand a submitter behind a stopped
	// retriever. Any racing enqueue remains in the old bounded queue, not the new one.
	select {
	case <-stopped:
		return false
	case expected.reqsQueue.requests <- req:
		expected.latestRequestLock.Lock()
		expected.latestRequest = req
		expected.latestRequestLock.Unlock()
		return true
	}
}
