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

	"github.com/alibaba/schedulerx-worker-go/logger"
)

type ReqQueue struct {
	capacity int32
	requests []interface{}
	lock     sync.RWMutex
}

func NewReqQueue(capacity int32) (q *ReqQueue) {
	return &ReqQueue{
		capacity: capacity,
		requests: make([]interface{}, 0, capacity),
	}
}

func (q *ReqQueue) SubmitRequest(request interface{}) {
	q.push(request)
}

func (q *ReqQueue) RetrieveRequests(batchSize int32) []interface{} {
	res := make([]interface{}, 0, batchSize)
	for i := int32(0); i < batchSize; i++ {
		request := q.pop()
		if request == nil {
			// empty, just break
			break
		}
		res = append(res, request)
	}
	return res
}

func (q *ReqQueue) push(req interface{}) {
	if req != nil {
		if q.capacity > 0 && int32(q.Size()) == q.capacity {
			logger.Warnf("req queue is full, capacity: %d", q.capacity)
			return
		}
		q.lock.Lock()
		q.requests = append(q.requests, req)
		q.lock.Unlock()
	}
}

func (q *ReqQueue) pop() interface{} {
	if q.Size() == 0 {
		return nil
	}
	q.lock.Lock()
	defer q.lock.Unlock()
	req := q.requests[0]
	q.requests = q.requests[1:]
	return req
}

func (q *ReqQueue) SetCapacity(capacity int32) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.capacity = capacity
	if int32(q.Size()) > q.capacity {
		q.requests = q.requests[:q.capacity]
	}
}

func (q *ReqQueue) Size() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.requests)
}

func (q *ReqQueue) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.requests = nil
}
