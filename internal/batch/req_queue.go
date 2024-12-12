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

type ReqQueue struct {
	maxSize  int32
	requests chan any
}

func NewReqQueue(maxSize int32) (q *ReqQueue) {
	return &ReqQueue{
		maxSize:  maxSize,
		requests: make(chan any, maxSize),
	}
}

func (q *ReqQueue) SubmitRequest(req any) {
	if req == nil {
		return
	}
	q.requests <- req
}

func (q *ReqQueue) RetrieveRequests(batchSize int32) []any {
	requests := make([]any, 0, batchSize)
	for i := int32(0); i < batchSize; i++ {
		req := q.pop()
		if req == nil {
			break
		}
		requests = append(requests, req)
	}
	return requests
}

func (q *ReqQueue) pop() any {
	select {
	case req := <-q.requests:
		return req
	default:
		return nil
	}
}

func (q *ReqQueue) Size() int {
	return len(q.requests)
}

func (q *ReqQueue) Clear() {
	q.requests = nil
}
