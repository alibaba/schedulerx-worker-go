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

package utils

import "github.com/alibaba/schedulerx-worker-go/internal/common"

type LimitedQueue struct {
	limit int
	queue []*common.ProgressHistory
}

func NewLimitedQueue(limit int) *LimitedQueue {
	return &LimitedQueue{
		limit: limit,
		queue: []*common.ProgressHistory{},
	}
}

func (lq *LimitedQueue) Enqueue(item *common.ProgressHistory) {
	if len(lq.queue) == lq.limit {
		lq.queue = lq.queue[1:]
	}
	lq.queue = append(lq.queue, item)
}

func (lq *LimitedQueue) Dequeue() *common.ProgressHistory {
	if len(lq.queue) == 0 {
		return nil
	}
	item := lq.queue[0]
	lq.queue = lq.queue[1:]
	return item
}

func (lq *LimitedQueue) Convert2Slice() []*common.ProgressHistory {
	var ret []*common.ProgressHistory
	for {
		item := lq.Dequeue()
		if item == nil {
			break
		}
		ret = append(ret, item)
	}
	return ret
}
