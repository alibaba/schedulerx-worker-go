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

import (
	"container/heap"
	"sync"
)

type PriorityQueue struct {
	items []ComparatorItem
	mu    sync.RWMutex
}

type ComparatorItem interface {
	Priority() int64
	Value() interface{}
}

func NewPriorityQueue(initialCapacity int) *PriorityQueue {
	return &PriorityQueue{
		items: make([]ComparatorItem, 0, initialCapacity),
	}
}

func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.items[i].Priority() < pq.items[j].Priority()
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	item := x.(ComparatorItem)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	n := len(pq.items)
	item := pq.items[n-1]
	pq.items = pq.items[0 : n-1]
	return item
}

func (pq *PriorityQueue) PushItem(item ComparatorItem) {
	heap.Push(pq, item)
}

func (pq *PriorityQueue) PopItem() ComparatorItem {
	return heap.Pop(pq).(ComparatorItem)
}

func (pq *PriorityQueue) Peek() ComparatorItem {
	var ret ComparatorItem
	if item := heap.Pop(pq); item != nil {
		ret = item.(ComparatorItem)
		heap.Push(pq, item)
	}
	return ret
}

func (pq *PriorityQueue) Clear() {
	for pq.Len() > 0 {
		pq.PopItem()
	}
}
