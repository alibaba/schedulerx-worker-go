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
}

func NewPriorityQueue(initialCapacity int) *PriorityQueue {
	return &PriorityQueue{
		items: make([]ComparatorItem, 0, initialCapacity),
	}
}

func (pq *PriorityQueue) Len() int {
	// Called by heap package, don't add lock
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	// Called by heap package, don't add lock
	return pq.items[i].Priority() < pq.items[j].Priority()
}

func (pq *PriorityQueue) Swap(i, j int) {
	// Called by heap package, don't add lock
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	// Called by heap package, don't add lock
	item := x.(ComparatorItem)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	// Called by heap package, don't add lock
	n := len(pq.items)
	item := pq.items[n-1]
	pq.items = pq.items[0 : n-1]
	return item
}

func (pq *PriorityQueue) PushItem(item ComparatorItem) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq, item)
}

func (pq *PriorityQueue) PopItem() ComparatorItem {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return heap.Pop(pq).(ComparatorItem)
}

func (pq *PriorityQueue) Peek() ComparatorItem {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if len(pq.items) > 0 {
		return pq.items[0].(ComparatorItem)
	}
	return nil
}

func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.items = make([]ComparatorItem, 0)
}

// RemoveBy removes all items that satisfy the predicate function
// Returns the number of removed items
func (pq *PriorityQueue) RemoveBy(predicate func(ComparatorItem) bool) int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	originalLen := len(pq.items)
	filtered := make([]ComparatorItem, 0, originalLen)

	// Iterate through all elements, keep those that don't match the removal condition
	for _, item := range pq.items {
		if !predicate(item) {
			filtered = append(filtered, item)
		}
	}

	// Rebuild the heap
	pq.items = filtered
	heap.Init(pq)

	return originalLen - len(filtered)
}
