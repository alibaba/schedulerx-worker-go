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
	"sync"
	"testing"
)

// TestItem is a test implementation of ComparatorItem
type TestItem struct {
	priority int64
	value    string
}

func (t *TestItem) Priority() int64 {
	return t.priority
}

func TestNewPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue(10)
	if pq == nil {
		t.Error("NewPriorityQueue should not return nil")
	}
	if pq.Len() != 0 {
		t.Errorf("New queue should be empty, got length %d", pq.Len())
	}
}

func TestPriorityQueue_PushAndPop(t *testing.T) {
	pq := NewPriorityQueue(10)

	item1 := &TestItem{priority: 3, value: "item3"}
	item2 := &TestItem{priority: 1, value: "item1"}
	item3 := &TestItem{priority: 2, value: "item2"}

	pq.PushItem(item1)
	pq.PushItem(item2)
	pq.PushItem(item3)

	if pq.Len() != 3 {
		t.Errorf("Expected length 3, got %d", pq.Len())
	}

	// Should pop in priority order (lowest first)
	popped := pq.PopItem()
	if popped.(*TestItem).priority != 1 {
		t.Errorf("Expected priority 1, got %d", popped.(*TestItem).priority)
	}

	popped = pq.PopItem()
	if popped.(*TestItem).priority != 2 {
		t.Errorf("Expected priority 2, got %d", popped.(*TestItem).priority)
	}

	popped = pq.PopItem()
	if popped.(*TestItem).priority != 3 {
		t.Errorf("Expected priority 3, got %d", popped.(*TestItem).priority)
	}

	if pq.Len() != 0 {
		t.Errorf("Queue should be empty after popping all items, got length %d", pq.Len())
	}
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue(10)

	item1 := &TestItem{priority: 5, value: "item5"}
	item2 := &TestItem{priority: 2, value: "item2"}
	item3 := &TestItem{priority: 8, value: "item8"}

	pq.PushItem(item1)
	pq.PushItem(item2)
	pq.PushItem(item3)

	// Peek should return the lowest priority item without removing it
	peeked := pq.Peek()
	if peeked == nil {
		t.Error("Peek should not return nil for non-empty queue")
	}
	if peeked.(*TestItem).priority != 2 {
		t.Errorf("Expected priority 2, got %d", peeked.(*TestItem).priority)
	}

	// Queue length should remain the same after peek
	if pq.Len() != 3 {
		t.Errorf("Queue length should remain 3 after peek, got %d", pq.Len())
	}

	// Peek again should return the same item
	peeked2 := pq.Peek()
	if peeked2.(*TestItem).priority != 2 {
		t.Errorf("Second peek should return same priority 2, got %d", peeked2.(*TestItem).priority)
	}
}

func TestPriorityQueue_PeekEmpty(t *testing.T) {
	pq := NewPriorityQueue(10)

	peeked := pq.Peek()
	if peeked != nil {
		t.Error("Peek on empty queue should return nil")
	}
}

func TestPriorityQueue_Clear(t *testing.T) {
	pq := NewPriorityQueue(10)

	pq.PushItem(&TestItem{priority: 1, value: "item1"})
	pq.PushItem(&TestItem{priority: 2, value: "item2"})
	pq.PushItem(&TestItem{priority: 3, value: "item3"})

	if pq.Len() != 3 {
		t.Errorf("Expected length 3 before clear, got %d", pq.Len())
	}

	pq.Clear()

	if pq.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", pq.Len())
	}

	peeked := pq.Peek()
	if peeked != nil {
		t.Error("Peek on cleared queue should return nil")
	}
}

func TestPriorityQueue_PopItem(t *testing.T) {
	pq := NewPriorityQueue(10)

	item1 := &TestItem{priority: 1, value: "item1"}
	item2 := &TestItem{priority: 2, value: "item2"}
	item3 := &TestItem{priority: 3, value: "item3"}

	pq.PushItem(item1)
	pq.PushItem(item2)
	pq.PushItem(item3)

	popped := pq.PopItem()
	if popped != item1 {
		t.Error("pop item should be item1")
	}

	popped = pq.PopItem()
	if popped != item2 {
		t.Error("pop item should be item2")
	}

	popped = pq.PopItem()
	if popped != item3 {
		t.Error("pop item should be item3")
	}
}

func TestPriorityQueue_RemoveBy(t *testing.T) {
	pq := NewPriorityQueue(10)

	pq.PushItem(&TestItem{priority: 1, value: "remove"})
	pq.PushItem(&TestItem{priority: 2, value: "keep"})
	pq.PushItem(&TestItem{priority: 3, value: "remove"})
	pq.PushItem(&TestItem{priority: 4, value: "keep"})
	pq.PushItem(&TestItem{priority: 5, value: "remove"})

	// Remove all items with value "remove"
	count := pq.RemoveBy(func(item ComparatorItem) bool {
		return item.(*TestItem).value == "remove"
	})

	if count != 3 {
		t.Errorf("Expected to remove 3 items, got %d", count)
	}

	if pq.Len() != 2 {
		t.Errorf("Expected length 2 after RemoveBy, got %d", pq.Len())
	}

	// Verify remaining items
	item1 := pq.PopItem().(*TestItem)
	if item1.value != "keep" || item1.priority != 2 {
		t.Errorf("First item should be priority 2 with value 'keep', got priority %d value %s", item1.priority, item1.value)
	}

	item2 := pq.PopItem().(*TestItem)
	if item2.value != "keep" || item2.priority != 4 {
		t.Errorf("Second item should be priority 4 with value 'keep', got priority %d value %s", item2.priority, item2.value)
	}
}

func TestPriorityQueue_RemoveByNoMatch(t *testing.T) {
	pq := NewPriorityQueue(10)

	pq.PushItem(&TestItem{priority: 1, value: "item1"})
	pq.PushItem(&TestItem{priority: 2, value: "item2"})

	count := pq.RemoveBy(func(item ComparatorItem) bool {
		return item.(*TestItem).value == "nonexistent"
	})

	if count != 0 {
		t.Errorf("Expected to remove 0 items, got %d", count)
	}

	if pq.Len() != 2 {
		t.Errorf("Expected length 2 after RemoveBy with no match, got %d", pq.Len())
	}
}

func TestPriorityQueue_Concurrent(t *testing.T) {
	pq := NewPriorityQueue(100)
	var wg sync.WaitGroup

	// Concurrent pushes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(priority int64) {
			defer wg.Done()
			pq.PushItem(&TestItem{priority: priority, value: "concurrent"})
		}(int64(i))
	}

	wg.Wait()

	if pq.Len() != 50 {
		t.Errorf("Expected length 50 after concurrent pushes, got %d", pq.Len())
	}

	// Concurrent pops
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pq.PopItem()
		}()
	}

	wg.Wait()

	if pq.Len() != 25 {
		t.Errorf("Expected length 25 after concurrent pops, got %d", pq.Len())
	}
}
