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

package master

import (
	"sync"
	"testing"
)

func TestTimeQueuePopAfterCancellationIsEmptySafe(t *testing.T) {
	q := NewTimeQueue()
	q.Add(NewTimePlanEntry(1, 10, nil))
	if q.Peek() == nil {
		t.Fatal("missing initial plan")
	}
	// Kill may remove the plan after the scanner observed it, before dequeue.
	q.Remove(1)
	if got := q.Pop(); got != nil {
		t.Fatalf("canceled plan returned: %v", got)
	}
}

func TestTimeQueueCanceledHeadDoesNotClaimFuturePlan(t *testing.T) {
	q := NewTimeQueue()
	q.Add(NewTimePlanEntry(1, 10, nil))
	future := NewTimePlanEntry(2, 100, nil)
	q.Add(future)
	if q.Peek().JobInstanceId() != 1 {
		t.Fatal("wrong initial head")
	}
	q.Remove(1)
	if got := q.PopDue(10); got != nil {
		t.Fatalf("future plan claimed early: %v", got)
	}
	if q.Size() != 1 {
		t.Fatal("future plan disappeared")
	}
	if got := q.PopDue(100); got != future {
		t.Fatalf("future plan unavailable at its deadline: %v", got)
	}
	if got := q.PopDue(100); got != nil {
		t.Fatalf("plan claimed twice: %v", got)
	}
}

func TestTimeQueueDueOrderAndBoundary(t *testing.T) {
	q := NewTimeQueue()
	q.Add(NewTimePlanEntry(3, 30, nil))
	q.Add(NewTimePlanEntry(1, 10, nil))
	q.Add(NewTimePlanEntry(2, 20, nil))
	if got := q.PopDue(9); got != nil {
		t.Fatal("plan claimed before deadline")
	}
	for _, want := range []int64{1, 2} {
		got := q.PopDue(20)
		if got == nil || got.JobInstanceId() != want {
			t.Fatalf("claim=%v, want job %d", got, want)
		}
	}
	if got := q.PopDue(20); got != nil {
		t.Fatal("future plan claimed")
	}
	if got := q.PopDue(30); got == nil || got.JobInstanceId() != 3 {
		t.Fatalf("last due plan=%v", got)
	}
	if !q.IsEmpty() || q.Size() != 0 || q.Pop() != nil {
		t.Fatal("queue not empty after final claim")
	}
}

func TestTimeQueueRemoveAndClearPermitRescheduling(t *testing.T) {
	q := NewTimeQueue()
	a := NewTimePlanEntry(1, 10, nil)
	q.Add(a)
	q.Add(NewTimePlanEntry(1, 20, nil))
	q.Add(NewTimePlanEntry(2, 30, nil))
	q.Remove(1)
	if q.Size() != 1 {
		t.Fatalf("Remove must remove all matching instance plans, size=%d", q.Size())
	}
	q.Add(a)
	if got := q.PopDue(10); got != a {
		t.Fatal("removed identity could not be scheduled again")
	}
	q.Clear()
	q.Add(a)
	q.Add(a)
	if q.Size() != 1 || q.PopDue(10) != a || !q.IsEmpty() {
		t.Fatal("Clear left queue and membership inconsistent")
	}
}

func TestTimeQueueConcurrentDuplicateAddIsClaimedOnce(t *testing.T) {
	for round := 0; round < 32; round++ {
		q := NewTimeQueue()
		p := NewTimePlanEntry(1, 10, nil)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; q.Add(p) }()
		}
		close(start)
		wg.Wait()
		if q.Size() != 1 {
			t.Fatalf("duplicate concurrent Add queued %d copies", q.Size())
		}
		if q.Pop() != p || !q.IsEmpty() {
			t.Fatal("duplicate plan remained after claim")
		}
	}
}

func TestTimeQueueConcurrentScanRemoveAndClear(t *testing.T) {
	q := NewTimeQueue()
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 128; i++ {
			q.Add(NewTimePlanEntry(1, int64(i), nil))
			q.Remove(1)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 128; i++ {
			q.IsEmpty()
			q.Size()
			q.Peek()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 128; i++ {
			q.Clear()
		}
	}()
	close(start)
	wg.Wait()
	q.Clear()
	p := NewTimePlanEntry(2, 1, nil)
	q.Add(p)
	if q.Pop() != p || !q.IsEmpty() {
		t.Fatal("queue unusable after concurrent cancellation")
	}
}
