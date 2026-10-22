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

package container

import (
	"bytes"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

func TestLifecycleGoexitDoesNotStrandConsumers(t *testing.T) {
	p := newTreadContainerPool()
	defer p.DestroyByInstance(9001)
	exited := make(chan struct{})
	c := &testContainer{start: func() { defer close(exited); runtime.Goexit() }}
	if err := p.Submit(1, 9001, 1, c, 1); err != nil {
		t.Fatal(err)
	}
	<-exited
	started := make(chan int64, 1)
	submitContainer(t, p, 9001, 2, 1, func() { started <- 2 })
	select {
	case <-started:
	case <-time.After(300 * time.Millisecond):
		p.executionMu.Lock()
		workers, queued := p.executors[9001].workers, p.executors[9001].queue.Len()
		p.executionMu.Unlock()
		t.Fatalf("runner exited but workers=%d, queued=%d; no replacement starts", workers, queued)
	}
}

type retainedContainer struct{ payload []byte }

func (c *retainedContainer) Start() { runtime.KeepAlive(c.payload) }
func (c *retainedContainer) Kill()  {}

func enqueueAndCancelPayloads(t *testing.T, p *ThreadContainerPool, done chan<- struct{}) {
	t.Helper()
	for id := int64(2); id <= 17; id++ {
		c := &retainedContainer{payload: make([]byte, 1<<20)}
		runtime.SetFinalizer(c, func(*retainedContainer) { done <- struct{}{} })
		uid := utils.GetUniqueId(1, 9002, id)
		p.Put(uid, c)
		if err := p.Submit(1, 9002, id, c, 1); err != nil {
			t.Fatal(err)
		}
		p.Remove(uid, c)
	}
}
func TestLifecycleCanceledQueueReleasesPayload(t *testing.T) {
	p := newTreadContainerPool()
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	defer p.DestroyByInstance(9002)
	submitContainer(t, p, 9002, 1, 1, func() { close(started); <-release })
	<-started
	collected := make(chan struct{}, 16)
	enqueueAndCancelPayloads(t, p, collected)
	waitFinalizers(t, collected, 16)

}

func TestLifecycleDuplicateQueuedKillDoesNotStartOldCopy(t *testing.T) {
	p := newTreadContainerPool()
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	defer p.DestroyByInstance(9003)
	submitContainer(t, p, 9003, 1, 1, func() { close(started); <-release })
	<-started
	ran := make(chan int64, 2)
	submitContainer(t, p, 9003, 2, 1, func() { ran <- 1 })
	submitContainer(t, p, 9003, 2, 1, func() { ran <- 2 })
	uid := utils.GetUniqueId(1, 9003, 2)
	p.Remove(uid, p.Get(uid))
	once.Do(func() { close(release) })
	select {
	case copy := <-ran:
		t.Fatalf("killed task still executed queued copy %d", copy)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestLifecycleHealthyCyclesLeaveNoRunnerGoroutines(t *testing.T) {
	p := newTreadContainerPool()
	for id := int64(1); id <= 100; id++ {
		var done sync.WaitGroup
		done.Add(20)
		for task := int64(1); task <= 20; task++ {
			c := &testContainer{start: func() { done.Done() }}
			if err := p.Submit(1, id, task, c, 3); err != nil {
				t.Fatal(err)
			}
		}
		done.Wait()
		p.DestroyByInstance(id)
	}
	deadline := time.Now().Add(time.Second)
	for {
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		runners := strings.Count(stacks.String(), "(*ThreadContainerPool).run(")
		if runners == 0 {
			t.Log("100 cycles / 2000 tasks: zero live execution runners after cleanup")
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d execution runners remain after cleanup", runners)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLifecycleDestroyReleasesCanceledPayloads(t *testing.T) {
	p := newTreadContainerPool()
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	defer p.DestroyByInstance(9002)
	submitContainer(t, p, 9002, 1, 1, func() { close(started); <-release })
	<-started
	collected := make(chan struct{}, 16)
	enqueueAndCancelPayloads(t, p, collected)
	p.DestroyByInstance(9002)
	waitFinalizers(t, collected, 16)

	t.Log("16/16 canceled 1MiB payloads reclaimed after Destroy; retention is in queue before destruction")
}

func waitFinalizers(t *testing.T, collected <-chan struct{}, count int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for n := 0; n < count; {
		runtime.GC()
		select {
		case <-collected:
			n++
		case <-time.After(time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d canceled payloads reclaimed", n, count)
		}
	}
}

func TestLifecycleIdlePoolReleasesExecutionMetadataWithoutDestroy(t *testing.T) {
	p := newTreadContainerPool()
	var completed sync.WaitGroup
	completed.Add(1000)
	for id := int64(1); id <= 1000; id++ {
		c := &testContainer{start: func() { completed.Done() }}
		if err := p.Submit(1, id, 1, c, 1); err != nil {
			t.Fatal(err)
		}
	}
	completed.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.executionMu.Lock()
		empty := len(p.items) == 0 && len(p.executors) == 0
		p.executionMu.Unlock()
		if empty {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("idle pool kept execution state without a destroy notification")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLifecycleOldCompletionCannotReportIntoNewCycle(t *testing.T) {
	handlers := batch.GetContainerStatusReqHandlerPool()
	const instance = int64(9123)
	old := batch.NewContainerStatusReqHandler(instance, 1, 1, 10, batch.NewReqQueue(10), "old")
	current := batch.NewContainerStatusReqHandler(instance, 1, 1, 10, batch.NewReqQueue(10), "new")
	handlers.Start(instance, old)
	handlers.Stop(instance)
	handlers.Start(instance, current)
	defer handlers.Stop(instance)
	ctx := new(jobcontext.JobContext)
	ctx.SetJobId(1)
	ctx.SetJobInstanceId(instance)
	ctx.SetTaskId(1)
	ctx.SetSerialNum(7)
	c := &ThreadContainer{jobCtx: ctx, statusHandler: old}
	// Exercise the actual reporting path, including its direct-send fallback.
	// No actor context is needed: a retired generation must drop its output.
	c.reportTaskStatus(processor.NewProcessResult(processor.WithSucceed()), "worker")
	if current.GetLatestRequest() != nil {
		t.Fatal("old completion entered the new cycle's status batch")
	}
}
