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
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	pa "github.com/asynkron/protoactor-go/actor"
)

type panicBroadcastProcessor struct{}

func (panicBroadcastProcessor) PreProcess(*jobcontext.JobContext) error {
	panic("dispatch-fail-fast-sentinel")
}
func (panicBroadcastProcessor) Process(*jobcontext.JobContext) (*processor.ProcessResult, error) {
	return nil, nil
}
func (panicBroadcastProcessor) PostProcess(*jobcontext.JobContext) (*processor.ProcessResult, error) {
	return nil, nil
}

func TestSecondJobDispatchPanicKeepsFailFastSemantics(t *testing.T) {
	const child = "SCHEDULERX_DISPATCH_PANIC_TEST"
	if os.Getenv(child) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSecondJobDispatchPanicKeepsFailFastSemantics$", "-test.timeout=5s")
		cmd.Env = append(os.Environ(), child+"=1")
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("dispatch swallowed a lifecycle panic and left the claimed cycle unfinished: %s", out)
		}
		if !strings.Contains(string(out), "panic: dispatch-fail-fast-sentinel") {
			t.Fatalf("child failed without the expected lifecycle panic: %v\n%s", err, out)
		}
		return
	}
	system := pa.NewActorSystem()
	defer system.Shutdown()
	info := new(common.JobInstanceInfo)
	info.SetJobId(71)
	info.SetJobInstanceId(71)
	info.SetJobType("golang")
	info.SetContent(`{"jobName":"dispatch-panic"}`)
	registry := tasks.GetTaskMap()
	registry.Register("dispatch-panic", panicBroadcastProcessor{})
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(registry))
	m := &BroadcastTaskMaster{}
	ctx := scannerBackpressureContext{system: system}
	h := NewSecondJobUpdateInstanceStatusHandler(ctx, m, info)
	m.TaskMaster = NewTaskMaster(ctx, info, h)
	masterpool.GetTaskMasterPool().Put(71, m)
	scheduler := newTimeScheduler()
	scheduler.dispatchPlan(NewTimePlanEntry(71, 1, h))
	waitDispatchIdle(t, scheduler, h)
	// Only the real panic may end this child successfully for the parent. If a
	// future recover swallows it, the test timeout has a different panic message.
	select {}
}

// Submit is the dispatcher's lifecycle boundary. Slow real actor replies are
// covered separately; this double supplies a user hook that terminates its
// goroutine, which a remote actor cannot do to the caller.
type dispatchHookMaster struct {
	*TaskMaster
	submit func()
}

func (m *dispatchHookMaster) SubmitInstance(*common.JobInstanceInfo) error { m.submit(); return nil }

func waitDispatchIdle(t *testing.T, scheduler *TimeScheduler, owner *SecondJobUpdateInstanceStatusHandler) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		scheduler.dispatchMu.Lock()
		_, running := scheduler.dispatches[owner]
		scheduler.dispatchMu.Unlock()
		if !running {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("dispatch runner did not relinquish owner after its continuation finished")
}

func TestSecondJobDispatchReleasesOwnerAfterHookExit(t *testing.T) {
	for _, exit := range []string{"goexit", "killed"} {
		t.Run(exit, func(t *testing.T) {
			system := pa.NewActorSystem()
			defer system.Shutdown()
			ctx := scannerBackpressureContext{system: system}
			info := new(common.JobInstanceInfo)
			info.SetJobId(7)
			info.SetJobInstanceId(7)
			m := &dispatchHookMaster{}
			h := NewSecondJobUpdateInstanceStatusHandler(ctx, m, info)
			m.TaskMaster = NewTaskMaster(ctx, info, h)
			entered, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			m.submit = func() {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
					switch exit {
					case "goexit":
						runtime.Goexit()
					}
					return
				}
				close(second)
			}
			scheduler := newTimeScheduler()
			scheduler.dispatchPlan(NewTimePlanEntry(7, 1, h))
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first dispatch never entered hook")
			}
			scheduler.dispatchPlan(NewTimePlanEntry(7, 2, h))
			if exit == "killed" {
				if err := m.TaskMaster.KillInstance("killed from test"); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			if exit != "killed" {
				select {
				case <-second:
				case <-time.After(time.Second):
					t.Fatal("dead runner retained its pending plan")
				}
			}
			waitDispatchIdle(t, scheduler, h)
			want := int32(2)
			if exit == "killed" {
				want = 1
			}
			if got := calls.Load(); got != want {
				t.Fatalf("Submit calls=%d, want %d", got, want)
			}
		})
	}
}
