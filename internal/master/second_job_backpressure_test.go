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
	"sync"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	pa "github.com/asynkron/protoactor-go/actor"
)

// This wrapper observes the production snapshot call without replacing its
// behavior or the reporter/health/TaskMaster implementations under test.
type observedProgressMaster struct {
	*TaskMaster
	progress chan struct{}
	once     sync.Once
}

func (m *observedProgressMaster) GetJobInstanceProgress() (string, error) {
	value, err := m.TaskMaster.GetJobInstanceProgress()
	m.once.Do(func() { close(m.progress) })
	return value, err
}

// The real reporter must still execute need2KillSelf when progress cannot enter
// the shared actor queue. Removing the final live worker is a health failure.
func TestProgressBackpressureDoesNotStarveHealthCheck(t *testing.T) {
	const child = "SCHEDULERX_PROGRESS_BACKPRESSURE"
	if os.Getenv(child) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProgressBackpressureDoesNotStarveHealthCheck$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), child+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("reporter child: %v\n%s", err, out)
		}
		return
	}
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	queue := actorcomm.TaskMasterMsgReceiver()
	for len(queue) < cap(queue) {
		queue <- "occupied"
	}
	info := new(common.JobInstanceInfo)
	info.SetJobId(123)
	info.SetJobInstanceId(123)
	system := pa.NewActorSystem()
	defer system.Shutdown()
	ctx := scannerBackpressureContext{system: system}
	h := NewSecondJobUpdateInstanceStatusHandler(ctx, nil, info)
	m := NewTaskMaster(ctx, info, h)
	observed := &observedProgressMaster{TaskMaster: m, progress: make(chan struct{})}
	h.taskMaster = observed
	m.inited = true
	m.aliveCheckWorkerSet.Add("healthy-worker")
	utils.GetHealthTimeHolder().ResetServerHeartbeatTime()
	done := make(chan struct{})
	go func() { defer close(done); h.reportJobInstanceProgress() }()
	// Wait for the actual snapshot attempt rather than assuming eleven ticks
	// have run on a busy CI host. The shared actor queue remains full.
	select {
	case <-observed.progress:
	case <-time.After(13 * time.Second):
		t.Fatal("reporter did not attempt its real progress snapshot")
	}
	m.aliveCheckWorkerSet.Remove("healthy-worker")
	select {
	case <-done:
		if !m.IsKilled() {
			t.Fatal("reporter exited without applying the worker-loss health check")
		}
	case <-time.After(2 * time.Second):
		h.stop()
		<-queue // unblock the old implementation so teardown never leaks its send.
		<-done
		t.Fatal("full progress queue prevented the reporter from checking worker loss")
	}
}

func TestPendingProgressIsDeliveredOrReleasedOnStop(t *testing.T) {
	const child = "SCHEDULERX_PENDING_PROGRESS"
	if mode := os.Getenv(child); mode == "" {
		for _, mode := range []string{"deliver", "stop"} {
			t.Run(mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestPendingProgressIsDeliveredOrReleasedOnStop$", "-test.timeout=10s")
				cmd.Env = append(os.Environ(), child+"="+mode)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("pending progress child: %v\n%s", err, out)
				}
			})
		}
		return
	}
	system := pa.NewActorSystem()
	defer system.Shutdown()
	ctx := scannerBackpressureContext{system: system}
	info := new(common.JobInstanceInfo)
	info.SetJobId(124)
	info.SetJobInstanceId(124)
	h := NewSecondJobUpdateInstanceStatusHandler(ctx, nil, info)
	m := NewTaskMaster(ctx, info, h)
	h.taskMaster = m
	queue := actorcomm.TaskMasterMsgReceiver()
	for len(queue) < cap(queue) {
		queue <- "occupied"
	}
	msg := &actorcomm.SchedulerWrappedMsg{Msg: "pending snapshot"}
	done := make(chan bool, 1)
	go func() { done <- h.sendProgressWhileCheckingHealth(m.GetSerialNum(), msg, make(chan time.Time)) }()
	if os.Getenv(child) == "stop" {
		h.stop()
		select {
		case sent := <-done:
			if sent {
				t.Fatal("stopped report was admitted into a full queue")
			}
		case <-time.After(time.Second):
			t.Fatal("stop did not release blocked progress report")
		}
		if len(queue) != cap(queue) {
			t.Fatal("stopping altered unrelated queued messages")
		}
		return
	}
	<-queue
	select {
	case sent := <-done:
		if !sent {
			t.Fatal("live progress report was abandoned")
		}
	case <-time.After(time.Second):
		t.Fatal("freed capacity did not admit pending progress")
	}
	found := 0
	for len(queue) > 0 {
		if wrapped, ok := (<-queue).(*actorcomm.SchedulerWrappedMsg); ok && wrapped.Msg == msg.Msg {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("delivered pending snapshot %d times, want exactly once", found)
	}
}
