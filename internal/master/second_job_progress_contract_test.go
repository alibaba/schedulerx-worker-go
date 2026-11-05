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
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	pa "github.com/asynkron/protoactor-go/actor"
)

// This thin adapter supplies the optional lifecycle/origin contract introduced
// by lifecycle-aware SDK versions. It keeps the real TaskMaster serial/state;
// production Base/Map integration is exercised in the combined SDK regressions.
type progressContractMaster struct {
	*TaskMaster
	gate    sync.Mutex
	retired bool
	entered chan struct{}
	once    sync.Once
}

func (m *progressContractMaster) withCycleControl(serial int64, send func()) bool {
	m.gate.Lock()
	defer m.gate.Unlock()
	if m.retired || serial != m.GetSerialNum() {
		return false
	}
	m.once.Do(func() { close(m.entered) })
	send()
	return true
}

type progressOriginContextKey struct{}

func (m *progressContractMaster) withMessageOrigin(message interface{}) interface{} {
	wrapped := message.(*actorcomm.SchedulerWrappedMsg)
	bound := *wrapped
	ctx := bound.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// The public baseline predates GroupId/Reply wrapper fields. A context
	// decoration verifies consumption of the optional binder's returned wrapper
	// without adding those newer transport fields to this independent PR.
	bound.Ctx = context.WithValue(ctx, progressOriginContextKey{}, m.GetJobInstanceInfo().GetGroupId())
	return &bound
}

func TestProgressHonorsOptionalLifecycleContract(t *testing.T) {
	const child = "SCHEDULERX_PROGRESS_CONTRACT"
	mode := os.Getenv(child)
	if mode == "" {
		for _, mode := range []string{"deny", "origin", "retire", "serial"} {
			t.Run(mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestProgressHonorsOptionalLifecycleContract$", "-test.timeout=8s")
				cmd.Env = append(os.Environ(), child+"="+mode)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("progress contract child: %v\n%s", err, out)
				}
			})
		}
		return
	}
	system := pa.NewActorSystem()
	defer system.Shutdown()
	ctx := scannerBackpressureContext{system: system}
	info := new(common.JobInstanceInfo)
	info.SetJobId(125)
	info.SetJobInstanceId(125)
	info.SetGroupId("contract-origin")
	m := &progressContractMaster{entered: make(chan struct{})}
	h := NewSecondJobUpdateInstanceStatusHandler(ctx, m, info)
	m.TaskMaster = NewTaskMaster(ctx, info, h)
	defer h.stop()
	queue := actorcomm.TaskMasterMsgReceiver()
	msg := &actorcomm.SchedulerWrappedMsg{Msg: "snapshot"}
	if mode == "deny" {
		m.retired = true
		if h.sendProgressWhileCheckingHealth(0, msg, nil) || len(queue) != 0 {
			t.Fatal("retired lifecycle contract did not prevent progress enqueue")
		}
		return
	}
	if mode == "origin" {
		requestContext := context.WithValue(context.Background(), struct{}{}, "request-context")
		msg.Ctx, msg.SenderPath = requestContext, "request-sender"
		if !h.sendProgressWhileCheckingHealth(0, msg, nil) {
			t.Fatal("current owner snapshot was rejected")
		}
		got := (<-queue).(*actorcomm.SchedulerWrappedMsg)
		if got.Ctx == nil || got.Ctx.Value(progressOriginContextKey{}) != "contract-origin" || got.Msg != "snapshot" || got.SenderPath != "request-sender" || msg.Ctx != requestContext || requestContext.Value(progressOriginContextKey{}) != nil {
			t.Fatalf("origin contract result was lost or caller changed: got=%#v caller=%#v", got, msg)
		}
		return
	}
	for len(queue) < cap(queue) {
		queue <- "unrelated"
	}
	done := make(chan bool, 1)
	go func() { done <- h.sendProgressWhileCheckingHealth(0, msg, nil) }()
	select {
	case <-m.entered:
	case <-time.After(time.Second):
		t.Fatal("pending progress ignored its optional lifecycle contract")
	}
	transition := make(chan struct{})
	go func() {
		m.gate.Lock()
		if mode == "retire" {
			m.retired = true
		} else {
			m.AcquireSerialNum()
		}
		m.gate.Unlock()
		close(transition)
	}()
	select {
	case <-transition:
	case <-time.After(time.Second):
		t.Fatal("progress capacity wait pinned the lifecycle gate")
	}
	select {
	case sent := <-done:
		if sent {
			t.Fatal("old pending snapshot crossed the lifecycle transition")
		}
	case <-time.After(time.Second):
		t.Fatal("old pending snapshot was not released after transition")
	}
	if len(queue) != cap(queue) {
		t.Fatal("lifecycle transition changed unrelated queued messages")
	}
}

// Exercise the actual Map gate now present on develop, in addition to the
// optional-contract adapter. No persistence, actors, or periodic loops are
// replaced: this test drives the real nonblocking progress handoff directly.
func TestProgressBackpressureHonorsRealMapLifecycle(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	queue := actorcomm.TaskMasterMsgReceiver()
	for _, mode := range []string{"serial", "retire", "idle"} {
		t.Run(mode, func(t *testing.T) {
			drainR4(queue)
			t.Cleanup(func() { drainR4(queue) })
			base, _ := attemptTestMaster(0)
			master := &MapTaskMaster{TaskMaster: base, retireCh: make(chan struct{})}
			h := NewSecondJobUpdateInstanceStatusHandler(nil, master, base.jobInstanceInfo)
			base.statusHandler = h
			t.Cleanup(h.stop)
			if mode == "idle" {
				master.statusWriteMu.Lock()
				master.cycleClosed = true
				master.statusWriteMu.Unlock()
				msg := &actorcomm.SchedulerWrappedMsg{Msg: "idle snapshot"}
				if !h.sendProgressWhileCheckingHealth(0, msg, nil) || <-queue != msg {
					t.Fatal("current-owner idle progress must still be admitted")
				}
				return
			}
			for len(queue) < cap(queue) {
				queue <- "unrelated"
			}
			done := make(chan bool, 1)
			completed := make(chan struct{})
			go func() {
				defer close(completed)
				done <- h.sendProgressWhileCheckingHealth(0, &actorcomm.SchedulerWrappedMsg{Msg: "old snapshot"}, nil)
			}()
			t.Cleanup(func() {
				h.stop()
				select {
				case <-completed:
				case <-time.After(time.Second):
					t.Error("stopped progress sender did not exit")
				}
			})
			r4WaitStack(t, "sendProgressWhileCheckingHealth")
			transitioned := make(chan struct{})
			go func() {
				if mode == "retire" {
					master.RetireStatusWrites()
				} else {
					master.AcquireSerialNum()
				}
				close(transitioned)
			}()
			select {
			case <-transitioned:
			case <-time.After(time.Second):
				t.Fatal("progress capacity wait pinned the real lifecycle gate")
			}
			select {
			case sent := <-done:
				if sent {
					t.Fatal("old snapshot crossed the real lifecycle transition")
				}
			case <-time.After(time.Second):
				t.Fatal("old pending snapshot did not release after transition")
			}
			if len(queue) != cap(queue) {
				t.Fatal("rejection altered unrelated queued messages")
			}
		})
	}
}
