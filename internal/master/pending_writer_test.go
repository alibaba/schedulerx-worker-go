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
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/asynkron/protoactor-go/actor"
	"runtime"
	"strings"
	"testing"
	"time"
)

func r4WaitStack(t *testing.T, signature string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		if strings.Contains(string(b[:n]), signature) {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("missing blocked stack %s", signature)
}

func TestClearCancelsBlockedSenderWithPendingStateWriter(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	target := make(chan interface{})
	sendDone := make(chan struct{})
	go func() { defer close(sendDone); m.sendCycleMessage(0, target, "blocked", false) }()
	r4WaitStack(t, "(*MapTaskMaster).sendCycleMessage")
	m.statusWriteMu.RLock() // Keep the state writer pending while Clear starts.
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); m.withCycleState(0, func() {}) }()
	r4WaitStack(t, "(*MapTaskMaster).withCycleState")
	// A queued writer now prevents further RLock admission. Clear cannot reach cancel.
	clearDone := make(chan struct{})
	go func() { defer close(clearDone); m.Clear(m) }()
	m.statusWriteMu.RUnlock()
	select {
	case <-clearDone:
	case <-time.After(200 * time.Millisecond):
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		t.Log(string(b[:n]))
		t.Error("Clear cannot cancel its blocked sender when a cycle-state writer is pending")
	}
	// External cancellation is solely test cleanup; production Clear should provide it.
	m.cycleCancel()
	waitRetirementTest(t, sendDone)
	waitRetirementTest(t, writerDone)
	waitRetirementTest(t, clearDone)
	m.Stop()
}

type r4CaptureContext struct {
	retirementActorContext
	sent chan interface{}
}

func (c r4CaptureContext) Send(_ *actor.PID, msg interface{}) { c.sent <- msg }
func TestRetiredKillTaskCannotSendToReplacement(t *testing.T) {
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	sent := make(chan interface{}, 2)
	old.actorCtx = r4CaptureContext{retirementActorContext{system: actor.NewActorSystem()}, sent}
	current := newRetirementGrid(t, 0)
	pool.Put(1001, current)
	defer pool.Remove(1001)
	old.KillTask("1_1001_7", "worker", "127.0.0.1:1234")
	select {
	case msg := <-sent:
		req := msg.(*schedulerx.MasterKillContainerRequest)
		t.Fatalf("retired owner sent KillTask after replacement: attempt=%d serial=%d task=%d", req.GetAttempt(), req.GetSerialNum(), req.GetTaskId())
	default:
	}
}
