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
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/asynkron/protoactor-go/actor"
	"testing"
)

type controlRecorder struct {
	retirementActorContext
	sends int
}

func (c *controlRecorder) Send(*actor.PID, interface{}) { c.sends++ }
func TestRetiredMasterCannotSendKill(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	c := &controlRecorder{retirementActorContext: retirementActorContext{system: actor.NewActorSystem()}}
	m.actorCtx = c
	m.Stop()
	m.KillInstance("late kill")
	if c.sends != 0 {
		t.Fatalf("retired master sent %d kill messages", c.sends)
	}
}

func TestKillInstanceDuringSecondDelay(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.Clear(m)
	c := &controlRecorder{retirementActorContext: retirementActorContext{system: actor.NewActorSystem()}}
	m.actorCtx = c
	m.KillInstance("killed from server")
	if !m.IsKilled() {
		t.Fatal("idle second cycle ignored explicit kill")
	}
	m.Stop()
}

func TestDelayedTerminalCannotFinishClosedCycle(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.Clear(m)
	m.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, "late callback")
	if m.GetInstanceStatus() != processor.InstanceStatusRunning {
		t.Fatal("late terminal changed closed-cycle status and will stop next-cycle loops")
	}
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	m.Stop()
}
