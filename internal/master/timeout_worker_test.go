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

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
)

type r5TimeoutWarning struct {
	heldProgressLogger
	hold sync.Once
}

func (l *r5TimeoutWarning) Warnf(f string, _ ...interface{}) {
	if f == "jobInstanceId=%d, worker[%s] is down, try another worker, size:%d" {
		l.hold.Do(func() { close(l.entered); <-l.release })
	}
}
func TestOldTimeoutCannotRemoveNextCycleWorker(t *testing.T) {
	if isolateHandoffProbe(t) {
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	workers := []string{"worker@127.0.0.1:1234", "other@127.0.0.1:1235"}
	m.jobInstanceInfo.SetAllWorkers(workers)
	m.actorCtx = r3TimeoutContext{retirementActorContext{system: actor.NewActorSystem()}}
	l := &r5TimeoutWarning{heldProgressLogger: heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}}
	logger.SetLogger(l)
	done := make(chan struct{})
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(7), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	go func() {
		defer close(done)
		m.BatchDispatchTasks([]*schedulerx.MasterStartContainerRequest{req}, workers[0])
	}()
	waitRetirementTest(t, l.entered)
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.Clear(m)
	m.AcquireSerialNum()
	m.jobInstanceInfo.SetAllWorkers(workers)
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	close(l.release)
	waitRetirementTest(t, done)
	got := m.jobInstanceInfo.GetAllWorkers()
	if len(got) != 2 {
		t.Fatalf("cycle-0 timeout removed current cycle-1 worker: got=%v", got)
	}
}
