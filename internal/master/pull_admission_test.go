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
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"google.golang.org/protobuf/proto"
	"testing"
)

type r4HeldPull struct {
	batch.TaskDispatchReqHandler
	entered, release chan struct{}
}

func (h *r4HeldPull) SyncHandleReqsWithAdmission(handler batch.ReqHandler, size int32, worker string, admit func(func()) bool) []interface{} {
	close(h.entered)
	<-h.release
	return h.TaskDispatchReqHandler.SyncHandleReqsWithAdmission(handler, size, worker, admit)
}
func TestOldPullCannotDrainNextCycleQueue(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.jobInstanceInfo.SetAllWorkers(nil)
	m.xAttrs = &common.MapTaskXAttrs{GlobalConsumerSize: 100, TaskDispatchMode: "pull"}
	held := &r4HeldPull{m.taskDispatchReqHandler, make(chan struct{}), make(chan struct{})}
	m.taskDispatchReqHandler = held
	done := make(chan struct{})
	var response []*schedulerx.MasterStartContainerRequest
	go func() { defer close(done); response = m.SyncPullTasks(1, "worker@127.0.0.1:1234") }()
	waitRetirementTest(t, held.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("current"), Attempt: proto.Int32(0), SerialNum: proto.Int64(1)}
	m.taskBlockingQueue.SubmitRequest(req)
	close(held.release)
	waitRetirementTest(t, done)
	n, err := m.taskPersistence.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if len(response) != 0 || m.taskBlockingQueue.Size() != 1 || n != 0 || err != nil {
		t.Fatal("old pull drained current-cycle task, persisted it, then returned no task; healthy worker never received task 99")
	}
	m.taskDispatchReqHandler = held.TaskDispatchReqHandler
	if got := m.SyncPullTasks(1, "worker@127.0.0.1:1234"); len(got) != 1 || got[0].GetTaskId() != 99 {
		t.Fatal("current-cycle pull did not deliver preserved task")
	}
}
