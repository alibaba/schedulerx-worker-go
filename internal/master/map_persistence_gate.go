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
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

// withTaskPersistence joins the entire local persistence operation, including
// Pull's read-and-update and retry sleeps. Never call a status/user callback or
// perform container RPCs here: those may synchronously re-enter Stop/Clear.
func (m *MapTaskMaster) withTaskPersistence(serial int64, operation func()) bool {
	m.statusWriteMu.RLock()
	defer m.statusWriteMu.RUnlock()
	if m.statusWritesRetired || m.cycleClosed || serial != m.GetSerialNum() {
		return false
	}
	operation()
	return true
}

func (m *MapTaskMaster) acceptsTaskGeneration(attempt *int32, serial *int64) bool {
	return m.acceptsStatusAttempt(attempt) && (serial == nil || *serial == m.GetSerialNum())
}

func (m *MapTaskMaster) cycleActive(serial int64) bool {
	m.statusWriteMu.RLock()
	defer m.statusWriteMu.RUnlock()
	return !m.statusWritesRetired && !m.cycleClosed && serial == m.GetSerialNum()
}

func (m *MapTaskMaster) pullPersistedTasks(serial, id int64, size int32) (tasks []*common.TaskInfo, err error, admitted bool) {
	admitted = m.withTaskPersistence(serial, func() { tasks, err = m.taskPersistence.Pull(id, size) })
	return
}
func (m *MapTaskMaster) updatePersistedTasks(serial, id int64, ids []int64, status taskstatus.TaskStatus, worker, addr string) (count int64, err error, admitted bool) {
	admitted = m.withTaskPersistence(serial, func() { count, err = m.taskPersistence.UpdateTaskStatus(id, ids, status, worker, addr) })
	return
}
func (m *MapTaskMaster) batchUpdatePersistedTasks(serial, id int64, status taskstatus.TaskStatus, worker, addr string) (count int64, admitted bool) {
	admitted = m.withTaskPersistence(serial, func() { count = m.taskPersistence.BatchUpdateTaskStatus(id, status, worker, addr) })
	return
}

// Called only while constructing the concrete wrapper, before publication or
// Start. A queued handler must never look up a replacement by instance ID.
func (m *MapTaskMaster) bindBatchHandlers(owner taskmaster.MapTaskMaster) {
	m.taskStatusReqBatchHandler.BindTaskMaster(owner)
	m.taskDispatchReqHandler.(interface {
		BindTaskMaster(taskmaster.MapTaskMaster)
	}).BindTaskMaster(owner)
}

func (m *MapTaskMaster) filterDispatchRequests(reqs []*schedulerx.MasterStartContainerRequest) []*schedulerx.MasterStartContainerRequest {
	accepted := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
	for _, req := range reqs {
		if m.acceptsTaskGeneration(req.Attempt, req.SerialNum) {
			accepted = append(accepted, req)
		}
	}
	return accepted
}

// State commits are short and exclude callbacks, queues and I/O. In particular,
// revalidate after an I/O wait before updating a possibly reused cycle's maps.
func (m *MapTaskMaster) withCycleState(serial int64, operation func()) bool {
	m.statusWriteMu.Lock()
	defer m.statusWriteMu.Unlock()
	if m.statusWritesRetired || m.cycleClosed || serial != m.GetSerialNum() {
		return false
	}
	operation()
	return true
}
func (m *MapTaskMaster) acceptsMapContext(ctx *jobcontext.JobContext) bool {
	if !m.cycleActive(m.GetSerialNum()) {
		return false
	}
	// Remote legacy Map messages have no generation fields; local contexts do.
	return ctx == nil || (ctx.Attempt() == m.jobInstanceInfo.GetAttempt() && ctx.SerialNum() == m.GetSerialNum())
}
func (m *MapTaskMaster) cycleDone() <-chan struct{} {
	m.statusWriteMu.RLock()
	defer m.statusWriteMu.RUnlock()
	if m.cycleCtx == nil {
		return nil
	}
	return m.cycleCtx.Done()
}
