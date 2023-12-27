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

package persistence

import (
	"time"
)

type TaskSnapshot struct {
	JobId         int64     `db:"job_id"`
	JobInstanceId int64     `db:"job_instance_id"`
	TaskId        int64     `db:"task_id"`
	TaskName      string    `db:"task_name"`
	Status        int32     `db:"status"`
	Progress      float64   `db:"progress"`
	GmtCreate     time.Time `db:"gmt_create"`
	GmtModified   time.Time `db:"gmt_modified"`
	WorkerAddr    string    `db:"worker_addr"`
	WorkerId      string    `db:"worker_id"`
	TaskBody      []byte    `db:"task_body"`
}

func NewTaskSnapshot() (rcvr *TaskSnapshot) {
	rcvr = &TaskSnapshot{}
	return
}

func (t *TaskSnapshot) GetJobId() int64 {
	return t.JobId
}

func (t *TaskSnapshot) SetJobId(jobId int64) {
	t.JobId = jobId
}

func (t *TaskSnapshot) GetJobInstanceId() int64 {
	return t.JobInstanceId
}

func (t *TaskSnapshot) SetJobInstanceId(jobInstanceId int64) {
	t.JobInstanceId = jobInstanceId
}

func (t *TaskSnapshot) GetTaskId() int64 {
	return t.TaskId
}

func (t *TaskSnapshot) SetTaskId(taskId int64) {
	t.TaskId = taskId
}

func (t *TaskSnapshot) GetTaskName() string {
	return t.TaskName
}

func (t *TaskSnapshot) SetTaskName(taskName string) {
	t.TaskName = taskName
}

func (t *TaskSnapshot) GetStatus() int32 {
	return t.Status
}

func (t *TaskSnapshot) SetStatus(Status int32) {
	t.Status = Status
}

func (t *TaskSnapshot) GetProgress() float64 {
	return t.Progress
}

func (t *TaskSnapshot) SetProgress(progress float64) {
	t.Progress = progress
}

func (t *TaskSnapshot) GetGmtCreate() time.Time {
	return t.GmtCreate
}

func (t *TaskSnapshot) SetGmtCreate(gmtCreate time.Time) {
	t.GmtCreate = gmtCreate
}

func (t *TaskSnapshot) GetGmtModified() time.Time {
	return t.GmtModified
}

func (t *TaskSnapshot) SetGmtModified(gmtModified time.Time) {
	t.GmtModified = gmtModified
}

func (t *TaskSnapshot) GetWorkerAddr() string {
	return t.WorkerAddr
}

func (t *TaskSnapshot) SetWorker(workerAddr string) {
	t.WorkerAddr = workerAddr
}

func (t *TaskSnapshot) GetWorkerId() string {
	return t.WorkerId
}

func (t *TaskSnapshot) SetWorkerId(workerId string) {
	t.WorkerId = workerId
}

func (t *TaskSnapshot) GetTaskBody() []byte {
	return t.TaskBody
}

func (t *TaskSnapshot) SetTaskBody(taskBody []byte) {
	t.TaskBody = taskBody
}
