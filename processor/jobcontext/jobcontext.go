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

package jobcontext

import (
	"context"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ context.Context = &JobContext{}

type JobContext struct {
	context.Context

	// basic information
	jobId                   int64
	jobInstanceId           int64
	wfInstanceId            int64
	taskId                  int64
	jobName                 string
	scheduleTime            time.Duration
	dataTime                time.Duration
	executeMode             string
	jobType                 string
	instanceMasterActorPath string
	taskName                string
	task                    []byte
	groupId                 string
	content                 string
	user                    string

	// jobInstance maximum number of retries
	maxAttempt int32
	// jobInstance current retry count
	attempt int32

	// job custom parameters
	jobParameters string

	// custom parameters be passed every time when task is triggered, and can they only be passed through API triggers
	instanceParameters string

	// upstream Data for the workflow
	upstreamData []*common.JobInstanceData

	// results of all child tasks, map[int64]string
	taskResults map[int64]string

	// status of all child tasks, map[int64]TaskStatus
	taskStatuses map[int64]taskstatus.TaskStatus

	// task maximum retry count
	taskMaxAttempt int32
	// task current retry count
	taskAttempt int32
	// task retry interval (seconds)
	taskAttemptInterval int32

	// second-level task loop times
	serialNum int64

	// sharding id
	shardingId int64
	// sharding parameter
	shardingParameter string
	// sharding count
	shardingNum int32

	allWorkerAddrs []string
	workerAddr     string

	timeType       int32
	timeExpression string
}

func (j *JobContext) UpstreamData() []*common.JobInstanceData {
	return j.upstreamData
}

func (j *JobContext) SetUpstreamData(upstreamData []*common.JobInstanceData) {
	j.upstreamData = upstreamData
}

func (j *JobContext) JobId() int64 {
	return j.jobId
}

func (j *JobContext) SetJobId(jobId int64) {
	j.jobId = jobId
}

func (j *JobContext) JobInstanceId() int64 {
	return j.jobInstanceId
}

func (j *JobContext) SetJobInstanceId(jobInstanceId int64) {
	j.jobInstanceId = jobInstanceId
}

func (j *JobContext) WfInstanceId() int64 {
	return j.wfInstanceId
}

func (j *JobContext) SetWfInstanceId(wfInstanceId int64) {
	j.wfInstanceId = wfInstanceId
}

func (j *JobContext) TaskId() int64 {
	return j.taskId
}

func (j *JobContext) SetTaskId(taskId int64) {
	j.taskId = taskId
}

func (j *JobContext) JobName() string {
	return j.jobName
}

func (j *JobContext) SetJobName(jobName string) {
	j.jobName = jobName
}

func (j *JobContext) ScheduleTime() time.Duration {
	return j.scheduleTime
}

func (j *JobContext) SetScheduleTime(scheduleTime time.Duration) {
	j.scheduleTime = scheduleTime
}

func (j *JobContext) DataTime() time.Duration {
	return j.dataTime
}

func (j *JobContext) SetDataTime(dataTime time.Duration) {
	j.dataTime = dataTime
}

func (j *JobContext) ExecuteMode() string {
	return j.executeMode
}

func (j *JobContext) SetExecuteMode(executeMode string) {
	j.executeMode = executeMode
}

func (j *JobContext) JobType() string {
	return j.jobType
}

func (j *JobContext) SetJobType(jobType string) {
	j.jobType = jobType
}

func (j *JobContext) InstanceMasterActorPath() string {
	return j.instanceMasterActorPath
}

func (j *JobContext) SetInstanceMasterActorPath(instanceMasterActorPath string) {
	j.instanceMasterActorPath = instanceMasterActorPath
}

func (j *JobContext) TaskName() string {
	return j.taskName
}

func (j *JobContext) SetTaskName(taskName string) {
	j.taskName = taskName
}

func (j *JobContext) Task() []byte {
	return j.task
}

func (j *JobContext) SetTask(task []byte) {
	j.task = task
}

func (j *JobContext) GroupId() string {
	return j.groupId
}

func (j *JobContext) SetGroupId(groupId string) {
	j.groupId = groupId
}

func (j *JobContext) Content() string {
	return j.content
}

func (j *JobContext) SetContent(content string) {
	j.content = content
}

func (j *JobContext) User() string {
	return j.user
}

func (j *JobContext) SetUser(user string) {
	j.user = user
}

func (j *JobContext) MaxAttempt() int32 {
	return j.maxAttempt
}

func (j *JobContext) SetMaxAttempt(maxAttempt int32) {
	j.maxAttempt = maxAttempt
}

func (j *JobContext) Attempt() int32 {
	return j.attempt
}

func (j *JobContext) SetAttempt(attempt int32) {
	j.attempt = attempt
}

func (j *JobContext) JobParameters() string {
	return j.jobParameters
}

func (j *JobContext) SetJobParameters(jobParameters string) {
	j.jobParameters = jobParameters
}

func (j *JobContext) InstanceParameters() string {
	return j.instanceParameters
}

func (j *JobContext) SetInstanceParameters(instanceParameters string) {
	j.instanceParameters = instanceParameters
}

func (j *JobContext) TaskResults() map[int64]string {
	return j.taskResults
}

func (j *JobContext) SetTaskResults(taskResults map[int64]string) {
	j.taskResults = taskResults
}

func (j *JobContext) TaskStatuses() map[int64]taskstatus.TaskStatus {
	return j.taskStatuses
}

func (j *JobContext) SetTaskStatuses(taskStatuses map[int64]taskstatus.TaskStatus) {
	j.taskStatuses = taskStatuses
}

func (j *JobContext) TaskMaxAttempt() int32 {
	return j.taskMaxAttempt
}

func (j *JobContext) SetTaskMaxAttempt(taskMaxAttempt int32) {
	j.taskMaxAttempt = taskMaxAttempt
}

func (j *JobContext) TaskAttempt() int32 {
	return j.taskAttempt
}

func (j *JobContext) SetTaskAttempt(taskAttempt int32) {
	j.taskAttempt = taskAttempt
}

func (j *JobContext) TaskAttemptInterval() int32 {
	return j.taskAttemptInterval
}

func (j *JobContext) SetTaskAttemptInterval(taskAttemptInterval int32) {
	j.taskAttemptInterval = taskAttemptInterval
}

func (j *JobContext) SerialNum() int64 {
	return j.serialNum
}

func (j *JobContext) SetSerialNum(serialNum int64) {
	j.serialNum = serialNum
}

func (j *JobContext) ShardingId() int64 {
	return j.shardingId
}

func (j *JobContext) SetShardingId(shardingId int64) {
	j.shardingId = shardingId
}

func (j *JobContext) ShardingParameter() string {
	return j.shardingParameter
}

func (j *JobContext) SetShardingParameter(shardingParameter string) {
	j.shardingParameter = shardingParameter
}

func (j *JobContext) ShardingNum() int32 {
	return j.shardingNum
}

func (j *JobContext) SetShardingNum(shardingNum int32) {
	j.shardingNum = shardingNum
}

func (j *JobContext) AllWorkerAddrs() []string {
	return j.allWorkerAddrs
}

func (j *JobContext) SetAllWorkerAddrs(allWorkerAddrs []string) {
	j.allWorkerAddrs = allWorkerAddrs
}

func (j *JobContext) WorkerAddr() string {
	return j.workerAddr
}

func (j *JobContext) SetWorkerAddr(workerAddr string) {
	j.workerAddr = workerAddr
}

func (j *JobContext) TimeType() int32 {
	return j.timeType
}

func (j *JobContext) SetTimeType(timeType int32) {
	j.timeType = timeType
}

func (j *JobContext) TimeExpression() string {
	return j.timeExpression
}

func (j *JobContext) SetTimeExpression(timeExpression string) {
	j.timeExpression = timeExpression
}
