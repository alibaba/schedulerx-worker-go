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

package common

import (
	"time"
)

type JobInstanceInfo struct {
	jobId          int64
	jobInstanceId  int64
	wfInstanceId   int64
	regionId       int32
	appGroupId     int64
	groupId        string
	jobName        string
	scheduleTime   time.Duration
	dataTime       time.Duration
	jobType        string
	executeMode    string
	content        string
	user           string
	timeType       int32
	timeExpression string
	priority       int32
	status         int32
	aliveTimestamp int64

	// workerIdAddr={workerId}@{ip}:{port}
	workerIdAddr string
	triggerType  int32

	// job custom parameters
	parameters     string
	xattrs         string
	allWorkers     []string
	jobConcurrency int32
	maxAttempt     int32
	attempt        int32

	// custom parameters for each task scheduling
	instanceParameters string
	isRetry            bool
	upstreamData       []*JobInstanceData

	// specify the particular machine for task execution
	workerInfo *WorkerInfo
}

func (j *JobInstanceInfo) SetJobId(jobId int64) {
	j.jobId = jobId
}

func (j *JobInstanceInfo) SetJobInstanceId(jobInstanceId int64) {
	j.jobInstanceId = jobInstanceId
}

func (j *JobInstanceInfo) SetWfInstanceId(wfInstanceId int64) {
	j.wfInstanceId = wfInstanceId
}

func (j *JobInstanceInfo) SetRegionId(regionId int32) {
	j.regionId = regionId
}

func (j *JobInstanceInfo) SetAppGroupId(appGroupId int64) {
	j.appGroupId = appGroupId
}

func (j *JobInstanceInfo) SetGroupId(groupId string) {
	j.groupId = groupId
}

func (j *JobInstanceInfo) SetJobName(jobName string) {
	j.jobName = jobName
}

func (j *JobInstanceInfo) SetScheduleTime(scheduleTime time.Duration) {
	j.scheduleTime = scheduleTime
}

func (j *JobInstanceInfo) SetDataTime(dataTime time.Duration) {
	j.dataTime = dataTime
}

func (j *JobInstanceInfo) SetJobType(jobType string) {
	j.jobType = jobType
}

func (j *JobInstanceInfo) SetExecuteMode(executeMode string) {
	j.executeMode = executeMode
}

func (j *JobInstanceInfo) SetContent(content string) {
	j.content = content
}

func (j *JobInstanceInfo) SetUser(user string) {
	j.user = user
}

func (j *JobInstanceInfo) SetTimeType(timeType int32) {
	j.timeType = timeType
}

func (j *JobInstanceInfo) SetTimeExpression(timeExpression string) {
	j.timeExpression = timeExpression
}

func (j *JobInstanceInfo) SetPriority(priority int32) {
	j.priority = priority
}

func (j *JobInstanceInfo) SetStatus(status int32) {
	j.status = status
}

func (j *JobInstanceInfo) SetAliveTimestamp(aliveTimestamp int64) {
	j.aliveTimestamp = aliveTimestamp
}

func (j *JobInstanceInfo) SetWorkerIdAddr(workerIdAddr string) {
	j.workerIdAddr = workerIdAddr
}

func (j *JobInstanceInfo) SetTriggerType(triggerType int32) {
	j.triggerType = triggerType
}

func (j *JobInstanceInfo) SetParameters(parameters string) {
	j.parameters = parameters
}

func (j *JobInstanceInfo) SetXattrs(xattrs string) {
	j.xattrs = xattrs
}

func (j *JobInstanceInfo) SetJobConcurrency(jobConcurrency int32) {
	j.jobConcurrency = jobConcurrency
}

func (j *JobInstanceInfo) SetMaxAttempt(maxAttempt int32) {
	j.maxAttempt = maxAttempt
}

func (j *JobInstanceInfo) SetAttempt(attempt int32) {
	j.attempt = attempt
}

func (j *JobInstanceInfo) SetInstanceParameters(instanceParameters string) {
	j.instanceParameters = instanceParameters
}

func (j *JobInstanceInfo) SetIsRetry(isRetry bool) {
	j.isRetry = isRetry
}

func (j *JobInstanceInfo) SetUpstreamData(upstreamData []*JobInstanceData) {
	j.upstreamData = upstreamData
}

func (j *JobInstanceInfo) SetWorkerInfo(workerInfo *WorkerInfo) {
	j.workerInfo = workerInfo
}

func NewJobInstanceInfo() *JobInstanceInfo {
	return &JobInstanceInfo{
		workerInfo: NewWorkerInfo(),
	}
}

func (j *JobInstanceInfo) SetAllWorkers(allWorkers []string) {
	j.allWorkers = allWorkers
}

func (j *JobInstanceInfo) GetJobId() int64 {
	return j.jobId
}

func (j *JobInstanceInfo) GetJobInstanceId() int64 {
	return j.jobInstanceId
}

func (j *JobInstanceInfo) GetWfInstanceId() int64 {
	return j.wfInstanceId
}

func (j *JobInstanceInfo) GetRegionId() int32 {
	return j.regionId
}

func (j *JobInstanceInfo) GetAppGroupId() int64 {
	return j.appGroupId
}

func (j *JobInstanceInfo) GetGroupId() string {
	return j.groupId
}

func (j *JobInstanceInfo) GetJobName() string {
	return j.jobName
}

func (j *JobInstanceInfo) GetScheduleTime() time.Duration {
	return j.scheduleTime
}

func (j *JobInstanceInfo) GetDataTime() time.Duration {
	return j.dataTime
}

func (j *JobInstanceInfo) GetJobType() string {
	return j.jobType
}

func (j *JobInstanceInfo) GetExecuteMode() string {
	return j.executeMode
}

func (j *JobInstanceInfo) GetContent() string {
	return j.content
}

func (j *JobInstanceInfo) GetUser() string {
	return j.user
}

func (j *JobInstanceInfo) GetTimeType() int32 {
	return j.timeType
}

func (j *JobInstanceInfo) GetTimeExpression() string {
	return j.timeExpression
}

func (j *JobInstanceInfo) GetPriority() int32 {
	return j.priority
}

func (j *JobInstanceInfo) GetStatus() int32 {
	return j.status
}

func (j *JobInstanceInfo) GetAliveTimestamp() int64 {
	return j.aliveTimestamp
}

func (j *JobInstanceInfo) GetWorkerIdAddr() string {
	return j.workerIdAddr
}

func (j *JobInstanceInfo) GetTriggerType() int32 {
	return j.triggerType
}

func (j *JobInstanceInfo) GetParameters() string {
	return j.parameters
}

func (j *JobInstanceInfo) GetXattrs() string {
	return j.xattrs
}

func (j *JobInstanceInfo) GetAllWorkers() []string {
	return j.allWorkers
}

func (j *JobInstanceInfo) GetJobConcurrency() int32 {
	return j.jobConcurrency
}

func (j *JobInstanceInfo) GetMaxAttempt() int32 {
	return j.maxAttempt
}

func (j *JobInstanceInfo) GetAttempt() int32 {
	return j.attempt
}

func (j *JobInstanceInfo) GetInstanceParameters() string {
	return j.instanceParameters
}

func (j *JobInstanceInfo) GetIsRetry() bool {
	return j.isRetry
}

func (j *JobInstanceInfo) GetUpstreamData() []*JobInstanceData {
	return j.upstreamData
}

func (j *JobInstanceInfo) GetWorkerInfo() *WorkerInfo {
	return j.workerInfo
}
