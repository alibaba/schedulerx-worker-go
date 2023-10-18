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

package openapi

import (
	"encoding/json"
	"reflect"
)

// JavaJobConfig java task basic configuration
type JavaJobConfig struct {
	JobBaseConfig

	// Execution class full path
	className string
	// Support oss address acquisition and execution jar
	jarUrl string
	// Advanced configuration (grid and parallel tasks only)
	mapTaskXAttrs MapTaskXAttrs
}

func NewDefaultJavaJobConfig(className string) JavaJobConfig {
	cfg := JavaJobConfig{
		className:     className,
		JobBaseConfig: NewDefaultJobBaseConfig(),
	}

	type Content struct {
		ClassName string `json:"className"`
	}
	c := Content{
		ClassName: className,
	}
	contentData, _ := json.Marshal(c)

	cfg.JobBaseConfig.paramMap = map[string]interface{}{
		"jobType":     "java",
		"content":     string(contentData),
		"contentType": "text",
	}
	return cfg
}

func (c JavaJobConfig) IsRequired() bool {
	return c.className != "" && c.JobBaseConfig.IsRequired()
}

// JobBaseConfig task public information
type JobBaseConfig struct {
	jobId      int64
	workflowId int64

	// Task name
	name string

	// Job details
	description string
	// standalone、broadcast、parallel、grid、batch
	executeMode string
	// Custom parameters
	parameters string
	// Maximum number of concurrently running instances (default number of concurrency is 1)
	maxConcurrency int32
	// Maximum number of error retries
	maxAttempt int32
	// Retry interval (unit s) defaults to 30s
	attemptInterval int32
	// Task status (0: disabled; 1: enabled) is enabled by default after creation,
	// and the assignment is invalid and modified for the interface (default 1)
	status         int
	timeConfig     TimeConfig
	jobMonitorInfo JobMonitorInfo
	priority       int8
	paramMap       map[string]interface{}
}

func NewDefaultJobBaseConfig() JobBaseConfig {
	return JobBaseConfig{
		maxConcurrency:  1,
		maxAttempt:      0,
		attemptInterval: 30,
		status:          1,
		timeConfig:      NewDefaultTimeConfig(),
		jobMonitorInfo:  NewDefaultJobMonitorInfo(),
	}
}

func (c JobBaseConfig) IsRequired() bool {
	if c.name != "" && c.executeMode != "" && c.timeConfig.IsRequired() && c.jobMonitorInfo.IsRequired() {
		return true
	}
	return false
}

type MapTaskXAttrs struct {
	// The default number of execution threads for a single trigger on a single machine is 5
	consumerSize int
	// The default number of subtask distribution threads is 5
	dispatcherSize int
	// Number of retries on failed subtask
	taskMaxAttempt int
	// Subtask failure retry interval
	taskAttemptInterval int
	// Subtask distribution mode (push/pull)
	taskDispatchMode string

	//==== Pull model exclusive ======
	// The number of subtasks pulled by a single machine each time, the default is 5
	pageSize int
	// Single-machine subtask queue cache, default 10
	queueSize int
	// Number of concurrent consumption of global subtasks
	globalConsumerSize int
}

func NewDefaultMapTaskXAttrs() MapTaskXAttrs {
	return MapTaskXAttrs{
		consumerSize:       DefaultXattrsConsumerSize,
		dispatcherSize:     DefaultXattrsDispatcherSize,
		pageSize:           DefaultXattrsPageSize,
		queueSize:          DefaultXattrsQueueSize,
		globalConsumerSize: DefaultXattrsGlobalConsumerSize,
	}
}

// TimeConfig time expression
type TimeConfig struct {
	/**
	 * cron：1
	 * fix_rate: 3
	 * api: 100
	 * Default cron type
	 */
	timeType int8
	// cron: express time expression
	// fix_rate: fixed period (unit s)
	// api: no need to fill in
	timeExpression string
	// Custom calendar
	calendar string
	// Custom time offset unit s
	dataOffset int64
	// Default empty
	// Use the time zone of the schedulerx server. Please refer to the documentation for special requirements.
	timezone string

	paramMap map[string]interface{}
}

func NewDefaultTimeConfig() TimeConfig {
	return TimeConfig{
		timeType: CronType,
	}
}

func (c TimeConfig) IsRequired() bool {
	if reflect.DeepEqual(c, TimeConfig{}) || c.timeType == 0 {
		return false
	}
	if c.timeType != APIType && c.timeExpression == "" {
		// 非api任务都必须指定时间表达式
		return false
	}
	return true
}

// JobMonitorInfo task alarm information configuration
type JobMonitorInfo struct {
	// alarm configuration information
	monitorConfig MonitorConfig
	// alarm contact information
	contactInfo []ContactInfo

	paramMap map[string]interface{}
}

func NewDefaultJobMonitorInfo() JobMonitorInfo {
	return JobMonitorInfo{
		monitorConfig: NewDefaultMonitorConfig(),
	}
}

func (c JobMonitorInfo) IsRequired() bool {
	return !reflect.DeepEqual(c, JobMonitorInfo{})
}

// MonitorConfig monitor configuration json content
type MonitorConfig struct {
	// Timeout switch (enabled by default)
	timeoutEnable bool
	// Task failure alarm (enabled by default)
	failEnable bool
	// Number of consecutive failure alarms (default is 1)
	failLimitTimes int32
	// Error rate alarm
	failRate int32

	// No machine available alarm, closed by default
	missWorkerEnable bool

	// timeout threshold
	timeout int64
	// Timeout kill switch (off by default)
	timeoutKillEnable bool
	// Deadline (accurate to the hour and minute)
	deadline string
	// Deadline spans days
	daysOfDeadline int32

	// Alarm sending channels (sms, mail, phone, ding) default to DingTalk
	sendChannel string
}

func NewDefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		timeoutEnable:  true,
		failEnable:     true,
		failLimitTimes: 1,
		sendChannel:    "ding",
	}
}

// ContactInfo monitoring alarm contacts
type ContactInfo struct {
	// Job number (for internal use within the group)
	empId string
	// Convenient interface configuration and use within the group (flower name)
	userName string
	// No need to fill in within the group
	userPhone string
	// No need to fill in within the group
	userMail string
	// DingTalk robot webhook, no need to fill in within the group
	ding string
}
