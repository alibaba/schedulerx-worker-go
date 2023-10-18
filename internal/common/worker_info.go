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

type WorkerInfo struct {
	ip         string
	port       int
	workerId   string
	workerAddr string
	akkaPath   string
	metrics    Metrics
	version    string
	starter    string
	label      string

	// busyStatus is the trigger action, busy flag, default value is free
	busyStatus WorkerBusyStatus
}

func NewWorkerInfo() *WorkerInfo {
	return &WorkerInfo{
		busyStatus: FREE,
	}
}

type WorkerBusyStatus int32

const (
	FREE       WorkerBusyStatus = 0
	LOAD5_BUSY WorkerBusyStatus = 1
	HEAP5_BUSY WorkerBusyStatus = 2
	DISK_BUSY  WorkerBusyStatus = 3
)

var workerBusyStatusDesc = map[WorkerBusyStatus]string{
	FREE:       "空闲",
	LOAD5_BUSY: "load5过高",
	HEAP5_BUSY: "heap5过高",
	DISK_BUSY:  "磁盘使用率过高",
}

func (status WorkerBusyStatus) Descriptor() string {
	return workerBusyStatusDesc[status]
}
