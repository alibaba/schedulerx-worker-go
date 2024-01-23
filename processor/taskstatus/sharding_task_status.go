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

package taskstatus

import (
	"strconv"
)

type ShardingTaskStatus struct {
	id         int64
	workerAddr string
	status     int32
	statusType *TypeInfo
}

func NewShardingTaskStatus(id int64, workerAddr string, status int32) *ShardingTaskStatus {
	return &ShardingTaskStatus{
		id:         id,
		workerAddr: workerAddr,
		status:     status,
		statusType: NewTypeInfo(strconv.Itoa(int(status)), TaskStatus(status).Descriptor()),
	}
}

func (s *ShardingTaskStatus) Id() int64 {
	return s.id
}

func (s *ShardingTaskStatus) SetId(id int64) {
	s.id = id
}

func (s *ShardingTaskStatus) WorkerAddr() string {
	return s.workerAddr
}

func (s *ShardingTaskStatus) SetWorkerAddr(workerAddr string) {
	s.workerAddr = workerAddr
}

func (s *ShardingTaskStatus) Status() int32 {
	return s.status
}

func (s *ShardingTaskStatus) SetStatus(status int32) {
	s.status = status
}

func (s *ShardingTaskStatus) StatusType() *TypeInfo {
	return s.statusType
}

func (s *ShardingTaskStatus) SetStatusType(statusType *TypeInfo) {
	s.statusType = statusType
}
