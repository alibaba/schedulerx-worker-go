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

package processor

type InstanceStatus int32

const (
	InstanceStatusUnknown       InstanceStatus = 0
	InstanceStatusWaiting       InstanceStatus = 1
	InstanceStatusReady         InstanceStatus = 2
	InstanceStatusRunning       InstanceStatus = 3
	InstanceStatusSucceed       InstanceStatus = 4
	InstanceStatusFailed        InstanceStatus = 5
	InstanceStatusKilled        InstanceStatus = 6
	InstanceStatusPaused        InstanceStatus = 7
	InstanceStatusSubmitted     InstanceStatus = 8
	InstanceStatusRejected      InstanceStatus = 9
	InstanceStatusAccepted      InstanceStatus = 10
	InstanceStatusPartialFailed InstanceStatus = 11
	InstanceStatusRemoved       InstanceStatus = 99
)

var (
	instanceStatusDesc = map[InstanceStatus]string{
		0:  "未知",
		1:  "等待",
		2:  "池子",
		3:  "运行",
		4:  "成功",
		5:  "失败",
		7:  "暂停",
		8:  "已提交",
		9:  "拒绝",
		10: "接收",
		11: "部分失败",
		99: "删除",
	}
	instanceStatusEnDesc = map[InstanceStatus]string{
		0:  "unknown",
		1:  "waiting",
		2:  "ready",
		3:  "running",
		4:  "success",
		5:  "failed",
		7:  "paused",
		8:  "submitted",
		9:  "rejected",
		10: "accepted",
		11: "partial_failed",
		99: "removed",
	}
)

func (status InstanceStatus) Descriptor() string {
	return instanceStatusDesc[status]
}

func (status InstanceStatus) EnDescriptor() string {
	return instanceStatusEnDesc[status]
}

func (status InstanceStatus) IsFinished() bool {
	return status == InstanceStatusSucceed || status == InstanceStatusFailed || status == InstanceStatusRemoved
}
