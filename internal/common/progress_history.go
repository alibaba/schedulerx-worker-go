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

type ProgressHistory struct {
	serialNum       int64
	startTime       int64
	endTime         int64
	costTime        int64
	success         bool
	taskProgressMap map[string]*TaskProgressCounter
}

func NewProgressHistory() (rcvr *ProgressHistory) {
	rcvr = &ProgressHistory{}
	return
}

func (p *ProgressHistory) SerialNum() int64 {
	return p.serialNum
}

func (p *ProgressHistory) SetSerialNum(serialNum int64) {
	p.serialNum = serialNum
}

func (p *ProgressHistory) StartTime() int64 {
	return p.startTime
}

func (p *ProgressHistory) SetStartTime(startTime int64) {
	p.startTime = startTime
}

func (p *ProgressHistory) EndTime() int64 {
	return p.endTime
}

func (p *ProgressHistory) SetEndTime(endTime int64) {
	p.endTime = endTime
}

func (p *ProgressHistory) CostTime() int64 {
	return p.costTime
}

func (p *ProgressHistory) SetCostTime(costTime int64) {
	p.costTime = costTime
}

func (p *ProgressHistory) Success() bool {
	return p.success
}

func (p *ProgressHistory) SetSuccess(success bool) {
	p.success = success
}

func (p *ProgressHistory) TaskProgressMap() map[string]*TaskProgressCounter {
	return p.taskProgressMap
}

func (p *ProgressHistory) SetTaskProgressMap(taskProgressMap map[string]*TaskProgressCounter) {
	p.taskProgressMap = taskProgressMap
}
