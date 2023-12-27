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

type LogConfigInfo struct {
	Id       int64  `json:"id"`
	LogType  string `json:"logType"`
	Config   string `json:"config"`
	UserId   string `json:"userId"`
	RegionId string `json:"regionId"`
}

func NewLogConfigInfo() (rcvr *LogConfigInfo) {
	rcvr = &LogConfigInfo{}
	return
}

func (rcvr *LogConfigInfo) GetConfig() string {
	return rcvr.Config
}

func (rcvr *LogConfigInfo) GetId() int64 {
	return rcvr.Id
}

func (rcvr *LogConfigInfo) GetRegionId() string {
	return rcvr.RegionId
}

func (rcvr *LogConfigInfo) GetType() string {
	return rcvr.LogType
}

func (rcvr *LogConfigInfo) GetUserId() string {
	return rcvr.UserId
}

func (rcvr *LogConfigInfo) SetConfig(config string) {
	rcvr.Config = config
}

func (rcvr *LogConfigInfo) SetId(id int64) {
	rcvr.Id = id
}

func (rcvr *LogConfigInfo) SetRegionId(regionId string) {
	rcvr.RegionId = regionId
}

func (rcvr *LogConfigInfo) SetType(logType string) {
	rcvr.LogType = logType
}

func (rcvr *LogConfigInfo) SetUserId(userId string) {
	rcvr.UserId = userId
}
