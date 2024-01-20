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

// AppGroupInfo application group information
type AppGroupInfo struct {
	Id            int64          `json:"id"`
	GroupId       string         `json:"groupId"`
	AppName       string         `json:"appName"`
	AppKey        string         `json:"appKey"`
	MaxJobs       int32          `json:"maxJobs"`
	Status        int32          `json:"status"`
	NamespaceId   string         `json:"namespaceId"`
	EnableLog     bool           `json:"enableLog"`
	LogConfigId   int64          `json:"logConfigId"`
	Version       int32          `json:"version"`
	LogConfigInfo *LogConfigInfo `json:"logConfigInfo"`
}

func NewAppGroupInfo() (rcvr *AppGroupInfo) {
	rcvr = &AppGroupInfo{}
	return
}

func (rcvr *AppGroupInfo) GetAppKey() string {
	return rcvr.AppKey
}

func (rcvr *AppGroupInfo) GetAppName() string {
	return rcvr.AppName
}

func (rcvr *AppGroupInfo) GetEnableLog() bool {
	return rcvr.EnableLog
}

func (rcvr *AppGroupInfo) GetGroupId() string {
	return rcvr.GroupId
}

func (rcvr *AppGroupInfo) GetId() int64 {
	return rcvr.Id
}

func (rcvr *AppGroupInfo) GetLogConfigId() int64 {
	return rcvr.LogConfigId
}

func (rcvr *AppGroupInfo) GetLogConfigInfo() *LogConfigInfo {
	return rcvr.LogConfigInfo
}

func (rcvr *AppGroupInfo) GetMaxJobs() int32 {
	return rcvr.MaxJobs
}

func (rcvr *AppGroupInfo) GetNamespaceId() string {
	return rcvr.NamespaceId
}

func (rcvr *AppGroupInfo) GetStatus() int32 {
	return rcvr.Status
}

func (rcvr *AppGroupInfo) GetVersion() int32 {
	return rcvr.Version
}

func (rcvr *AppGroupInfo) SetAppKey(appKey string) {
	rcvr.AppKey = appKey
}

func (rcvr *AppGroupInfo) SetAppName(appName string) {
	rcvr.AppName = appName
}

func (rcvr *AppGroupInfo) SetEnableLog(enableLog bool) {
	rcvr.EnableLog = enableLog
}

func (rcvr *AppGroupInfo) SetGroupId(groupId string) {
	rcvr.GroupId = groupId
}

func (rcvr *AppGroupInfo) SetId(id int64) {
	rcvr.Id = id
}

func (rcvr *AppGroupInfo) SetLogConfigId(logConfigId int64) {
	rcvr.LogConfigId = logConfigId
}

func (rcvr *AppGroupInfo) SetLogConfigInfo(logConfigInfo *LogConfigInfo) {
	rcvr.LogConfigInfo = logConfigInfo
}

func (rcvr *AppGroupInfo) SetMaxJobs(maxJobs int32) {
	rcvr.MaxJobs = maxJobs
}

func (rcvr *AppGroupInfo) SetNamespaceId(namespaceId string) {
	rcvr.NamespaceId = namespaceId
}

func (rcvr *AppGroupInfo) SetStatus(status int32) {
	rcvr.Status = status
}

func (rcvr *AppGroupInfo) SetVersion(version int32) {
	rcvr.Version = version
}
