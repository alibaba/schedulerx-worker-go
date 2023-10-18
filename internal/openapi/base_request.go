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

import "time"

type BaseRequest interface {
	setGroupId(groupId string)
	setAppGroupId(appGroupId int64)
	setNamespace(namespace string)
	setNamespaceSource(namespaceSource string)
	setParam(key string, val interface{})
	getParams() map[string]interface{}
}

// BaseRequest SDK request common params
type baseRequest struct {
	// groupId
	groupId    string
	appGroupId int64

	// Request send time
	timeStamp int64

	signature string

	// Operator（default is openapi）
	operator        string
	namespace       string
	namespaceSource string

	parameterMap map[string]interface{}
}

func NewDefaultBaseRequest() BaseRequest {
	return &baseRequest{
		timeStamp:    time.Now().UnixMilli(),
		operator:     "openapi",
		parameterMap: make(map[string]interface{}),
	}
}

func (r *baseRequest) setGroupId(groupId string) {
	r.groupId = groupId
	r.setParam("groupId", groupId)
}

func (r *baseRequest) setAppGroupId(appGroupId int64) {
	r.appGroupId = appGroupId
	r.setParam("appGroupId", appGroupId)
}

func (r *baseRequest) setNamespace(namespace string) {
	r.namespace = namespace
	r.setParam("namespace", namespace)
}

func (r *baseRequest) setNamespaceSource(namespaceSource string) {
	r.namespaceSource = namespaceSource
	r.setParam("namespaceSource", namespaceSource)
}

func (r *baseRequest) setParam(key string, val interface{}) {
	if r.parameterMap != nil {
		r.parameterMap[key] = val
	} else {
		r.parameterMap = map[string]interface{}{
			key: val,
		}
	}
}

func (r *baseRequest) getParams() map[string]interface{} {
	return r.parameterMap
}
