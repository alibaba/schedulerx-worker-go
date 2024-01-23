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

package batch

type ReqHandler interface {
	Start(h ReqHandler) error
	Stop()
	Clear()
	IsActive() bool
	GetLatestRequest() interface{}
	SetBatchSize(batchSize int32)
	SetWorkThreadNum(workThreadNum int)
	SubmitRequest(request interface{})
	AsyncHandleReqs(h ReqHandler) []interface{}
	SyncHandleReqs(h ReqHandler, pageSize int32, workerIdAddr string) []interface{}
	Process(jobInstanceId int64, reqs []interface{}, workerIdAddr string)
}

type TaskDispatchReqHandler interface {
	ReqHandler
	SetDispatchSize(dispatchSize int)
}
