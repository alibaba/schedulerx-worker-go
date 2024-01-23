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

type BaseTaskDispatchReqHandler struct {
	*BaseReqHandler
	dispatchSize int
}

func NewBaseTaskDispatchReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32,
	queue *ReqQueue, batchProcessThreadName string, batchRetrieveThreadName string) *BaseTaskDispatchReqHandler {
	rcvr := new(BaseTaskDispatchReqHandler)
	rcvr.BaseReqHandler = NewBaseReqHandler(jobInstanceId, coreBatchThreadNum, maxBatchThreadNum, batchSize, queue, batchProcessThreadName, batchRetrieveThreadName)
	return rcvr
}

func (rcvr *BaseTaskDispatchReqHandler) SetDispatchSize(dispatchSize int) {
	rcvr.dispatchSize = dispatchSize
}
