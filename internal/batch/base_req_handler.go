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

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"

	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var _ ReqHandler = &BaseReqHandler{}

// BaseReqHandler used for every parallel/Grid job instance
// every parallel/Grid task master has a BaseReqHandler, a BaseReqHandler will
// batch retrieve reqs then merge these reqs into a batch request for hugely reducing network reqs number
type BaseReqHandler struct {
	jobInstanceId           int64
	coreBatchThreadNum      int
	maxBatchThreadNum       int
	batchSize               int32
	batchProcessThreadName  string
	batchRetrieveThreadName string
	reqsQueue               *ReqQueue
	batchRetrieveFunc       func()
	stopBatchRetrieveCh     chan struct{}
	batchProcessSvc         *ants.Pool
	defaultSleepMs          time.Duration
	emptySleepMs            time.Duration
	latestRequest           interface{}
	taskMasterPool          *masterpool.TaskMasterPool

	// onging runnable number, every subclass implement process method should decrement this value when a job was done.
	activeRunnableNum *atomic.Int64
}

func NewBaseReqHandler(jobInstanceId int64, coreBatchThreadNum int, maxBatchThreadNum int, batchSize int32,
	queue *ReqQueue, batchProcessThreadName string, batchRetrieveThreadName string) (rcvr *BaseReqHandler) {
	rcvr = new(BaseReqHandler)
	rcvr.taskMasterPool = masterpool.GetTaskMasterPool()
	rcvr.defaultSleepMs = 500 * time.Millisecond
	rcvr.emptySleepMs = 1000 * time.Millisecond
	rcvr.activeRunnableNum = atomic.NewInt64(0)
	rcvr.jobInstanceId = jobInstanceId
	rcvr.coreBatchThreadNum = coreBatchThreadNum
	if maxBatchThreadNum > coreBatchThreadNum {
		rcvr.maxBatchThreadNum = maxBatchThreadNum
	} else {
		rcvr.maxBatchThreadNum = coreBatchThreadNum
	}
	rcvr.batchSize = batchSize
	rcvr.batchProcessThreadName = batchProcessThreadName
	rcvr.batchRetrieveThreadName = batchRetrieveThreadName
	rcvr.reqsQueue = queue
	return
}

func (rcvr *BaseReqHandler) AsyncHandleReqs(h ReqHandler) []interface{} {
	reqs := rcvr.reqsQueue.RetrieveRequests(rcvr.batchSize)
	if len(reqs) > 0 {
		rcvr.activeRunnableNum.Inc()
		h.Process(rcvr.jobInstanceId, reqs, "")
	}
	return reqs
}

func (rcvr *BaseReqHandler) SyncHandleReqs(h ReqHandler, pageSize int32, workerIdAddr string) []interface{} {
	reqs := rcvr.reqsQueue.RetrieveRequests(pageSize)
	if len(reqs) > 0 {
		rcvr.activeRunnableNum.Inc()
		h.Process(rcvr.jobInstanceId, reqs, workerIdAddr)
		rcvr.activeRunnableNum.Dec()
	}
	return reqs
}

func (rcvr *BaseReqHandler) Clear() {
	if rcvr.reqsQueue != nil {
		rcvr.reqsQueue.Clear()
	}
	rcvr.activeRunnableNum.Store(0)
}

func (rcvr *BaseReqHandler) GetLatestRequest() interface{} {
	return rcvr.latestRequest
}

// IsActive queue has remaining or at least on runnable running, using this method with attention
// because batch process may be async so activeRunnableNum should be decremented when job really down,
func (rcvr *BaseReqHandler) IsActive() bool {
	return rcvr.reqsQueue.Size() != 0 || rcvr.activeRunnableNum.Load() > 0
}

// Process logic implemented by subclass for processing this batch of reqs
// jobInstanceId: id of job instance which these reqs belong to.
// reqs: batch of reqs
// workerIdAddr: workerIdAddr of PullModel
func (rcvr *BaseReqHandler) Process(jobInstanceId int64, reqs []interface{}, workerIdAddr string) {
	// logic implemented by subclass for processing this batch of reqs
}

func (rcvr *BaseReqHandler) SetBatchSize(batchSize int32) {
	rcvr.batchSize = batchSize
}

func (rcvr *BaseReqHandler) SetWorkThreadNum(workThreadNum int) {
	rcvr.coreBatchThreadNum = workThreadNum
	rcvr.maxBatchThreadNum = workThreadNum
}

func (rcvr *BaseReqHandler) Start(h ReqHandler) error {
	gopool, err := ants.NewPool(rcvr.maxBatchThreadNum,
		ants.WithExpiryDuration(30*time.Second),
		ants.WithPanicHandler(func(i interface{}) {
			if r := recover(); r != nil {
				logger.Errorf("Panic happened in BaseReqHandler Start, %v\n%s", r, debug.Stack())
			}
		}))
	if err != nil {
		return fmt.Errorf("New gopool failed, err=%s ", err.Error())
	}
	rcvr.batchProcessSvc = gopool

	rcvr.batchRetrieveFunc = func() {
		for {
			select {
			case <-rcvr.stopBatchRetrieveCh:
				break
			default:
				reqs := rcvr.AsyncHandleReqs(h)
				logger.Debugf("jobInstanceId=%s, batch retrieve reqs, size:%d, remain size:%d, batchSize:%d",
					rcvr.jobInstanceId, len(reqs), len(rcvr.reqsQueue.requests), rcvr.batchSize)
				if int32(len(reqs)) < rcvr.batchSize*4/5 {
					// no element in reqs, sleep a while for aggregation
					time.Sleep(rcvr.emptySleepMs)
				} else {
					// not reach expect batch size, sleep a while for aggregation
					time.Sleep(rcvr.defaultSleepMs)
				}
			}
		}
	}
	go rcvr.batchRetrieveFunc()

	return nil
}

func (rcvr *BaseReqHandler) Stop() {
	if rcvr.batchRetrieveFunc != nil {
		rcvr.stopBatchRetrieveCh <- struct{}{}
	}
	if rcvr.batchProcessSvc != nil {
		rcvr.batchProcessSvc.Release()
	}
	if rcvr.reqsQueue != nil {
		rcvr.reqsQueue.Clear()
	}
}

func (rcvr *BaseReqHandler) SubmitRequest(request interface{}) {
	rcvr.latestRequest = request
	rcvr.reqsQueue.SubmitRequest(request)
}

func (rcvr *BaseReqHandler) GetTaskMasterPool() *masterpool.TaskMasterPool {
	return rcvr.taskMasterPool
}
