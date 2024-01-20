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

package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ processor.MapReduceJobProcessor = &TestMapReduceJob{}

type OrderInfo struct {
	Id    string `json:"id"`
	Value int    `json:"value"`
}

func NewOrderInfo(id string, value int) *OrderInfo {
	return &OrderInfo{Id: id, Value: value}
}

type TestMapReduceJob struct {
	*mapjob.MapReduceJobProcessor
}

func (mr *TestMapReduceJob) Kill(jobCtx *jobcontext.JobContext) error {
	//TODO implement me
	panic("implement me")
}

// Process the MapReduce model is used to distributed scan orders for timeout confirmation
func (mr *TestMapReduceJob) Process(jobCtx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	var (
		num = 100 * 10000
		err error
	)
	taskName := jobCtx.TaskName()

	if jobCtx.JobParameters() != "" {
		num, err = strconv.Atoi(jobCtx.JobParameters())
		if err != nil {
			return nil, err
		}
	}

	if mr.IsRootTask(jobCtx) {
		fmt.Println("start root task")
		var orderInfos []interface{}
		for i := 1; i <= num; i++ {
			orderInfos = append(orderInfos, NewOrderInfo(fmt.Sprintf("id_%d", i), i))
		}
		return mr.Map(jobCtx, orderInfos, "OrderInfo")
	} else if taskName == "OrderInfo" {
		orderInfo := new(OrderInfo)
		if err := json.Unmarshal(jobCtx.Task(), orderInfo); err != nil {
			fmt.Printf("task is not OrderInfo, task=%+v\n", jobCtx.Task())
		}
		fmt.Printf("orderInfo=%+v\n", orderInfo)
		time.Sleep(1 * time.Second)
		fmt.Println("Finish Process...")
		return processor.NewProcessResult(
			processor.WithSucceed(),
			processor.WithResult(strconv.Itoa(orderInfo.Value)),
		), nil
	}
	return processor.NewProcessResult(processor.WithFailed()), nil
}

func (mr *TestMapReduceJob) Reduce(jobCtx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	allTaskResults := jobCtx.TaskResults()
	allTaskStatuses := jobCtx.TaskStatuses()
	count := 0
	fmt.Printf("reduce: all task count=%d\n", len(allTaskResults))
	for key, val := range allTaskResults {
		if key == 0 {
			continue
		}
		if allTaskStatuses[key] == taskstatus.TaskStatusSucceed {
			num, err := strconv.Atoi(val)
			if err != nil {
				return nil, err
			}
			count += num
		}
	}
	fmt.Printf("reduce: succeed task count=%d\n", count)
	return processor.NewProcessResult(
		processor.WithSucceed(),
		processor.WithResult(strconv.Itoa(count)),
	), nil
}
