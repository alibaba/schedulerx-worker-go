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
	"fmt"
	"math/rand"
	"strconv"

	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ processor.BroadcastProcessor = &TestBroadcast{}

type TestBroadcast struct{}

// Process all machines would execute it.
func (t *TestBroadcast) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	value := rand.Intn(10)
	fmt.Printf("Total sharding num=%d, sharding=%d, value=%d\n", ctx.ShardingNum(), ctx.ShardingId(), value)
	ret := new(processor.ProcessResult)
	ret.SetSucceed()
	ret.SetResult(strconv.Itoa(value))
	return ret, nil
}

// PreProcess only one machine will execute it.
func (t *TestBroadcast) PreProcess(ctx *jobcontext.JobContext) error {
	fmt.Println("TestBroadcastJob PreProcess")
	return nil
}

// PostProcess only one machine will execute it.
func (t *TestBroadcast) PostProcess(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	fmt.Println("TestBroadcastJob PostProcess")
	allTaskResults := ctx.TaskResults()
	allTaskStatuses := ctx.TaskStatuses()
	num := 0

	for key, val := range allTaskResults {
		fmt.Printf("%v:%v\n", key, val)
		if allTaskStatuses[key] == taskstatus.TaskStatusSucceed {
			valInt, _ := strconv.Atoi(val)
			num += valInt
		}
	}

	fmt.Printf("TestBroadcastJob PostProcess(), num=%d\n", num)
	ret := new(processor.ProcessResult)
	ret.SetSucceed()
	ret.SetResult(strconv.Itoa(num))
	return ret, nil
}

func (t *TestBroadcast) Kill(ctx *jobcontext.JobContext) error {
	fmt.Println("[Kill] Start kill my task: TestBroadcast")
	return nil
}
