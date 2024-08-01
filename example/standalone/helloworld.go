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
	"time"

	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var _ processor.Processor = &HelloWorld{}

type HelloWorld struct{}

var Stop = false

func (h *HelloWorld) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	fmt.Println("[Process] Start process my task: Hello world!")
	// mock execute task
	for i := 0; i < 10; i++ {
		fmt.Printf("Hello%d\n", i)
		time.Sleep(2 * time.Second)
		if Stop {
			break
		}
	}
	ret := new(processor.ProcessResult)
	ret.SetSucceed()
	fmt.Println("[Process] End process my task: Hello world!")
	return ret, nil
}

func (h *HelloWorld) Kill(ctx *jobcontext.JobContext) error {
	fmt.Println("[Kill] Start kill my task: Hello world!")
	Stop = true
	return nil
}
