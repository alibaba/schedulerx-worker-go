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
	"github.com/alibaba/schedulerx-worker-go"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob"
)

func main() {
	// This is just an example, the real configuration needs to be obtained from the platform
	cfg := &schedulerx.Config{
		Endpoint:  "acm.aliyun.com",
		Namespace: "fa6ed99e-1469-4477-855c-a2bf1659d039",
		GroupId:   "xueren_test_sub",
		AppKey:    "myV5K5Xaf1knuzKdPBaj3A==",
	}
	client, err := schedulerx.GetClient(cfg)
	if err != nil {
		panic(err)
	}

	// The name TestMapReduceJob registered here must be consistent with the configured on the platform
	task := &TestMapReduceJob{
		mapjob.NewMapReduceJobProcessor(), // FIXME how define user behavior
	}
	client.RegisterTask("TestMapReduceJob", task)
	select {}
}
