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

package processor

import (
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

type Processor interface {
	Process(ctx *jobcontext.JobContext) (*ProcessResult, error)
	Kill(ctx *jobcontext.JobContext) error
}

type BroadcastProcessor interface {
	Processor
	PreProcess(ctx *jobcontext.JobContext) error
	PostProcess(ctx *jobcontext.JobContext) (*ProcessResult, error)
}

type MapJobProcessor interface {
	Processor
	Map(jobCtx *jobcontext.JobContext, taskList []interface{}, taskName string) (*ProcessResult, error)
}

type MapReduceJobProcessor interface {
	MapJobProcessor
	Reduce(jobCtx *jobcontext.JobContext) (*ProcessResult, error)
	RunReduceIfFail(jobCtx *jobcontext.JobContext) bool
}
