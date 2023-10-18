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

package tracer

import (
	"sync"

	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var (
	once   sync.Once
	tracer Tracer
)

type Tracer interface {
	Start(ctx *jobcontext.JobContext) *jobcontext.JobContext
	End(ctx *jobcontext.JobContext, ret *processor.ProcessResult) *processor.ProcessResult
}

func InitTracer(t Tracer) {
	once.Do(func() {
		tracer = t
	})
}

func GetTracer() Tracer {
	return tracer
}
