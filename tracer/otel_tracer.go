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
	"context"

	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

// instrumentationName 是 SDK 内置埋点的 instrumentation 名称。
const instrumentationName = "github.com/alibaba/schedulerx-worker-go"

// defaultTracer 是内置的 OpenTelemetry Tracer 实现。
//
// 使用 instgo（ARMS）编译时，otel 全局 TracerProvider 由探针注入，span 会自动上报；
// 未接入任何 OTel SDK 时，全局 TracerProvider 为 no-op 实现，Start/End 开销可忽略，
// 不会影响既有行为。
type defaultTracer struct{}

// Default 返回内置的默认 Tracer（基于 OpenTelemetry API）。
func Default() Tracer {
	return &defaultTracer{}
}

// Start 以任务粒度开启 span，并把携带 span 的 ctx 写回 jobCtx.Context，
// 使业务回调内的日志与下游调用（DB/HTTP 等）自动共用同一 traceId。
func (t *defaultTracer) Start(jobCtx *jobcontext.JobContext) *jobcontext.JobContext {
	if jobCtx.Context == nil {
		jobCtx.Context = context.Background()
	}
	ctx, _ := otel.Tracer(instrumentationName).Start(jobCtx.Context, spanName(jobCtx),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("schedulerx.job_id", jobCtx.JobId()),
			attribute.Int64("schedulerx.job_instance_id", jobCtx.JobInstanceId()),
			attribute.Int64("schedulerx.task_id", jobCtx.TaskId()),
			attribute.Int64("schedulerx.attempt", int64(jobCtx.Attempt())),
			attribute.String("schedulerx.group_id", jobCtx.GroupId()),
			attribute.String("schedulerx.job_name", jobCtx.JobName()),
		),
	)
	jobCtx.Context = ctx
	return jobCtx
}

// End 根据处理结果回填 span 状态并结束 span。
func (t *defaultTracer) End(jobCtx *jobcontext.JobContext, ret *processor.ProcessResult) *processor.ProcessResult {
	span := trace.SpanFromContext(jobCtx.Context)
	if ret != nil && ret.Status() == processor.InstanceStatusFailed {
		span.SetStatus(codes.Error, ret.Result())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
	return ret
}

// spanName 与 worker 侧任务查找逻辑保持一致：优先取 content.jobName，java 任务取 className。
func spanName(jobCtx *jobcontext.JobContext) string {
	jobName := gjson.Get(jobCtx.Content(), "jobName").String()
	if jobCtx.JobType() == "java" {
		jobName = gjson.Get(jobCtx.Content(), "className").String()
	}
	if jobName == "" {
		jobName = jobCtx.TaskName()
	}
	if jobName == "" {
		jobName = "unknown"
	}
	return "SchedulerX/" + jobName
}
