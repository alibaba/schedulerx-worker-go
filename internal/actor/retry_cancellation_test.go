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

package actor

import (
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"google.golang.org/protobuf/proto"
)

type baselineBackoffProcessor struct{ *blockingProcessor }

func (p *baselineBackoffProcessor) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	if ctx.TaskId() == 1 {
		p.started <- 1
		return processor.NewProcessResult(processor.WithFailed()), nil
	}
	return p.blockingProcessor.Process(ctx)
}
func TestBaselineAuditKillWakesSDKRetryBackoff(t *testing.T) {
	h := newPushHarness(t)
	tasks.GetTaskMap().Register(t.Name(), &baselineBackoffProcessor{h.processor})
	first := h.request(1, "parallel", proto.Int32(1))
	first.MaxAttempt = proto.Int32(1)
	first.TaskAttemptInterval = proto.Int32(1)
	h.send(first)
	h.started()
	h.push(2, "parallel", proto.Int32(1))
	h.send(&schedulerx.MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(1)})
	select {
	case id := <-h.processor.started:
		if id != 2 {
			t.Fatalf("unexpected task %d", id)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("canceled SDK retry backoff still owns the only consumer slot")
	}
}

func TestRetiredGenerationStopsSDKRetryBackoff(t *testing.T) {
	for _, action := range []string{"new-attempt", "destroy-then-serial"} {
		t.Run(action, func(t *testing.T) {
			h := newPushHarness(t)
			tasks.GetTaskMap().Register(t.Name(), &baselineBackoffProcessor{h.processor})
			first := h.request(1, "parallel", proto.Int32(1))
			first.MaxAttempt = proto.Int32(1)
			first.TaskAttemptInterval = proto.Int32(1)
			h.send(first)
			h.started()
			next := h.request(2, "parallel", proto.Int32(1))
			if action == "new-attempt" {
				next.Attempt = proto.Int32(1)
			} else {
				h.send(&schedulerx.MasterDestroyContainerPoolRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001)})
				next.SerialNum = proto.Int64(1)
			}
			h.send(next)
			select {
			case id := <-h.processor.started:
				if id != 2 {
					t.Fatalf("retired retry executed: %d", id)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatal("retired SDK retry wait blocks the current generation")
			}
		})
	}
}
