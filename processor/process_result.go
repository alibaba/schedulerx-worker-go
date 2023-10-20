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

type ProcessResult struct {
	status InstanceStatus
	result string
}

type Option func(*ProcessResult)

func WithResult(result string) Option {
	return func(pr *ProcessResult) {
		pr.result = result
	}
}

func WithStatus(status InstanceStatus) Option {
	return func(pr *ProcessResult) {
		pr.status = status
	}
}

func WithIsSucceed(isSucceed bool) Option {
	return func(pr *ProcessResult) {
		if isSucceed {
			pr.status = InstanceStatusSucceed
		} else {
			pr.status = InstanceStatusFailed
		}
	}
}

func NewProcessResult(opts ...Option) *ProcessResult {
	pr := new(ProcessResult)
	for _, opt := range opts {
		opt(pr)
	}
	return pr
}

func (pr *ProcessResult) GetStatus() InstanceStatus {
	return pr.status
}

func (pr *ProcessResult) SetStatus(status InstanceStatus) {
	pr.status = status
}

func (pr *ProcessResult) GetResult() string {
	return pr.result
}

func (pr *ProcessResult) SetResult(result string) {
	pr.result = result
}

func (pr *ProcessResult) String() string {
	return "ProcessResult [status=" + pr.status.Descriptor() + ", result=" + pr.result + "]"
}
