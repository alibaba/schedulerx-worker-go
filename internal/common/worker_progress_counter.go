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

package common

import "go.uber.org/atomic"

type WorkerProgressCounter struct {
	WorkerAddr string        `json:"workerAddr"`
	Total      *atomic.Int64 `json:"total"`
	Pulled     *atomic.Int64 `json:"pulled"`
	Running    *atomic.Int64 `json:"running"`
	Success    *atomic.Int64 `json:"success"`
	Failed     *atomic.Int64 `json:"failed"`
}

func NewWorkerProgressCounter(workerAddr string) (c *WorkerProgressCounter) {
	return &WorkerProgressCounter{
		Total:      atomic.NewInt64(0),
		Pulled:     atomic.NewInt64(0),
		Running:    atomic.NewInt64(0),
		Success:    atomic.NewInt64(0),
		Failed:     atomic.NewInt64(0),
		WorkerAddr: workerAddr,
	}
}

func (c *WorkerProgressCounter) DecrementFailed() {
	if c.Failed.Load() > 0 {
		c.Failed.Sub(1)
	}
}

func (c *WorkerProgressCounter) DecrementRunning(delta int64) {
	if c.Running.Load() >= delta {
		c.Running.Sub(delta)
	}
	if c.Total.Load() >= delta {
		c.Total.Sub(delta)
	}
}

func (c *WorkerProgressCounter) DecrementSuccess() {
	if c.Success.Load() > 0 {
		c.Success.Sub(1)
	}
}

func (c *WorkerProgressCounter) GetFailed() int64 {
	return c.Failed.Load()
}

func (c *WorkerProgressCounter) GetRunning() int64 {
	return c.Running.Load()
}

func (c *WorkerProgressCounter) GetSuccess() int64 {
	return c.Success.Load()
}

func (c *WorkerProgressCounter) GetTotal() int64 {
	return c.Total.Load()
}

func (c *WorkerProgressCounter) GetWorkerAddr() string {
	return c.WorkerAddr
}

func (c *WorkerProgressCounter) IncrementFailed(delta int64) {
	if c.Running.Load() >= delta {
		c.Running.Sub(delta)
	}
	c.Failed.Add(delta)
}

func (c *WorkerProgressCounter) IncrementOneFailed() {
	c.IncrementFailed(1)
}

func (c *WorkerProgressCounter) IncrementPulled() {
	c.Pulled.Add(1)
}

func (c *WorkerProgressCounter) IncrementRunning() {
	if c.Pulled.Load() > 0 {
		c.Pulled.Sub(1)
	}
	c.Running.Add(1)
}

func (c *WorkerProgressCounter) IncrementSuccess() {
	if c.Running.Load() > 0 {
		c.Running.Sub(1)
	}
	c.Success.Add(1)
}

func (c *WorkerProgressCounter) IncrementTotal() {
	c.Total.Add(1)
}
