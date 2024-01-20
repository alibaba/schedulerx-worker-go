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

type TaskProgressCounter struct {
	Name    string        `json:"name"`
	Total   *atomic.Int64 `json:"total"`
	Pulled  *atomic.Int64 `json:"pulled"`
	Running *atomic.Int64 `json:"running"`
	Success *atomic.Int64 `json:"success"`
	Failed  *atomic.Int64 `json:"failed"`
}

func NewTaskProgressCounter(name string) (c *TaskProgressCounter) {
	return &TaskProgressCounter{
		Total:   atomic.NewInt64(0),
		Pulled:  atomic.NewInt64(0),
		Running: atomic.NewInt64(0),
		Success: atomic.NewInt64(0),
		Failed:  atomic.NewInt64(0),
		Name:    name,
	}
}

func (c *TaskProgressCounter) DecrementFailed() {
	if c.Failed.Load() > 0 {
		c.Failed.Sub(1)
	}
}

func (c *TaskProgressCounter) DecrementRunning() {
	if c.Running.Load() > 0 {
		c.Running.Sub(1)
	}
	c.Pulled.Add(1)
}

func (c *TaskProgressCounter) DecrementSuccess() {
	if c.Success.Load() > 0 {
		c.Success.Sub(1)
	}
}

func (c *TaskProgressCounter) GetFailed() int64 {
	return c.Failed.Load()
}

func (c *TaskProgressCounter) GetName() string {
	return c.Name
}

func (c *TaskProgressCounter) GetPulled() int64 {
	return c.Pulled.Load()
}

func (c *TaskProgressCounter) GetRunning() int64 {
	return c.Running.Load()
}

func (c *TaskProgressCounter) GetSuccess() int64 {
	return c.Success.Load()
}

func (c *TaskProgressCounter) GetTotal() int64 {
	return c.Total.Load()
}

func (c *TaskProgressCounter) IncrementOneFailed() {
	c.IncrementFailed(1)
}

func (c *TaskProgressCounter) IncrementFailed(delta int64) {
	if c.Running.Load() >= delta {
		c.Running.Sub(delta)
	}
	c.Failed.Add(delta)
}

func (c *TaskProgressCounter) IncrementOnePulled() {
	c.IncrementPulled(1)
}

func (c *TaskProgressCounter) IncrementPulled(delta int64) {
	c.Pulled.Add(delta)
}

func (c *TaskProgressCounter) IncrementRunning() {
	if c.Pulled.Load() > 0 {
		c.Pulled.Sub(1)
	}
	c.Running.Add(1)
}

func (c *TaskProgressCounter) IncrementRunning2(delta int64) {
	if c.Pulled.Load() >= delta {
		c.Pulled.Sub(delta)
	}
	c.Running.Add(delta)
}

func (c *TaskProgressCounter) IncrementOneSuccess() {
	if c.Running.Load() > 0 {
		c.Running.Sub(1)
	}
	c.Success.Add(1)
}

func (c *TaskProgressCounter) IncrementSuccess(delta int64) {
	if c.Running.Load() >= delta {
		c.Running.Sub(delta)
	}
	c.Success.Add(delta)
}

func (c *TaskProgressCounter) IncrementOneTotal() {
	c.Total.Add(1)
}

func (c *TaskProgressCounter) IncrementTotal(delta int64) {
	c.Total.Add(delta)
}
