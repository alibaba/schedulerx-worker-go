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

import (
	"sync"
	"testing"
)

func TestWorkerSnapshotConcurrentReplacement(t *testing.T) {
	info := new(JobInstanceInfo)
	info.SetAllWorkers([]string{"a", "b"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			info.SetAllWorkers([]string{"a", "b"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			got := info.GetAllWorkers()
			if len(got) != 2 {
				t.Error("incomplete worker snapshot")
				return
			}
		}
	}()
	wg.Wait()
}
