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

package persistence

import "sync"

var (
	_ TaskPersistence = &H2MemoryPersistence{}

	h2mOnce             sync.Once
	h2MemoryPersistence *H2MemoryPersistence
)

func GetH2MemoryPersistence() *H2MemoryPersistence {
	var err error
	h2mOnce.Do(func() {
		h2MemoryPersistence, err = newH2MemoryPersistence()
		if err != nil {
			panic("NewH2MemoryPersistence err=" + err.Error())
		}
	})
	return h2MemoryPersistence
}

// H2MemoryPersistence is a singleton, so that only executes initTable on the first initialization
type H2MemoryPersistence struct {
	*H2Persistence
}

func newH2MemoryPersistence() (*H2MemoryPersistence, error) {
	h2CP, err := NewH2ConnectionPool(WithDataSourceName(memoryModeDataSourceName))
	if err != nil {
		return nil, err
	}
	fp := &H2MemoryPersistence{
		NewH2Persistence(),
	}
	fp.h2CP = h2CP
	fp.taskDao = NewTaskDao(h2CP)
	return fp, nil
}
