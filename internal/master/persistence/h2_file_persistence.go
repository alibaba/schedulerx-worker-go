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

import (
	"fmt"
	"sync"

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

var (
	_ TaskPersistence = &H2FilePersistence{}

	h2fOnce           sync.Once
	h2FilePersistence *H2FilePersistence
)

func GetH2FilePersistence() *H2FilePersistence {
	h2fOnce.Do(func() {
		var err error
		h2FilePersistence, err = newH2FilePersistence()
		if err != nil {
			panic("NewH2FilePersistence err=" + err.Error())
		}
	})
	return h2FilePersistence
}

type H2FilePersistence struct {
	*H2Persistence
}

func newH2FilePersistence() (*H2FilePersistence, error) {
	h2CP, err := NewH2ConnectionPool(WithDataSourceName(fmt.Sprintf("schedulerx2_%s_sqlite3.db", utils.GetWorkerId())))
	if err != nil {
		return nil, err
	}

	fp := &H2FilePersistence{
		NewH2Persistence(),
	}
	fp.h2CP = h2CP
	fp.taskDao = NewTaskDao(h2CP)
	return fp, nil
}
