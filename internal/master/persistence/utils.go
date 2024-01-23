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
	"context"
	"database/sql"
	"fmt"
	"runtime"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

func LogDaoErr(ctx context.Context, err *error, params ...interface{}) {
	if *err != nil {
		funcName := ""
		pc, _, _, ok := runtime.Caller(1)
		if ok {
			funcName = runtime.FuncForPC(pc).Name()
		}

		logger.Errorf("DaoError,func:%v,params:%v,err:%+v,", funcName, params, *err)
	}
}

// SQLTxFunc is a function that will be called with an initialized 'DbTx' object
// that can be used for executing statements and queries against a database.
type SQLTxFunc func(tx *sql.Tx) error

// WithTransaction creates a new transaction and handles rollback/commit based on the
// error object returned by the 'SQLTxFunc'
func WithTransaction(ctx context.Context, db *sql.DB, fn SQLTxFunc) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Errorf("db begin error.%v", err)
		return
	}

	defer func() {
		if p := recover(); p != nil {
			// a panic occurred, rollback and repanic
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logger.Errorf("rollback error.%v", rollbackErr)
			}
			panic(p)
		} else if err != nil {
			// something went wrong, rollback
			logger.Errorf("fn error.%v", err)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logger.Errorf("rollback error.%v", rollbackErr)
			}
		} else {
			// all good, commit
			err = tx.Commit()
			if err != nil {
				logger.Errorf("commit error.%v", err)
			}
		}
	}()

	err = fn(tx)
	return err
}

// GetTaskStatusMap get task Status classfied by Status, workerIdAddr
// {Status -> {workerIdAddr -> list of taskIds}}
func getTaskStatusMap(taskStatusInfos []*schedulerx.ContainerReportTaskStatusRequest) map[int32]map[string][]int64 {
	status2WorkIdAddr2TaskIds := make(map[int32]map[string][]int64)
	for _, e := range taskStatusInfos {
		status := e.GetStatus()
		taskId := e.GetTaskId()
		workerIdAddr := fmt.Sprintf("%s@%s", e.GetWorkerId(), e.GetWorkerAddr())
		addTaskStatusInfo(status2WorkIdAddr2TaskIds, status, workerIdAddr, taskId)
	}
	return status2WorkIdAddr2TaskIds
}

func addTaskStatusInfo(status2WorkIdAddr2TaskIds map[int32]map[string][]int64, status int32, workerIdAddr string, taskId int64) {
	if workerIdAddr2TaskIds, ok := status2WorkIdAddr2TaskIds[status]; !ok {
		// Status not exists , all below must be first time add in too
		workerAddr2TaskIds := make(map[string][]int64)
		workerAddr2TaskIds[workerIdAddr] = []int64{taskId}
		status2WorkIdAddr2TaskIds[status] = workerAddr2TaskIds
	} else {
		// Status already exists
		if _, ok := workerIdAddr2TaskIds[workerIdAddr]; !ok {
			workerIdAddr2TaskIds[workerIdAddr] = []int64{taskId}
		} else {
			workerIdAddr2TaskIds[workerIdAddr] = append(workerIdAddr2TaskIds[workerIdAddr], taskId)
		}
	}
}
