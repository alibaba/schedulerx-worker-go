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
	"strings"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

type TaskDao struct {
	h2 *H2ConnectionPool
}

func NewTaskDao(h2CP *H2ConnectionPool) *TaskDao {
	return &TaskDao{
		h2: h2CP,
	}
}

func (d *TaskDao) CreateTable() error {
	sql := "CREATE TABLE IF NOT EXISTS task (" +
		"job_id unsigned bigint(20) NOT NULL," +
		"job_instance_id unsigned bigint(20) NOT NULL," +
		"task_id unsigned bigint(20) NOT NULL," +
		"task_name varchar(100) NOT NULL DEFAULT ''," +
		"status int(11) NOT NULL," +
		"progress float NOT NULL DEFAULT '0'," +
		"gmt_create datetime NOT NULL," +
		"gmt_modified datetime NOT NULL," +
		"worker_addr varchar(30) NOT NULL DEFAULT ''," +
		"worker_id varchar(30) NOT NULL DEFAULT ''," +
		"task_body blob DEFAULT NULL," +
		"CONSTRAINT uk_instance_and_task UNIQUE (job_instance_id,task_id));" +
		"CREATE INDEX idx_job_instance_id ON task (job_instance_id);" +
		"CREATE INDEX idx_status ON task (status);"

	_, err := d.h2.Exec(sql)
	return err
}

func (d *TaskDao) DropTable() error {
	sql := "DROP TABLE IF EXISTS task"
	_, err := d.h2.Exec(sql)
	return err
}

func (d *TaskDao) BatchDeleteTasks(jobInstanceId int64, taskIds []int64) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)

	err := WithTransaction(ctx, d.h2.DB, func(tx *sql.Tx) error {
		sql := "delete from task where job_instance_id=? and task_id=?"
		stmt, err := tx.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, taskId := range taskIds {
			ret, err := stmt.ExecContext(ctx, jobInstanceId, taskId)
			if err != nil {
				continue
			}
			affectCnt, _ := ret.RowsAffected()
			totalAffectCnt += affectCnt
		}
		return nil
	})
	return totalAffectCnt, err
}

func (d *TaskDao) BatchDeleteTasks2(jobInstanceId int64, workerId string, workerAddr string) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)

	err := WithTransaction(ctx, d.h2.DB, func(tx *sql.Tx) error {
		sql := "delete from task where job_instance_id=? and worker_id=? and worker_addr=?"
		stmt, err := tx.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		ret, err := stmt.ExecContext(ctx, jobInstanceId, workerId, workerAddr)
		if err != nil {
			return err
		}
		affectCnt, _ := ret.RowsAffected()
		totalAffectCnt += affectCnt
		return nil
	})
	return totalAffectCnt, err
}

func (d *TaskDao) BatchInsert(containers []*schedulerx.MasterStartContainerRequest, workerId string, workerAddr string) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)

	err := WithTransaction(ctx, d.h2.DB, func(tx *sql.Tx) error {
		sql := "insert into task(job_id,job_instance_id,task_id,task_name,status,gmt_create,gmt_modified,task_body,worker_id,worker_addr) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		stmt, err := tx.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, snapshot := range containers {
			timeNowMill := time.Now().UnixMilli()
			ret, err := stmt.ExecContext(ctx,
				snapshot.GetJobId(),
				snapshot.GetJobInstanceId(),
				snapshot.GetTaskId(),
				snapshot.GetTaskName(),
				int(taskstatus.TaskStatusPulled),
				timeNowMill,
				timeNowMill,
				snapshot.GetTask(),
				workerId,
				workerAddr)
			if err != nil {
				continue
			}
			affectCnt, _ := ret.RowsAffected()
			totalAffectCnt += affectCnt
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return totalAffectCnt, nil
}

func (d *TaskDao) BatchUpdateStatus(jobInstanceId int64, taskIdList []int64, status int) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)

	err := WithTransaction(ctx, d.h2.DB, func(tx *sql.Tx) error {
		sql := fmt.Sprintf("update task set status=? where job_instance_id=? and task_id in (%s)",
			strings.Join(utils.Int64SliceToStringSlice(taskIdList), ","))

		stmt, err := tx.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()
		ret, err := stmt.ExecContext(ctx, status, jobInstanceId)
		if err != nil {
			return err
		}
		affectCnt, _ := ret.RowsAffected()
		totalAffectCnt += affectCnt
		return nil
	})
	if err != nil {
		return 0, err
	}
	return totalAffectCnt, nil
}

func (d *TaskDao) BatchUpdateStatus2(jobInstanceId int64, status int, workerId string, workerAddr string) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)

	err := WithTransaction(ctx, d.h2.DB, func(tx *sql.Tx) error {
		sqlStr := "update task set status=?,gmt_modified=? where job_instance_id=?"
		if workerId != "" {
			sqlStr = "update task set status=?,gmt_modified=? where job_instance_id=? and worker_id=? and worker_addr=?"
		}
		if status == int(taskstatus.TaskStatusPulled) {
			sqlStr = fmt.Sprintf("%v%v", sqlStr, " and status = 3")
		}

		stmt, err := tx.Prepare(sqlStr)
		if err != nil {
			return err
		}
		defer stmt.Close()
		var ret sql.Result
		if workerId != "" {
			ret, err = stmt.ExecContext(ctx, status, time.Now().UnixMilli(), jobInstanceId, workerId, workerAddr)
		} else {
			ret, err = stmt.ExecContext(ctx, status, time.Now().UnixMilli(), jobInstanceId)
		}
		if err != nil {
			return err
		}

		affectCnt, _ := ret.RowsAffected()
		totalAffectCnt += affectCnt
		return nil
	})
	return totalAffectCnt, err
}

func (d *TaskDao) DeleteByJobInstanceId(jobInstanceId int64) (int64, error) {
	sql := "delete from task where job_instance_id=?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.Exec(jobInstanceId)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *TaskDao) Exist(jobInstanceId int64) (bool, error) {
	sql := "select EXISTS (select * from task where job_instance_id=?)"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	var isExisted bool
	err = stmt.QueryRow(jobInstanceId).Scan(&isExisted)
	return isExisted, err
}

func (d *TaskDao) GetDistinctInstanceIds() ([]int64, error) {
	sql := "select distinct job_instance_id from task"
	rows, err := d.h2.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []int64
	for rows.Next() {
		var instanceId int64
		if err = rows.Scan(&instanceId); err != nil {
			return nil, err
		}
		result = append(result, instanceId)
	}
	err = rows.Err()
	return result, err
}

func (d *TaskDao) GetTaskStatistics() (*common.TaskStatistics, error) {
	var result = new(common.TaskStatistics)

	sql := "select count(distinct job_instance_id) from task"
	var instanceId int64
	err := d.h2.QueryRow(sql).Scan(&instanceId)
	if err != nil {
		return nil, err
	}
	result.SetDistinctInstanceCount(instanceId)

	sql = "select count(*) from task"
	var taskCnt int64
	err = d.h2.QueryRow(sql).Scan(&taskCnt)
	if err != nil {
		return nil, err
	}
	result.SetTaskCount(taskCnt)

	return result, nil
}

func (d *TaskDao) Insert(jobId int64, jobInstanceId int64, taskId int64, taskName string, taskBody []byte) error {
	sql := "insert into task(job_id,job_instance_id,task_id,task_name,status,gmt_create,gmt_modified,task_body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	timeNowMill := time.Now().UnixMilli()
	_, err = stmt.Exec(jobId, jobInstanceId, taskId, taskName, int(taskstatus.TaskStatusPulled), timeNowMill, timeNowMill, taskBody)
	return err
}

func (d *TaskDao) QueryStatus(jobInstanceId int64) ([]int32, error) {
	sql := "select distinct(status) from task where job_instance_id=?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(jobInstanceId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statusList []int32
	for rows.Next() {
		var status int32
		if err = rows.Scan(&status); err != nil {
			return nil, err
		}
		statusList = append(statusList, status)
	}
	err = rows.Err()
	return statusList, err
}

func (d *TaskDao) QueryTaskCount(jobInstanceId int64) (int64, error) {
	sql := "select count(*) from task where job_instance_id=?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var taskCnt int64
	err = stmt.QueryRow(jobInstanceId).Scan(&taskCnt)
	return taskCnt, err
}

func (d *TaskDao) QueryTaskList(jobInstanceId int64, status int, pageSize int32) ([]*TaskSnapshot, error) {
	sql := "select * from task where job_instance_id=? and status=? limit ?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(jobInstanceId, status, pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var taskList []*TaskSnapshot
	for rows.Next() {
		snapshot := new(TaskSnapshot)
		if err = rows.Scan(
			&snapshot.JobId,
			&snapshot.JobInstanceId,
			&snapshot.TaskId,
			&snapshot.TaskName,
			&snapshot.Status,
			&snapshot.Progress,
			&snapshot.GmtCreate,
			&snapshot.GmtModified,
			&snapshot.WorkerAddr,
			&snapshot.WorkerId,
			&snapshot.TaskBody); err != nil {
			return nil, err
		}
		taskList = append(taskList, snapshot)
	}
	err = rows.Err()
	return taskList, err
}

func (d *TaskDao) QueryTasks(jobInstanceId int64, pageSize int32) ([]*TaskSnapshot, error) {
	sql := "select * from task where job_instance_id=? limit ?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(jobInstanceId, pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var taskList []*TaskSnapshot
	for rows.Next() {
		snapshot := new(TaskSnapshot)
		if err = rows.Scan(
			&snapshot.JobId,
			&snapshot.JobInstanceId,
			&snapshot.TaskId,
			&snapshot.TaskName,
			&snapshot.Status,
			&snapshot.Progress,
			&snapshot.GmtCreate,
			&snapshot.GmtModified,
			&snapshot.WorkerAddr,
			&snapshot.WorkerId,
			&snapshot.TaskBody); err != nil {
			return nil, err
		}
		taskList = append(taskList, snapshot)
	}
	err = rows.Err()
	return taskList, err
}

func (d *TaskDao) UpdateStatus(jobInstanceId int64, taskId int64, status int, workerAddr string) (int64, error) {
	sql := "update task set status=?,worker_addr=?,gmt_modified=? where job_instance_id=? and task_id=?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.Exec(status, workerAddr, time.Now().UnixMilli(), jobInstanceId, taskId)
	affectCnt, _ := ret.RowsAffected()
	return affectCnt, err
}

func (d *TaskDao) UpdateStatus2(jobInstanceId int64, taskIds []int64, status int, workerId string, workerAddr string) (int64, error) {
	sql := "update task set status=?, worker_id=?, worker_addr=? WHERE job_instance_id=? and task_id =?"
	if status == int(taskstatus.TaskStatusPulled) {
		sql = fmt.Sprintf("%v%v", sql, " and status = 3")
	}
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var totalAffectCnt int64
	for _, taskId := range taskIds {
		ret, err := stmt.Exec(status, workerId, workerAddr, jobInstanceId, taskId)
		if err != nil {
			continue
		}
		affectCnt, _ := ret.RowsAffected()
		totalAffectCnt += affectCnt
	}
	return totalAffectCnt, err
}

func (d *TaskDao) UpdateWorker(jobInstanceId int64, taskId int64, workerId string, workerAddr string) (int64, error) {
	sql := "update task set worker_id=?,worker_addr=?,gmt_modified=? where job_instance_id=? and task_id=?"
	stmt, err := d.h2.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.Exec(workerId, workerAddr, time.Now().UnixMilli(), jobInstanceId, taskId)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}
