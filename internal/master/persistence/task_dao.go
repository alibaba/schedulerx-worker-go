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
	sql1 := "CREATE TABLE IF NOT EXISTS task (" +
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

	stmt, err := d.h2.DB.Prepare(sql1)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (d *TaskDao) DropTable() error {
	sql := "DROP TABLE IF EXISTS task"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
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
	if err != nil {
		return 0, err
	}

	return totalAffectCnt, nil
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
	if err != nil {
		return 0, err
	}

	return totalAffectCnt, nil
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
	if err != nil {
		return 0, err
	}

	return totalAffectCnt, nil
}

func (d *TaskDao) DeleteByJobInstanceId(jobInstanceId int64) (int64, error) {
	var (
		totalAffectCnt int64
		ctx            = context.Background()
	)
	sql := "delete from task where job_instance_id=?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.ExecContext(ctx, jobInstanceId)
	affectCnt, _ := ret.RowsAffected()
	totalAffectCnt += affectCnt
	return totalAffectCnt, err
}

func (d *TaskDao) Exist(jobInstanceId int64) (bool, error) {
	ctx := context.Background()
	sql := "select EXISTS (select * from task where job_instance_id=?)"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(ctx, jobInstanceId)
	if err := row.Err(); err != nil {
		return false, err
	}
	var isExisted bool
	if err := stmt.QueryRowContext(ctx, jobInstanceId).Scan(&isExisted); err != nil {
		return false, err
	}
	return isExisted, nil
}

func (d *TaskDao) GetDistinctInstanceIds() ([]int64, error) {
	var (
		ctx    = context.Background()
		result []int64
	)
	sql := "select distinct job_instance_id from task"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var instanceId int64
		if err = rows.Scan(&instanceId); err != nil {
			return nil, err
		}
		result = append(result, instanceId)
	}
	return result, nil
}

func (d *TaskDao) GetTaskStatistics() (*common.TaskStatistics, error) {
	var (
		ctx    = context.Background()
		result = new(common.TaskStatistics)
	)
	sql := "select count(distinct job_instance_id) from task"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var instanceId int64
		if err = rows.Scan(&instanceId); err != nil {
			return nil, err
		}
		result.SetDistinctInstanceCount(instanceId)
	}

	sql = "select count(*) from task"
	stmt, err = d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var taskCnt int64
		if err = rows.Scan(&taskCnt); err != nil {
			return nil, err
		}
		result.SetTaskCount(taskCnt)
	}
	return result, nil
}

func (d *TaskDao) Insert(jobId int64, jobInstanceId int64, taskId int64, taskName string, taskBody []byte) error {
	ctx := context.Background()
	sql := "insert into task(job_id,job_instance_id,task_id,task_name,status,gmt_create,gmt_modified,task_body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	timeNowMill := time.Now().UnixMilli()
	_, err = stmt.ExecContext(ctx, jobId, jobInstanceId, taskId, taskName, int(taskstatus.TaskStatusPulled), timeNowMill, timeNowMill, taskBody)
	if err != nil {
		return err
	}
	return nil
}

func (d *TaskDao) QueryStatus(jobInstanceId int64) ([]int32, error) {
	var (
		ctx        = context.Background()
		statusList []int32
	)
	sql := "select distinct(status) from task where job_instance_id=?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, jobInstanceId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var status int32
		if err = rows.Scan(&status); err != nil {
			return nil, err
		}
		statusList = append(statusList, status)
	}
	return statusList, nil
}

func (d *TaskDao) QueryTaskCount(jobInstanceId int64) (int64, error) {
	var (
		ctx     = context.Background()
		taskCnt int64
	)
	sql := "select count(*) from task where job_instance_id=?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	row := stmt.QueryRowContext(ctx, jobInstanceId)
	if row.Err() != nil {
		return 0, err
	}
	if err = row.Scan(&taskCnt); err != nil {
		return 0, err
	}
	return taskCnt, nil
}

func (d *TaskDao) QueryTaskList(jobInstanceId int64, status int, pageSize int32) ([]*TaskSnapshot, error) {
	var (
		ctx      = context.Background()
		taskList []*TaskSnapshot
	)
	sql := "select * from task where job_instance_id=? and status=? limit ?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, jobInstanceId, status, pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	return taskList, nil
}

func (d *TaskDao) QueryTasks(jobInstanceId int64, pageSize int32) ([]*TaskSnapshot, error) {
	var (
		ctx      = context.Background()
		taskList []*TaskSnapshot
	)
	sql := "select * from task where job_instance_id=? limit ?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, jobInstanceId, pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	return taskList, nil
}

func (d *TaskDao) UpdateStatus(jobInstanceId int64, taskId int64, status int, workerAddr string) (int64, error) {
	ctx := context.Background()
	sql := "update task set status=?,worker_addr=?,gmt_modified=? where job_instance_id=? and task_id=?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.ExecContext(ctx, status, workerAddr, time.Now().UnixMilli(), jobInstanceId, taskId)
	affectCnt, _ := ret.RowsAffected()
	return affectCnt, err
}

func (d *TaskDao) UpdateStatus2(jobInstanceId int64, taskIds []int64, status int, workerId string, workerAddr string) (int64, error) {
	var (
		ctx            = context.Background()
		totalAffectCnt int64
	)
	sql := "update task set status=?, worker_id=?, worker_addr=? WHERE job_instance_id=? and task_id =?"
	if status == int(taskstatus.TaskStatusPulled) {
		sql = fmt.Sprintf("%v%v", sql, " and status = 3")
	}
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, taskId := range taskIds {
		ret, err := stmt.ExecContext(ctx, status, workerId, workerAddr, jobInstanceId, taskId)
		if err != nil {
			continue
		}
		affectCnt, _ := ret.RowsAffected()
		totalAffectCnt += affectCnt
	}
	return totalAffectCnt, err
}

func (d *TaskDao) UpdateWorker(jobInstanceId int64, taskId int64, workerId string, workerAddr string) (int64, error) {
	ctx := context.Background()
	sql := "update task set worker_id=?,worker_addr=?,gmt_modified=? where job_instance_id=? and task_id=?"
	stmt, err := d.h2.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	ret, err := stmt.ExecContext(ctx, workerId, workerAddr, time.Now().UnixMilli(), jobInstanceId, taskId)
	affectCnt, _ := ret.RowsAffected()
	return affectCnt, err
}
