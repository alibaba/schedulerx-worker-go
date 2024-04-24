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

package master

import (
	"fmt"

	"github.com/asynkron/protoactor-go/actor"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

var _ taskmaster.MapTaskMaster = &BatchTaskMaster{}

type BatchTaskMaster struct {
	*GridTaskMaster
}

func NewBatchTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) *BatchTaskMaster {
	batchTaskMaster := &BatchTaskMaster{
		NewGridTaskMaster(jobInstanceInfo, actorCtx),
	}
	batchTaskMaster.taskPersistence = persistence.GetH2FilePersistence()
	batchTaskMaster.taskPersistence.InitTable()

	return batchTaskMaster
}

// doMetricsCheck check indicators of the master
func (m *BatchTaskMaster) doMetricsCheck() error {
	diskUsedPercent, err := utils.GetUserDiskSpacePercent()
	if err != nil {
		return err
	}
	if diskUsedPercent > constants.UserSpacePercentMax {
		return fmt.Errorf("used space beyond %f%% ", constants.UserSpacePercentMax*100)
	}
	return nil
}
