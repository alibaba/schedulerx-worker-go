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

package utils

import (
	"strings"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
)

func IsMapJobType(executeMode string) bool {
	return strings.HasSuffix(string(common.GridExecuteMode), executeMode) ||
		executeMode == string(common.ParallelExecuteMode) ||
		executeMode == string(common.BatchExecuteMode) ||
		executeMode == string(common.ShardingExecuteMode)
}

func IsSecondTypeJob(timeType common.TimeType) bool {
	return timeType == common.TimeTypeSecondDelay
}
