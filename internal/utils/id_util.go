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
	"fmt"
	"strconv"
	"strings"
)

type IdType int32

const (
	IdTypeJobId         IdType = 0
	IdTypeJobInstanceId IdType = 1
	IdTypeTaskId        IdType = 2

	SplitterToken = "_"
)

func ParseId(uniqueId string, idType IdType) (int64, error) {
	tokens := strings.Split(uniqueId, SplitterToken)
	switch idType {
	case IdTypeJobId:
		return strconv.ParseInt(tokens[0], 10, 64)
	case IdTypeJobInstanceId:
		return strconv.ParseInt(tokens[1], 10, 64)
	case IdTypeTaskId:
		return strconv.ParseInt(tokens[2], 10, 64)
	}
	return -1, fmt.Errorf("Invalid idType: %d ", idType)
}

func GetUniqueId(jobId, jobInstanceId, taskId int64) string {
	ids := []string{
		strconv.FormatInt(jobId, 10),
		strconv.FormatInt(jobInstanceId, 10),
		strconv.FormatInt(taskId, 10),
	}
	return strings.Join(ids, SplitterToken)
}

func GetUniqueIdWithoutTaskId(jobId, jobInstanceId int64) string {
	ids := []string{
		strconv.FormatInt(jobId, 10),
		strconv.FormatInt(jobInstanceId, 10),
	}
	return strings.Join(ids, SplitterToken)
}
