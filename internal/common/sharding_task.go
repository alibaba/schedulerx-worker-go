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

type ShardingTask struct {
	Id        int64  `json:"id"`
	Parameter string `json:"parameter"`
}

func NewShardingTask(id int64, parameter string) *ShardingTask {
	return &ShardingTask{
		Id:        id,
		Parameter: parameter,
	}
}

func (t *ShardingTask) GetId() int64 {
	return t.Id
}

func (t *ShardingTask) SetId(id int64) {
	t.Id = id
}

func (t *ShardingTask) GetParameter() string {
	return t.Parameter
}

func (t *ShardingTask) SetParameter(parameter string) {
	t.Parameter = parameter
}
