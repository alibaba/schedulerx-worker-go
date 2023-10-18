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

type JobInstanceData struct {
	JobName string
	Data    string
}

func (j *JobInstanceData) SetJobName(jobName string) {
	j.JobName = jobName
}

func (j *JobInstanceData) SetData(data string) {
	j.Data = data
}

func (j JobInstanceData) GetJobName() string {
	return j.JobName
}

func (j JobInstanceData) GetData() string {
	return j.Data
}
