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

type TimeType int32

const (
	TimeTypeNone        TimeType = -1
	TimeTypeCron        TimeType = 1
	TimeTypeFixedRate   TimeType = 3
	TimeTypeSecondDelay TimeType = 4
	TimeTypeOneTime     TimeType = 5

	TimeTypeAPI  TimeType = 100
	TimeTypeTemp TimeType = 101
)

var (
	TimeTypeDesc = map[TimeType]string{
		TimeTypeNone:        "none",
		TimeTypeCron:        "cron",
		TimeTypeFixedRate:   "fixed_rate",
		TimeTypeSecondDelay: "second_delay",
		TimeTypeOneTime:     "one_time",
		TimeTypeAPI:         "api",
		TimeTypeTemp:        "temp",
	}
	TimeTypeClassName = map[TimeType]string{
		TimeTypeNone:        "",
		TimeTypeCron:        "com.alibaba.schedulerx.core.time.CronParser",
		TimeTypeFixedRate:   "com.alibaba.schedulerx.core.time.FixedRateParser",
		TimeTypeSecondDelay: "com.alibaba.schedulerx.core.time.FixedDelayParser",
		TimeTypeOneTime:     "com.alibaba.schedulerx.core.time.OneTimeParser",
		TimeTypeAPI:         "",
		TimeTypeTemp:        "",
	}
)

func (t TimeType) Descriptor() string {
	return TimeTypeDesc[t]
}

func (t TimeType) AutoNext() bool {
	if t == TimeTypeCron || t == TimeTypeFixedRate || t == TimeTypeSecondDelay {
		return true
	}
	return false
}
