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

import (
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/constants"
)

type SecondProgressDetail struct {
	TodayBeginTime           string               `json:"todayBeginTime"`
	TodayProgressCounter     *TaskProgressCounter `json:"todayProgressCounter"`
	YesterdayProgressCounter *TaskProgressCounter `json:"yesterdayProgressCounter"`
	RecentProgressHistory    []*ProgressHistory   `json:"recentProgressHistory"`
	RunningProgress          string               `json:"runningProgress"`
	RunningStartTime         int64                `json:"runningStartTime"`
}

func (s *SecondProgressDetail) GetTodayBeginTime() string {
	return s.TodayBeginTime
}

func (s *SecondProgressDetail) SetTodayBeginTime(todayBeginTime string) {
	s.TodayBeginTime = todayBeginTime
}

func (s *SecondProgressDetail) GetTodayProgressCounter() *TaskProgressCounter {
	return s.TodayProgressCounter
}

func (s *SecondProgressDetail) SetTodayProgressCounter(todayProgressCounter *TaskProgressCounter) {
	s.TodayProgressCounter = todayProgressCounter
}

func (s *SecondProgressDetail) GetYesterdayProgressCounter() *TaskProgressCounter {
	return s.YesterdayProgressCounter
}

func (s *SecondProgressDetail) SetYesterdayProgressCounter(yesterdayProgressCounter *TaskProgressCounter) {
	s.YesterdayProgressCounter = yesterdayProgressCounter
}

func (s *SecondProgressDetail) GetRecentProgressHistory() []*ProgressHistory {
	return s.RecentProgressHistory
}

func (s *SecondProgressDetail) SetRecentProgressHistory(recentProgressHistory []*ProgressHistory) {
	s.RecentProgressHistory = recentProgressHistory
}

func (s *SecondProgressDetail) GetRunningProgress() string {
	return s.RunningProgress
}

func (s *SecondProgressDetail) SetRunningProgress(runningProgress string) {
	s.RunningProgress = runningProgress
}

func (s *SecondProgressDetail) GetRunningStartTime() int64 {
	return s.RunningStartTime
}

func (s *SecondProgressDetail) SetRunningStartTime(runningStartTime int64) {
	s.RunningStartTime = runningStartTime
}

func NewSecondProgressDetail() *SecondProgressDetail {
	rcvr := &SecondProgressDetail{}
	rcvr.TodayBeginTime = time.Now().Format(constants.TimeFormat)
	rcvr.TodayProgressCounter = NewTaskProgressCounter(rcvr.TodayBeginTime)
	rcvr.TodayProgressCounter.IncrementRunning()
	rcvr.TodayProgressCounter.IncrementTotal(1)
	return rcvr
}
