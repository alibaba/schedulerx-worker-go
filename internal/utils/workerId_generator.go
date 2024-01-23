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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	workerId string
	once     sync.Once
)

func GetWorkerId() string {
	once.Do(func() {
		formatIpv4Addr, err := getFormatIpv4Addr()
		if err != nil {
			logger.Warnf("Generate workerId failed due to get format ipv4 addr failed, err=%s" + err.Error())
			return
		}
		pid := strconv.Itoa(os.Getpid())
		timestamp := strconv.Itoa(int(time.Now().UnixMilli() % 100000))
		workerId = strings.Join([]string{formatIpv4Addr, pid, timestamp}, "_")
	})
	return workerId
}
