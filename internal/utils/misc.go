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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/constants"
)

var (
	seed              = 1000
	uid        uint64 = 0
	deliveryId int64  = 0
)

const (
	manifestSplitter = "$"
	pathTplPrefix    = "$"
	base64Chars      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+~"
)

func SyncMapLen(m *sync.Map) int {
	length := 0
	m.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func GetMsgType(manifest string) (string, error) {
	if len(manifest) > 0 && strings.Contains(manifest, manifestSplitter) {
		parts := strings.Split(manifest, manifestSplitter)
		if len(parts) == 2 {
			return parts[1], nil
		}
	}
	return "", fmt.Errorf("Invalid manifest: %s ", manifest)
}

func ShuffleStringSlice(nums []string) []string {
	rand.Seed(time.Now().UnixNano())
	for i := len(nums) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		nums[i], nums[j] = nums[j], nums[i]
	}
	return nums
}

func GenPathTpl() string {
	if seed == math.MaxInt32 {
		seed = 0
	}

	seed++
	return base64Func(seed, pathTplPrefix)
}

func base64Func(l int, sb string) string {
	sb += string(base64Chars[l&63])
	next := l >> 6
	if next == 0 {
		return sb
	}
	return base64Func(next, sb)
}

func GetHandshakeUid() uint64 {
	if uid != 0 {
		return uid
	}
	min := -2147483648
	max := 2147483647
	rand.Seed(time.Now().UnixNano())
	uid = uint64(rand.Intn(max-min+1) + min)
	return uid
}

func GetDeliveryId() int64 {
	return atomic.AddInt64(&deliveryId, 1)
}

func RemoveSliceElem(s []string, elem string) []string {
	result := []string{}
	for _, value := range s {
		if value != elem {
			result = append(result, value)
		}
	}
	return result
}

func Int64SliceToStringSlice(intSlice []int64) []string {
	strSlice := make([]string, len(intSlice))
	for i, v := range intSlice {
		strSlice[i] = strconv.FormatInt(v, 10)
	}
	return strSlice
}

func GetUserDiskSpacePercent() float32 {
	var stat syscall.Statfs_t
	err := syscall.Statfs("/", &stat)
	if err != nil {
		return 0
	}
	totalSpace := stat.Blocks * uint64(stat.Bsize)
	freeSpace := stat.Bfree * uint64(stat.Bsize)
	usedSpace := totalSpace - freeSpace
	usedRatio := float32(usedSpace) / float32(totalSpace)
	return usedRatio
}

func IsRootTask(taskName string) bool {
	return taskName == constants.MapTaskRootName
}
