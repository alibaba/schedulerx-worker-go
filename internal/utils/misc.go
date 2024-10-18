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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/disk"
	atom "go.uber.org/atomic"

	"github.com/alibaba/schedulerx-worker-go/internal/constants"
)

var (
	seed              = atom.NewInt32(1000)
	uid        uint64 = 0
	deliveryId        = atom.NewInt64(0)
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
	if seed.Load() == math.MaxInt32 {
		seed.Store(0)
	}

	return base64Func(seed.Inc(), pathTplPrefix)
}

func base64Func(l int32, sb string) string {
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
	rand.Seed(time.Now().UnixNano())
	uid = rand.Uint64()
	return uid
}

func GetDeliveryId() int64 {
	return deliveryId.Inc()
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

func GetUserDiskSpacePercent() (float64, error) {
	path := "/"
	if runtime.GOOS == "windows" {
		path = "C:"
	}
	info, err := disk.Usage(path)
	if err != nil {
		return 0, err
	}
	return info.UsedPercent, nil
}

func IsRootTask(taskName string) bool {
	return taskName == constants.MapTaskRootName
}

// PrintCallStack print the latest 5-level call stack for troubleshooting
func PrintCallStack() {
	const size = 5
	var pc [size]uintptr           // Stores the program counter value for each level in the call stack
	n := runtime.Callers(2, pc[:]) // Skip runtime.Callers and PrintStack itself
	if n == 0 {
		// The call stack information is empty and returns directly
		return
	}

	frames := runtime.CallersFrames(pc[:n])
	for i := 0; i < size; i++ {
		frame, more := frames.Next()
		fmt.Printf("%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
}
