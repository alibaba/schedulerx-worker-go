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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncMapLen(t *testing.T) {
	var m *sync.Map

	if length := SyncMapLen(m); length != 0 {
		t.Errorf("Expect=0，actual=%d", length)
	}

	m.Store("key1", "value1")
	m.Store("key2", "value2")
	m.Store("key3", "value3")

	if length := SyncMapLen(m); length != 3 {
		t.Errorf("Expect=3，actual=%d", length)
	}

	m.Delete("key2")

	if length := SyncMapLen(m); length != 2 {
		t.Errorf("Expect=2，actual=%d", length)
	}
}

func TestBase64Func(t *testing.T) {
	testData := map[int32]string{
		0:      "$a",
		10:     "$k",
		102:    "$Mb",
		123:    "$7b",
		1000:   "$Op",
		9999:   "$pCc",
		65535:  "$~~p",
		65536:  "$aaq",
		100000: "$GAy",
	}
	for l, expected := range testData {
		result := base64Func(l, "$")
		if result != expected {
			t.Errorf("Base64 coding failed，l=%d，Expect=%s，actual=%s", l, expected, result)
		}
	}
}

func TestGetDeliveryId(t *testing.T) {
	a1 := GetDeliveryId()
	a2 := GetDeliveryId()
	a3 := GetDeliveryId()
	ass := assert.New(t)
	ass.Equal(a1, int64(1))
	ass.Equal(a2, int64(2))
	ass.Equal(a3, int64(3))
}

func TestInt64SliceToStringSlice(t *testing.T) {
	data := []int64{1, 2, 3}
	result := Int64SliceToStringSlice(data)
	if len(result) != 3 || result[0] != "1" || result[1] != "2" || result[2] != "3" {
		t.Errorf("Int64SliceToStringSlice failed，Expect=[1,2,3], actual=%v", result)
	}
}
