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
	"testing"
)

func TestSet(t *testing.T) {
	set := NewSet()

	set.Add(1)
	set.Add(2)
	set.Add(3)

	if !set.Contains(1) || !set.Contains(2) || !set.Contains(3) {
		t.Error("Add element failed")
	}

	set.Remove(1)
	set.Remove(2)

	if set.Contains(1) || set.Contains(2) {
		t.Error("Remove element failed")
	}
}
