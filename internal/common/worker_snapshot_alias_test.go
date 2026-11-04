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

import "testing"

func TestWorkersSnapshotCannotAliasInputOrOutput(t *testing.T) {
	info := NewJobInstanceInfo()
	input := []string{"a@127.0.0.1:1", "b@127.0.0.1:2"}
	info.SetAllWorkers(input)
	input[0] = "changed-input"
	first := info.GetAllWorkers()
	if first[0] != "a@127.0.0.1:1" {
		t.Fatal("SetAllWorkers retained input backing array")
	}
	first[1] = "changed-output"
	second := info.GetAllWorkers()
	if second[1] != "b@127.0.0.1:2" {
		t.Fatal("GetAllWorkers exposed mutable backing array")
	}
	info.SetAllWorkers([]string{"c@127.0.0.1:3"})
	if len(first) != 2 || first[0] != "a@127.0.0.1:1" {
		t.Fatal("replacement changed held snapshot")
	}
	info.SetAllWorkers(nil)
	if len(info.GetAllWorkers()) != 0 {
		t.Fatal("clearing workers failed")
	}
}
