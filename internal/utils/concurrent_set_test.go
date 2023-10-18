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
)

func TestConcurrentSet(t *testing.T) {
	set := NewConcurrentSet()

	// Test add
	set.Add(1)
	set.Add(2)
	set.Add(3)

	if !set.Contains(1) || !set.Contains(2) || !set.Contains(3) {
		t.Error("Add element failed")
	}

	// Test remove
	set.Remove(1)
	set.Remove(2)

	if set.Contains(1) || set.Contains(2) {
		t.Error("Remove element failed")
	}

	// Test concurrency safety
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			set.Add(i)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			set.Contains(i)
		}
	}()

	wg.Wait()
}

func TestToStringSlice(t *testing.T) {
	chs := &ConcurrentSet{}

	// Test add
	chs.Add("hello")
	chs.Add("world")

	slice := chs.ToStringSlice()

	if len(slice) != 2 {
		t.Fatalf("Expected length of 2, but got %d", len(slice))
	}

	// Check whether a slice contains an added item
	foundHello := false
	foundWorld := false

	for _, item := range slice {
		if item == "hello" {
			foundHello = true
		} else if item == "world" {
			foundWorld = true
		} else {
			t.Fatalf("Unexpected item found: %v", item)
		}
	}

	if !foundHello {
		t.Fatal("Item 'hello' not found in the slice")
	}

	if !foundWorld {
		t.Fatal("Item 'world' not found in the slice")
	}
}
