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

import "sync"

type ConcurrentSet struct {
	set sync.Map
}

func NewConcurrentSet() *ConcurrentSet {
	return &ConcurrentSet{
		set: sync.Map{},
	}
}

func (s *ConcurrentSet) Add(item interface{}) {
	s.set.Store(item, struct{}{})
}

func (s *ConcurrentSet) Remove(item interface{}) {
	s.set.Delete(item)
}

func (s *ConcurrentSet) Contains(item interface{}) bool {
	_, ok := s.set.Load(item)
	return ok
}

func (s *ConcurrentSet) ToStringSlice() []string {
	slice := make([]string, 0)
	s.set.Range(func(key, value interface{}) bool {
		k := key.(string)
		slice = append(slice, k)
		return true
	})
	return slice
}

func (s *ConcurrentSet) Clear() {
	s.set = sync.Map{}
}

func (s *ConcurrentSet) Len() int {
	return SyncMapLen(&s.set)
}
