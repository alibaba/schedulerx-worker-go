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

type Set struct {
	items map[interface{}]struct{}
}

func NewSet() *Set {
	return &Set{items: make(map[interface{}]struct{})}
}

func (s *Set) Add(item interface{}) {
	s.items[item] = struct{}{}
}

func (s *Set) Remove(item interface{}) {
	delete(s.items, item)
}

func (s *Set) Contains(item interface{}) bool {
	_, exists := s.items[item]
	return exists
}

func (s *Set) ToInt64Slice() []int64 {
	result := make([]int64, 0, len(s.items))
	for item := range s.items {
		if i, ok := item.(int64); ok {
			result = append(result, i)
		}
	}
	return result
}
