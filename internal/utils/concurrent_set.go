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

type ConcurrentSet[T comparable] struct {
	m sync.Map
}

func NewConcurrentSet[T comparable]() *ConcurrentSet[T] {
	return &ConcurrentSet[T]{
		m: sync.Map{},
	}
}

func (s *ConcurrentSet[T]) Add(key T) {
	s.m.Store(key, struct{}{})
}

func (s *ConcurrentSet[T]) Remove(key T) {
	s.m.Delete(key)
}

func (s *ConcurrentSet[T]) Contains(key T) bool {
	_, ok := s.m.Load(key)
	return ok
}

func (s *ConcurrentSet[T]) Keys() []T {
	keys := make([]T, 0)
	s.m.Range(func(key, _ any) bool {
		k := key.(T)
		keys = append(keys, k)
		return true
	})
	return keys
}

func (s *ConcurrentSet[T]) Clear() {
	s.m = sync.Map{}
}

func (s *ConcurrentSet[T]) Len() int {
	return SyncMapLen(&s.m)
}
