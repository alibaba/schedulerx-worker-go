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

package taskstatus

type TypeInfo struct {
	code string
	name string
	tips map[string]string
}

func NewTypeInfo(code, name string) *TypeInfo {
	return &TypeInfo{
		code: code,
		name: name,
	}
}

func (t *TypeInfo) Code() string {
	return t.code
}

func (t *TypeInfo) SetCode(code string) {
	t.code = code
}

func (t *TypeInfo) Name() string {
	return t.name
}

func (t *TypeInfo) SetName(name string) {
	t.name = name
}

func (t *TypeInfo) Tips() map[string]string {
	return t.tips
}

func (t *TypeInfo) SetTips(tips map[string]string) {
	t.tips = tips
}
