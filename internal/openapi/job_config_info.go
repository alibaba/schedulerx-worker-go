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

package openapi

// JobConfigInfo task configuration information
type JobConfigInfo struct {
	JobBaseConfig

	// Task type (modification is not supported)
	jobType string
	// Execute script content (used by script tasks)
	content     string
	contentType int8
	// Execution className
	className string
	// Support oss address acquisition and execution jar
	jarUrl string
	// Parallel, grid task properties
	mapTaskXAttrs MapTaskXAttrs
}
