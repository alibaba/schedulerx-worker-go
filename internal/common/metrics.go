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

// Metrics VM runtime details for check
type Metrics struct {
	// load1 for cpu
	cpuLoad1 float64
	// load5 for cpu
	cpuLoad5 float64
	// cpu available processor number
	cpuProcessors int32

	/**
	 * memory heap usage
	 * e.g. 37.5%
	 */
	heap1Usage float64
	/**
	 * memory heap usage
	 * e.g. 37.5%
	 */
	heap5Usage float64
	/**
	 * current memory heap used
	 * unit MB
	 */
	heap1Used int32
	/**
	 * memory heap max
	 * unit MB
	 */
	heapMax int32

	/**
	 * disk usage
	 * e.g. 37.5%
	 */
	diskUsage float64
	/**
	 * disk used
	 * unit MB
	 */
	diskUsed int32
	/**
	 * disk max
	 * unit MB
	 */
	diskMax int32
}
