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

const (
	// HTTP method
	HTTPGetMethod = "GET"

	// job default conf
	DefaultXattrsPageSize           = 5
	DefaultXattrsConsumerSize       = 5
	DefaultXattrsQueueSize          = 10
	DefaultXattrsDispatcherSize     = 5
	DefaultXattrsGlobalConsumerSize = 1000

	// time expression type
	CronType        = 1   // cron
	FixRateType     = 3   // fixed frequency
	SecondDelayType = 4   // second level delay
	OneTimeType     = 5   // one-time tasks
	APIType         = 100 // api type

	// execute mode
	ExecModeStandalone = "standalone"
	ExecModeBroadcast  = "broadcast"
	ExecModeParallel   = "parallel"
	ExecModeGrid       = "grid"
	ExecModeBatch      = "batch"

	// openAPI special header
	TIME_STAMP_HEADER       = "openapi-timestamp"
	SIGNATURE_HEADER        = "openapi-signature"
	GROUPID_HEADER          = "openapi-groupid"
	NAMESPACE_KEY_HEADER    = "schedulerx-namespace"
	NAMESPACE_SOURCE_HEADER = "schedulerx-namespace-source"
	CREATE_NAMESPACE_HEADER = "openapi-create-namespace"
	CREATE_GROUP_HEADER     = "openapi-create-group"
	LIST_NAMESPACES_HEADER  = "openapi-list-namespaces"
	LIST_GROUPS_HEADER      = "openapi-list-groups"
	ADMIN_USERS             = "app.admin.users"
	ENV                     = "app.env"
	ENV_NETWORK             = "app.env.network"

	// env stage
	envStagePre  = "pre"
	envStageProd = "prod"

	// server domain addr
	serverDomainDaily = "schedulerx2.taobao.net"
	serverDomainPre   = "pre.schedulerx2.alibaba-inc.com"
	serverDomainProd  = "center.schedulerx2.alibaba-inc.com"

	// env key
	envKeyStage = "SIGMA_APP_STAGE"
)
