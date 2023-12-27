/*
 *
 *  * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package discovery

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
)

func TestNewGroupManager(t *testing.T) {
	var (
		groupId = "133144"
		appKey  = "123123"
	)
	client := openapi.NewClient(
		openapi.WithHTTPClient(*http.DefaultClient),
		openapi.WithNamespace("d3f7df06-66a2-4331-b90c-89023b2e2413"),
		openapi.WithGroupId(groupId),
		openapi.WithAppKey(appKey),
	)

	ins := newGroupManager(client)
	ins.StartServerDiscovery(groupId, appKey)

	timer := time.NewTicker(time.Second)
	for range timer.C {
		fmt.Println(GetDiscovery(groupId).ActiveServer())
	}
}
