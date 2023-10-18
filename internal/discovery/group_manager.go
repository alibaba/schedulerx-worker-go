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

package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

const AppGroupURL = "/worker/v1/appgroup/getId"

var (
	once         sync.Once
	groupManager *GroupManager

	triggerChan = make(chan *TriggerEvent, 1000)
)

type TriggerEvent struct {
	ChildGroupId  string
	ChildAppKey   string
	ParentGroupId string
}

type GroupManager struct {
	groupId2AppGroupIdMap sync.Map
	groupId2AppKeyMap     sync.Map
	client                *openapi.Client
	stopCh                chan struct{}
}

func GetGroupManager() *GroupManager {
	once.Do(func() {
		groupManager = newGroupManager(openapi.GetOpenAPIClient())
	})
	return groupManager
}

func (g *GroupManager) GroupId2AppGroupIdMap() map[string]int {
	normalMap := make(map[string]int)
	g.groupId2AppGroupIdMap.Range(func(key, value interface{}) bool {
		k, v := key.(string), value.(int)
		normalMap[k] = v
		return true
	})
	return normalMap
}

func (g *GroupManager) groupExist(groupId string) bool {
	_, ok := g.groupId2AppGroupIdMap.Load(groupId)
	return ok
}

func (g *GroupManager) appendGroupId(groupId, appKey string) error {
	appGroupId, err := g.getAppGroupId(groupId, appKey)
	if err != nil {
		return fmt.Errorf("groupId=%s is not exist, namespace=%s %w", groupId, g.client.Namespace(), err)
	}
	g.groupId2AppGroupIdMap.Store(groupId, appGroupId)
	return nil
}

func (g *GroupManager) getAppGroupId(groupId, appKey string) (int, error) {
	if len(g.client.Domain()) == 0 {
		return 0, errors.New("domain missing")
	}
	var urlStr string
	if len(g.client.Namespace()) > 0 {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&namespace=%s&appKey=%s", g.client.Domain(), AppGroupURL, groupId, g.client.Namespace(), url.QueryEscape(appKey))
		if len(g.client.NamespaceSource()) > 0 {
			urlStr += "&namespaceSource=" + g.client.NamespaceSource()
		}
	} else {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&appKeys=%s", g.client.Domain(), AppGroupURL, groupId, appKey)
	}
	resp, err := g.client.HttpClient().Get(urlStr)
	if err != nil {
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, err:%s", groupId, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, status:%d", groupId, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, read body error:%s", groupId, err.Error())
	}
	defer resp.Body.Close()
	var respData struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Data    string `json:"data"`
	}
	err = json.Unmarshal(body, &respData)
	if err != nil {
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, body:%s unmarshal body error:%s", string(body), groupId, err.Error())
	}
	if !respData.Success {
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, message:%s", groupId, respData.Message)
	}

	ret, err := strconv.Atoi(respData.Data)
	if err != nil {
		return 0, fmt.Errorf("request appGroupId failed, data expect int, but got=%s", respData.Data)
	}
	return ret, nil
}

func (g *GroupManager) putGroupId2AppKeyMap(groupId, appKey string) {
	g.groupId2AppKeyMap.Store(groupId, appKey)
}

func (g *GroupManager) GetAppKeyByGroupId(groupId string) string {
	ret, ok := g.groupId2AppKeyMap.Load(groupId)
	if ok {
		return ret.(string)
	}
	return ""
}

func (g *GroupManager) StartServerDiscovery(groupId, appKey string) {
	if g.groupExist(groupId) {
		return
	}
	discovery := GetDiscovery(groupId)
	discovery.refreshActiveServer(groupId, appKey)

	go discovery.Start(groupId, appKey)
	go discovery.Stop(g.stopCh)

	err := g.appendGroupId(groupId, appKey)
	if err != nil {
		logger.Errorf("appendGroupId error %s", err.Error())
	}
	g.putGroupId2AppKeyMap(groupId, appKey)
}

func (g *GroupManager) Stop() {
	g.stopCh <- struct{}{}
}

func newGroupManager(client *openapi.Client) *GroupManager {
	instance := &GroupManager{
		client: client,
		stopCh: make(chan struct{}),
	}
	go func() {
		for event := range triggerChan {
			logger.Infof("receive trigger event childGroupId: %s parentGroupId:%s", event.ChildGroupId, event.ParentGroupId)
			instance.StartServerDiscovery(event.ChildGroupId, event.ChildAppKey)
		}
	}()
	return instance
}
