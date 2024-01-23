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

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

const (
	appGroupIdURL = "/worker/v1/appgroup/getId"
	appGroupURL   = "/worker/v1/appgroup/get"
)

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
	groupId2AppGroupIdMap     sync.Map
	groupId2AppKeyMap         sync.Map
	groupId2ParentAppGroupMap map[string]*common.AppGroupInfo
	parentGroupId2CountMap    map[string]int
	client                    *openapi.Client
	stopCh                    chan struct{}
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

func (g *GroupManager) appendGroupId(groupId, parentGroupId, appKey string) error {
	appGroupId, err := g.getAppGroupId(groupId, appKey)
	if err != nil {
		return fmt.Errorf("groupId=%s is not exist, namespace=%s %w", groupId, g.client.Namespace(), err)
	}
	g.groupId2AppGroupIdMap.Store(groupId, appGroupId)

	// get config of the parent application group
	parentAppGroup, err := g.getAppGroup(groupId, appKey)
	if err != nil {
		return fmt.Errorf("getAppGroup failed, groupId=%s, namespace=%s, err=%s", groupId, g.client.Namespace(), err.Error())
	}
	if parentAppGroup != nil {
		g.groupId2ParentAppGroupMap[groupId] = parentAppGroup
	}
	if _, ok := g.parentGroupId2CountMap[parentGroupId]; ok {
		count := g.parentGroupId2CountMap[parentGroupId]
		g.parentGroupId2CountMap[parentGroupId] = count + 1
	} else {
		g.parentGroupId2CountMap[parentGroupId] = 1
	}
	return nil
}

func (g *GroupManager) getAppGroupId(groupId, appKey string) (int, error) {
	if len(g.client.Domain()) == 0 {
		return 0, errors.New("domain missing")
	}
	var urlStr string
	if len(g.client.Namespace()) > 0 {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&namespace=%s&appKey=%s", g.client.Domain(), appGroupIdURL, groupId, g.client.Namespace(), url.QueryEscape(appKey))
		if len(g.client.NamespaceSource()) > 0 {
			urlStr += "&namespaceSource=" + g.client.NamespaceSource()
		}
	} else {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&appKeys=%s", g.client.Domain(), appGroupIdURL, groupId, appKey)
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
		return 0, fmt.Errorf("request appGroupId failed, groupId:%s, body:%s unmarshal body error:%s", groupId, string(body), err.Error())
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

	err := g.appendGroupId(groupId, groupId, appKey)
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
		groupId2AppGroupIdMap:     sync.Map{},
		groupId2AppKeyMap:         sync.Map{},
		groupId2ParentAppGroupMap: make(map[string]*common.AppGroupInfo),
		parentGroupId2CountMap:    make(map[string]int),
		client:                    client,
		stopCh:                    make(chan struct{}),
	}
	go func() {
		for event := range triggerChan {
			logger.Infof("receive trigger event childGroupId: %s parentGroupId:%s", event.ChildGroupId, event.ParentGroupId)
			instance.StartServerDiscovery(event.ChildGroupId, event.ChildAppKey)
		}
	}()
	return instance
}

func (g *GroupManager) getParentAppGroup(groupId string) *common.AppGroupInfo {
	return g.groupId2ParentAppGroupMap[groupId]
}

func (g *GroupManager) IsAdvancedVersion(groupId string) bool {
	if appGroupInfo := g.getParentAppGroup(groupId); appGroupInfo != nil {
		return appGroupInfo.GetVersion() == int32(constants.Advanced)
	}
	return false
}

func (g *GroupManager) getAppGroup(groupId, appKey string) (*common.AppGroupInfo, error) {
	var (
		urlStr string

		domain          = g.client.Domain()
		namespace       = g.client.Namespace()
		namespaceSource = g.client.NamespaceSource()
	)
	if domain == "" {
		return nil, errors.New("domain missing")
	}

	if namespace != "" {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&namespace=%s&appKey=%s", domain, appGroupURL, groupId, namespace, url.QueryEscape(appKey))
		if namespaceSource != "" {
			urlStr += fmt.Sprintf("&namespaceSource=%s", namespaceSource)
		}
	} else {
		urlStr = fmt.Sprintf("http://%s%s?groupId=%s&appKeys=%s", domain, appGroupURL, groupId, appKey)
	}

	resp, err := g.client.HttpClient().Get(urlStr)
	if err != nil {
		return nil, fmt.Errorf("request appGroup failed, groupId:%s, err:%s", groupId, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request appGroup failed, groupId:%s, status:%d", groupId, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("request appGroup failed, groupId:%s, read body error:%s", groupId, err.Error())
	}
	defer resp.Body.Close()
	var respData struct {
		Success bool                 `json:"success"`
		Message string               `json:"message"`
		Data    *common.AppGroupInfo `json:"data"`
	}
	err = json.Unmarshal(body, &respData)
	if err != nil {
		return nil, fmt.Errorf("request appGroup failed, groupId:%s, body:%s unmarshal body error:%s", groupId, string(body), err.Error())
	}
	if !respData.Success {
		return nil, fmt.Errorf("request appGroup failed, groupId:%s, message:%s", groupId, respData.Message)
	}

	return respData.Data, nil
}
