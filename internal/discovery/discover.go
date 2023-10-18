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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/atomic"

	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

const (
	ServiceDiscoverInterval = 5 * time.Second
	ActiveServerQueryURI    = "/worker/v1/appgroup/getLeaderAddr"
)

const GroupHasChild = 300

type ServiceDiscover struct {
	timer        *time.Ticker
	client       *openapi.Client
	activeServer atomic.String
	stopCh       chan struct{}
	changedCh    chan struct{}
}

func NewServiceDiscovery() *ServiceDiscover {
	return &ServiceDiscover{
		timer:     time.NewTicker(ServiceDiscoverInterval),
		client:    openapi.GetOpenAPIClient(),
		stopCh:    make(chan struct{}),
		changedCh: make(chan struct{}, 1),
	}
}

func (s *ServiceDiscover) refreshActiveServer(groupId, appKey string) {
	activeServerAddr, err := s.queryActiveServer(groupId, appKey)
	if err != nil {
		logger.Errorf("query active server from console failed err:%s", err.Error())
		return
	}
	if len(activeServerAddr) > 0 && activeServerAddr != s.activeServer.String() {
		logger.Warnf("[ServerDiscovery]: active server change from [%s] to [%s], groupId=%s, namespace=%s, namespaceSource=%s",
			s.activeServer.String(), activeServerAddr, groupId, s.client.Namespace(), s.client.NamespaceSource())
		s.activeServer.Store(activeServerAddr)
		s.changedCh <- struct{}{}
	}
	logger.Debugf("active server: %s", s.activeServer.String())
}

func (s *ServiceDiscover) Start(groupId, appKey string) {
	for {
		select {
		case <-s.timer.C:
			s.refreshActiveServer(groupId, appKey)
		case <-s.stopCh:
			logger.Infof("receive stop signal")
		}
	}
}

func (s *ServiceDiscover) queryActiveServer(groupId, appKey string) (string, error) {
	url := fmt.Sprintf("http://%s%s?groupId=%s&appKey=%s", s.client.Domain(), ActiveServerQueryURI, groupId, url.QueryEscape(appKey))
	if len(s.client.Namespace()) > 0 {
		url += "&namespace=" + s.client.Namespace()
	}
	if len(s.client.NamespaceSource()) > 0 {
		url += "&namespaceSource=" + s.client.NamespaceSource()
	}
	url += "&enableScale=true"

	resp, err := s.client.HttpClient().Get(url)
	if err != nil {
		return "", fmt.Errorf("http.Get error %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http.Get statusCode %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body error %w", err)
	}
	defer resp.Body.Close()
	var respData struct {
		Success   bool
		RequestId string
		Message   string
		Code      int
		Data      interface{}
	}
	err = json.Unmarshal(body, &respData)
	if err != nil {
		return "", fmt.Errorf("unmarshal resp body[%s] fail %w, url=%s", string(body), err, url)
	}
	if !respData.Success {
		return "", fmt.Errorf("result is not success requestId:%s message:%s, url=%s", respData.RequestId, respData.Message, url)
	}

	if respData.Code != GroupHasChild {
		return respData.Data.(string), nil
	}

	// This application group has enabled automatic scaling and has split child nodes, requiring the parsing of all groupIds, and register serverDiscovery.
	var groupResult struct {
		CurrentLeaderAddr string
		GroupIdMap        map[string]string // key=groupId, val=appKey
	}
	err = json.Unmarshal([]byte(respData.Data.(string)), &groupResult)
	if err != nil {
		return "", fmt.Errorf("unmarshal group result[%s] fail %w, url=%s", respData.Data, err, url)
	}

	for childGroupId, childAppKey := range groupResult.GroupIdMap {
		triggerChan <- &TriggerEvent{
			ChildGroupId:  childGroupId,
			ChildAppKey:   childAppKey,
			ParentGroupId: groupId,
		}
	}
	return groupResult.CurrentLeaderAddr, nil
}

func (s *ServiceDiscover) ActiveServer() string {
	return s.activeServer.String()
}

func (s *ServiceDiscover) Stop(stop <-chan struct{}) {
	<-stop
	s.stopCh <- struct{}{}
	s.timer.Stop()
}

func (s *ServiceDiscover) ResultChangedCh() chan struct{} {
	return s.changedCh
}
