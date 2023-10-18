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

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	client *Client
	once   sync.Once
)

func InitOpenAPIClient(cli *Client) {
	once.Do(func() {
		client = cli

		client.SetDomain()
	})
}

// GetOpenAPIClient first executes InitOpenAPIClient and then calls it, otherwise it returns nil
func GetOpenAPIClient() *Client {
	return client
}

type Option func(*Client)

type Client struct {
	// Schedulerx application management interface to view user identity verification (required)
	appKey string

	// View the groupId application management interface (required)
	groupId string

	// Send to the corresponding domain name (optional).
	// By default, it is filled in according to the environment where the machine is located.
	// Daily、Pre、Online
	domain string

	// endpoint only exited at Alibaba Cloud, using it to get domain
	endpoint string

	// Operator's convenience record query (optional)
	user string // Deprecated

	userId string

	userName string

	// Namespace specific environment usage
	namespace string

	namespaceSource string

	httpClient http.Client
	initMethod string
	app        string
}

func WithHTTPClient(httpClient http.Client) Option {
	return func(client *Client) {
		client.httpClient = httpClient
	}
}

func WithOpenAPIDomain(domain string) Option {
	return func(client *Client) {
		client.domain = domain
	}
}

func WithOpenAPIEndpoint(endpoint string) Option {
	return func(client *Client) {
		client.endpoint = endpoint
	}
}

func WithInitMethod(initMethod string) Option {
	return func(client *Client) {
		client.initMethod = initMethod
	}
}

func WithNamespace(namespace string) Option {
	return func(client *Client) {
		client.namespace = namespace
	}
}

func WithGroupId(groupId string) Option {
	return func(client *Client) {
		client.groupId = groupId
	}
}

func WithAppKey(appKey string) Option {
	return func(client *Client) {
		client.appKey = appKey
	}
}

func WithApp(app string) Option {
	return func(client *Client) {
		client.app = app
	}
}

func WithUser(user string) Option {
	return func(client *Client) {
		client.user = user
	}
}

func NewClient(opts ...Option) *Client {
	cli := new(Client)
	for _, opt := range opts {
		opt(cli)
	}
	return cli
}

type CreateJavaJobRequest struct {
	BaseRequest

	JavaJobConfig
}

type CreateJavaJobResponse struct {
	JobId int64 `json:"job_id"`
}

// CreateJob create java type job
func (c *Client) CreateJob(req *CreateJavaJobRequest) (int64, error) {
	if !req.IsRequired() {
		return -1, errors.New("Required fields of CreateJavaJobRequest is empty. ")
	}

	url := fmt.Sprintf("http://%s/openapi/v1/job/create", c.domain)
	respData, err := c.sendRequest(url, req)
	if err != nil {
		return -1, err
	}
	resp := new(CreateJavaJobResponse)
	if err := json.Unmarshal(respData, resp); err != nil {
		return -1, err
	}
	return resp.JobId, nil
}

type CreateHTTPJobRequest struct {
	BaseRequest

	HttpJobConfig
}

type CreateHTTPJobResponse struct {
	JobId int64 `json:"job_id"`
}

// CreateHTTPJob create http job
func (c *Client) CreateHTTPJob(req *CreateHTTPJobRequest) (int64, error) {
	if !req.IsRequired() {
		return -1, errors.New("Required fields of CreateHTTPJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/create", c.domain)
	respData, err := c.sendRequest(url, req)
	if err != nil {
		return -1, err
	}
	resp := new(CreateHTTPJobResponse)
	if err := json.Unmarshal(respData, resp); err != nil {
		return -1, err
	}
	return resp.JobId, nil
}

type UpdateJobRequest struct {
	BaseRequest

	// Task information to be modified
	JobConfigInfo
}

type UpdateJobResponse struct {
	JobId int64 `json:"job_id"`
}

// UpdateJob update job
func (c *Client) UpdateJob(req *UpdateJobRequest) (int64, error) {
	if !req.IsRequired() {
		return -1, errors.New("Required fields of UpdateJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/update", c.domain)
	respData, err := c.sendRequest(url, req)
	if err != nil {
		return -1, err
	}

	resp := new(UpdateJobResponse)
	if err := json.Unmarshal(respData, resp); err != nil {
		return -1, err
	}
	return resp.JobId, nil
}

type DeleteJobRequest struct {
	BaseRequest

	jobId int
}

// DeleteJob delete job
func (c *Client) DeleteJob(req *DeleteJobRequest) error {
	if req.jobId <= 0 {
		return errors.New("Required fields of UpdateJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/delete", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type ExecJobRequest struct {
	BaseRequest

	jobId int
}

func (c *Client) ExecJob(req *DeleteJobRequest) error {
	if req.jobId <= 0 {
		return errors.New("Required fields of ExecJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/execute", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type EnableJobRequest struct {
	BaseRequest

	jobId int
}

func (c *Client) EnableJob(req *EnableJobRequest) error {
	if req.jobId <= 0 {
		return errors.New("Required fields of EnableJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/enable", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type DisableJobRequest struct {
	BaseRequest

	jobId int
}

func (c *Client) DisableJob(req *DisableJobRequest) error {
	if req.jobId <= 0 {
		return errors.New("Required fields of DisableJob is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/job/disable", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type GetJobInstanceRequest struct {
	BaseRequest

	jobId         int
	jobInstanceId int
}

func (c *Client) GetJobInstance(req *GetJobInstanceRequest) error {
	if req.jobId <= 0 || req.jobInstanceId <= 0 {
		return errors.New("Required fields of GetJobInstance is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/instance/get", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type GetJobInstanceListRequest struct {
	BaseRequest

	jobId int
}

func (c *Client) GetJobInstanceList(req *GetJobInstanceListRequest) error {
	if req.jobId <= 0 {
		return errors.New("Required fields of GetJobInstanceList is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/instance/get", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

type KillJobInstanceRequest struct {
	BaseRequest

	jobId      int
	instanceId int
}

func (c *Client) KillJobInstance(req *KillJobInstanceRequest) error {
	if req.jobId <= 0 || req.instanceId <= 0 {
		return errors.New("Required fields of KillJobInstance is empty. ")
	}
	url := fmt.Sprintf("http://%s/openapi/v1/instance/kill", c.domain)
	_, err := c.sendRequest(url, req)
	if err != nil {
		return err
	}
	return nil
}

// Fill headers
func (c *Client) genHeaders() map[string]string {
	headers := make(map[string]string)

	// Optional parameters
	if c.namespace != "" {
		headers[NAMESPACE_KEY_HEADER] = c.namespace

		if c.namespaceSource != "" {
			headers[NAMESPACE_SOURCE_HEADER] = c.namespaceSource
		}
	}

	// Time
	timeNowStr := strconv.Itoa(int(time.Now().UnixMilli()))
	headers[TIME_STAMP_HEADER] = timeNowStr

	// Signature
	if c.groupId != "" && c.appKey != "" {
		headers[SIGNATURE_HEADER] = utils.HmacSHA1Encrypt(c.groupId+timeNowStr, c.appKey)
	}

	// GroupId
	headers[GROUPID_HEADER] = c.groupId

	// Whether to create a group
	if c.groupId != "" {
		headers[CREATE_GROUP_HEADER] = "true"
	} else {
		headers[CREATE_GROUP_HEADER] = "false"
	}

	return headers
}

func (c *Client) sendRequest(url string, request BaseRequest) ([]byte, error) {
	// Fill in initialization parameters
	if c.groupId != "" {
		request.setGroupId(c.groupId)
	}
	if c.namespace != "" {
		request.setNamespace(c.namespace)
	}
	if c.namespaceSource != "" {
		request.setGroupId(c.namespaceSource)
	}
	if c.userId != "" {
		request.setParam("userId", c.userId)
	}
	if c.userName != "" {
		request.setParam("userName", c.userName)
	}
	if c.userId != "" && c.userName != "" {
		request.setParam("user", c.userName+" "+c.userId)
	} else {
		user := "openapi"
		if c.user != "" {
			user = c.user
		}
		request.setParam("user", user)
	}

	// Required parameter verification
	if !c.validateRequiredParam() {
		return nil, errors.New("Missing required param. ")
	}

	// Fill headers
	headers := c.genHeaders()
	logger.Infof("SendRequest url: %s, request params: %+v, headers: %+v", url, request.getParams(), headers)

	postBody, _ := json.Marshal(request.getParams())
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(postBody))
	if err != nil {
		return nil, fmt.Errorf("HTTP NewRequest failed, err=%s ", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP post failed, err=%s ", err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read http post response failed, err=%s ", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Read http post response failed, statusCode=%s ", resp.Status)
	}
	return body, nil
}

func (c *Client) validateRequiredParam() bool {
	return c.domain != ""
}

func (c *Client) SetDomain() {
	var (
		// 1. using config's domain
		domainAddr = c.domain
		err        error
	)

	// 2. using endpoint to get domainAddr, compatible Alibaba Cloud
	// endpoint only exited at Alibaba Cloud
	if domainAddr == "" && client.endpoint != "" {
		domainAddr, err = client.getDomainByEndpoint()
		if err != nil {
			logger.Warnf("Cannot get domainAddr from endpoint, init openAPI client failed, endpoint=%s, err=%s", client.endpoint, err.Error())
		}
	}

	if domainAddr == "" {
		panic(fmt.Sprintf("Cannot get domainAddr, init openAPI client failed, err=%s", err.Error()))
	}
	c.domain = domainAddr
}

func (c *Client) getDomainByEndpoint() (string, error) {
	if c.endpoint == "" {
		return "", errors.New("Required fields of KillJobInstance is empty. ")
	}
	url := fmt.Sprintf("http://%s:%s/schedulerx2/consolelist", c.endpoint, "8080")
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("HTTP post failed, err=%s ", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Read http post response failed, statusCode=%s ", resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Read http post response failed, err=%s ", err.Error())
	}
	return strings.TrimSpace(string(body)), nil
}

func (c *Client) Domain() string {
	return c.domain
}

func (c *Client) Namespace() string {
	return c.namespace
}

func (c *Client) NamespaceSource() string {
	return c.namespaceSource
}

func (c *Client) HttpClient() *http.Client {
	return &c.httpClient
}
