/*
 * Copyright (c) 2026 Alibaba Group Holding Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package master

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/discovery"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/asynkron/protoactor-go/actor"
)

type groupHealthPool struct {
	conn     net.Conn
	failures atomic.Int32
}

func (p *groupHealthPool) Get(context.Context) (net.Conn, error) {
	if p.conn == nil {
		p.failures.Add(1)
		return nil, errors.New("test group's server is unavailable")
	}
	return p.conn, nil
}

func (*groupHealthPool) ReconnectTrigger() chan struct{} { return nil }

// This deliberately waits out the real 30-second watchdog. There is no test
// clock or production-state mutation across package boundaries. Discovery and
// heartbeat singletons live in a child process so repeated suites stay isolated.
func TestHeartbeatOfAnotherGroupCannotKeepExpiredOwnerAlive(t *testing.T) {
	const childKey = "SCHEDULERX_GROUP_HEALTH_TEST_CHILD"
	if os.Getenv(childKey) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHeartbeatOfAnotherGroupCannotKeepExpiredOwnerAlive$", "-test.timeout=55s")
		cmd.Env = append(os.Environ(), childKey+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("group heartbeat integration failed: %v\n%s", err, output)
		}
		return
	}

	const healthyGroup, failedGroup = "health-test-a", "health-test-b"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/worker/v1/appgroup/getId":
			id := "1"
			if r.URL.Query().Get("groupId") == failedGroup {
				id = "2"
			}
			fmt.Fprintf(w, `{"success":true,"data":%q}`, id)
		case "/worker/v1/appgroup/get":
			io.WriteString(w, `{"success":true,"data":{}}`)
		case discovery.ActiveServerQueryURI:
			io.WriteString(w, `{"success":true,"data":"127.0.0.1:12345"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	openapi.InitOpenAPIClient(openapi.NewClient(openapi.WithOpenAPIDomain(strings.TrimPrefix(server.URL, "http://"))))
	discovery.GetGroupManager().StartServerDiscovery(healthyGroup, "test")
	discovery.GetGroupManager().StartServerDiscovery(failedGroup, "test")
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))

	info := new(common.JobInstanceInfo)
	info.SetJobId(1)
	info.SetJobInstanceId(1002)
	info.SetGroupId(failedGroup)
	info.SetAppGroupId(2)
	info.SetAllWorkers([]string{"live-worker"})
	owner := &TaskMaster{inited: true, jobInstanceInfo: info, aliveCheckWorkerSet: utils.NewConcurrentSet[string]()}
	handler := NewSecondJobUpdateInstanceStatusHandler(nil, owner, info)
	masterpool.GetTaskMasterPool().Put(info.GetJobInstanceId(), owner)
	utils.GetHealthTimeHolder() // Start the legacy timer too, for the baseline.
	if ids := masterpool.GetTaskMasterPool().GetInstanceIds(1); len(ids) != 0 {
		t.Fatalf("healthy group's heartbeat would incorrectly contain B: %v", ids)
	}
	if ids := masterpool.GetTaskMasterPool().GetInstanceIds(2); len(ids) != 1 || ids[0] != 1002 {
		t.Fatalf("failed group's heartbeat is missing its owner: %v", ids)
	}
	time.Sleep(31 * time.Second)
	if err := handler.need2KillSelf(); err != nil {
		t.Fatal(err)
	}
	if !owner.IsKilled() {
		t.Fatal("positive control: expired owner was not killed")
	}
	owner.killed = false // No goroutine reads this test-owned owner.

	writer, reader := net.Pipe()
	defer writer.Close()
	defer reader.Close()
	go io.Copy(io.Discard, reader)
	failed := &groupHealthPool{}
	pool.InitConnPoolManager(context.Background(), func(_ context.Context, group string) pool.ConnPool {
		if group == healthyGroup {
			return &groupHealthPool{conn: writer}
		}
		return failed
	})
	system := actor.NewActorSystem()
	defer system.Shutdown()
	system.ProcessRegistry.Address = "127.0.0.1:12346"
	stop := make(chan os.Signal, 1)
	stop <- os.Interrupt
	done := make(chan struct{})
	go func() { defer close(done); remoting.KeepHeartbeat(context.Background(), system, stop) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not finish its online and shutdown passes")
	}
	if failed.failures.Load() < 2 {
		t.Fatal("failed group was not tried in both heartbeat passes")
	}
	if utils.GetHealthTimeHolder().IsServerHeartbeatHealthTimeout(30) {
		t.Fatal("positive control: healthy group did not send and renew legacy health")
	}
	if err := handler.need2KillSelf(); err != nil {
		t.Fatal(err)
	}
	if !owner.IsKilled() {
		t.Fatal("healthy group A renewed failed group B's owner; B must remain expired")
	}
}
