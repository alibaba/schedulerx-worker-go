/*
 * Copyright (c) 2026 Alibaba Group Holding Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package utils

import (
	"sync"
	"testing"
	"time"
)

func TestGroupHeartbeatSuccessDoesNotRenewAnotherGroup(t *testing.T) {
	for _, order := range [][]string{{"a", "b"}, {"b", "a"}} {
		h := newHealthTimeHolder()
		old := time.Now().Add(-31 * time.Second).UnixMilli()
		for _, group := range order {
			h.InitGroupServerHeartbeatTime(group)
			h.groupServerHeartbeatTime(group).Store(old)
		}
		h.ResetGroupServerHeartbeatTime("a")
		if h.IsGroupServerHeartbeatHealthTimeout("a", 30) {
			t.Fatal("successful group remains expired")
		}
		if !h.IsGroupServerHeartbeatHealthTimeout("b", 30) {
			t.Fatal("another group's success renewed failed group")
		}
		if h.GetGroupServerHeartbeatMsInterval("b") < 30_000 {
			t.Fatal("failed group elapsed time was reset")
		}
		h.ResetGroupServerHeartbeatTime("b")
		if h.IsGroupServerHeartbeatHealthTimeout("b", 30) {
			t.Fatal("recovered group remains expired")
		}
	}
}

func TestGroupHeartbeatGraceStartsWhenGroupIsFirstObserved(t *testing.T) {
	h := newHealthTimeHolder()
	old := time.Now().Add(-31 * time.Second).UnixMilli()
	h.latestServerHeartbeatTime.Store(old)
	h.InitGroupServerHeartbeatTime("existing")
	h.groupServerHeartbeatTime("existing").Store(old)
	h.InitGroupServerHeartbeatTime("existing")
	if !h.IsGroupServerHeartbeatHealthTimeout("existing", 30) {
		t.Fatal("initialization reset an existing expired group")
	}
	if h.IsGroupServerHeartbeatHealthTimeout("late-group", 30) {
		t.Fatal("newly discovered group inherited worker age")
	}
	h.ResetServerHeartbeatTime()
	if !h.IsGroupServerHeartbeatHealthTimeout("existing", 30) {
		t.Fatal("legacy success renewed an identified group")
	}
	if h.IsGroupServerHeartbeatHealthTimeout("", 30) {
		t.Fatal("empty group must retain legacy health semantics")
	}
	h.latestServerHeartbeatTime.Store(old)
	if !h.IsGroupServerHeartbeatHealthTimeout("", 30) {
		t.Fatal("empty group did not use the legacy watchdog")
	}
	h.ResetGroupServerHeartbeatTime("")
	if h.IsServerHeartbeatHealthTimeout(30) {
		t.Fatal("empty group success did not renew legacy health")
	}
}

func TestConcurrentGroupHeartbeatInitializationAndSuccess(t *testing.T) {
	h := newHealthTimeHolder()
	h.InitGroupServerHeartbeatTime("failed")
	h.groupServerHeartbeatTime("failed").Store(time.Now().Add(-31 * time.Second).UnixMilli())
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.InitGroupServerHeartbeatTime("active")
				h.ResetGroupServerHeartbeatTime("active")
				h.IsGroupServerHeartbeatHealthTimeout("active", 30)
				h.GetGroupServerHeartbeatMsInterval("active")
				h.ResetServerHeartbeatTime()
				h.IsServerHeartbeatHealthTimeout(30)
			}
		}()
	}
	wg.Wait()
	if !h.IsGroupServerHeartbeatHealthTimeout("failed", 30) {
		t.Fatal("concurrent healthy traffic renewed another group")
	}
	if h.IsGroupServerHeartbeatHealthTimeout("active", 30) {
		t.Fatal("active group was not renewed")
	}
}
