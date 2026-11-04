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

package master

import (
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"os"
	"os/exec"
	"sync"
	"testing"
)

type heldProgressLogger struct {
	entered, release chan struct{}
	once             sync.Once
}

func (*heldProgressLogger) Debugf(string, ...interface{}) {}
func (*heldProgressLogger) Infof(string, ...interface{})  {}
func (*heldProgressLogger) Errorf(string, ...interface{}) {}
func (*heldProgressLogger) Level(string)                  {}
func (*heldProgressLogger) OutputPath(string) error       { return nil }
func (l *heldProgressLogger) Warnf(format string, _ ...interface{}) {
	if format == "jobInstanceId=%d, failover task number:%d, workerId:%s, workerAddr:%s" {
		l.once.Do(func() { close(l.entered); <-l.release })
	}
}
func TestWorkerShutdownProgressCannotCrossCycle(t *testing.T) {
	// Logger.SetLogger is process-wide and has no getter. Isolate this scheduling
	// seam so it cannot replace another test's logger or race background logging.
	if os.Getenv("SCHEDULERX_PROGRESS_PROBE") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerShutdownProgressCannotCrossCycle$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "SCHEDULERX_PROGRESS_PROBE=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated progress probe failed: %v\n%s", err, out)
		}
		return
	}
	useRetirementTestDirectory(t)
	held := &heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}
	logger.SetLogger(held)
	m := newRetirementGrid(t, 0)
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	if err := db.CreateTask(1, 1001, 0, "old", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpdateTaskStatus(1001, []int64{0}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(held.release) }) }
	defer func() { release(); waitRetirementTest(t, done); m.Stop() }()
	go func() { defer close(done); m.handleWorkerShutdown("worker@127.0.0.1:1234", 0) }()
	waitRetirementTest(t, held.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	counter := common.NewWorkerProgressCounter("127.0.0.1:1234")
	counter.IncrementRunning()
	m.workerProgressMap.Store("127.0.0.1:1234", counter)
	release()
	waitRetirementTest(t, done)
	if counter.GetFailed() != 0 {
		t.Fatalf("old completion changed new-cycle failure count to %d", counter.GetFailed())
	}
}
