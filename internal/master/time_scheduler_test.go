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
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
)

type schedulerSubmission struct {
	taskmaster.TaskMaster
	serial atomic.Int64
	submit func(*common.JobInstanceInfo)
}

func (r *schedulerSubmission) IsKilled() bool           { return false }
func (r *schedulerSubmission) AcquireSerialNum() int64  { return r.serial.Add(1) }
func (r *schedulerSubmission) ExistInvalidWorker() bool { return false }
func (r *schedulerSubmission) SubmitInstance(info *common.JobInstanceInfo) error {
	r.submit(info)
	return nil
}

func scannerPlan(id, deadline int64, submit func(*common.JobInstanceInfo)) (*TimePlanEntry, *schedulerSubmission) {
	info := new(common.JobInstanceInfo)
	info.SetJobId(id)
	info.SetJobInstanceId(id)
	info.SetExecuteMode("standalone")
	recorder := &schedulerSubmission{submit: submit}
	handler := NewSecondJobUpdateInstanceStatusHandler(nil, recorder, info)
	return NewTimePlanEntry(id, deadline, handler), recorder
}

func TestTimeSchedulerContinuesAfterConcurrentCancellation(t *testing.T) {
	// Production's scanner is intentionally a process-lifetime goroutine. Keep
	// its real loop in a child test process rather than add a test-only stop API.
	const childKey = "SCHEDULERX_QUEUE_SCANNER_TEST"
	if os.Getenv(childKey) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTimeSchedulerContinuesAfterConcurrentCancellation$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), childKey+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("real scheduler probe: %v\n%s", err, out)
		}
		return
	}
	scheduler := newTimeScheduler()
	go scheduler.timeScan()
	var wg sync.WaitGroup
	start := make(chan struct{})
	future := time.Now().Add(time.Hour).UnixMilli()
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				scheduler.add(NewTimePlanEntry(id, future+int64(i), nil))
				scheduler.remove(id)
				scheduler.timeQueue.Size()
			}
		}(int64(100 + worker))
	}
	close(start)
	wg.Wait()
	// A canceled future plan must not prevent due plans or be dispatched early.
	scheduler.add(NewTimePlanEntry(900, future, nil))
	scheduler.remove(900)
	calls := make(chan int64, 2)
	second, secondRecorder := scannerPlan(2, time.Now().Add(-time.Second).UnixMilli(), func(info *common.JobInstanceInfo) { calls <- info.GetJobInstanceId() })
	first, firstRecorder := scannerPlan(1, time.Now().Add(-2*time.Second).UnixMilli(), func(info *common.JobInstanceInfo) {
		// Dispatch is outside the queue lock: a submitted task can enqueue another plan.
		scheduler.add(second)
		calls <- info.GetJobInstanceId()
	})
	scheduler.add(first)
	for _, want := range []int64{1, 2} {
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("dispatched %d, want %d", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("scanner stopped or held queue lock during submission")
		}
	}
	if firstRecorder.serial.Load() != 1 || secondRecorder.serial.Load() != 1 {
		t.Fatal("scheduled instance triggered more than once")
	}
	if !scheduler.timeQueue.IsEmpty() {
		t.Fatal("canceled or claimed plan remains queued")
	}
}
