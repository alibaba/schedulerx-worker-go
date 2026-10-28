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
	"fmt"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestLegacyQueuedStatusRetainsAdmissionCycle(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	db := m.taskPersistence
	defer db.ClearTasks(1001)
	req := attemptTestRequest(nil)
	req.SerialNum = nil
	m.UpdateTaskStatus(req)
	oldBatch := m.taskStatusReqQueue.RetrieveRequests(4)
	if len(oldBatch) != 1 {
		t.Fatal("legacy request not admitted")
	}
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	db.CreateTask(1, 1001, 0, "current", nil)
	m.taskStatusReqBatchHandler.Process(1001, oldBatch, "")
	got := db.CheckInstanceStatus(1001)
	if got != processor.InstanceStatusRunning {
		t.Fatalf("old locally dequeued legacy batch deleted cycle-1 row: status=%v", got)
	}
}

type reviewR2Reduce struct {
	processor.MapReduceJobProcessor
	entered, release chan struct{}
}

func (r *reviewR2Reduce) RunReduceIfFail(*jobcontext.JobContext) bool { return true }
func (r *reviewR2Reduce) Reduce(*jobcontext.JobContext) (*processor.ProcessResult, error) {
	close(r.entered)
	<-r.release
	return processor.NewProcessResult(processor.WithSucceed()), nil
}
func TestDelayedReducePreservesNextCycleRowsAndProgress(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	m.GetJobInstanceInfo().SetContent(`{"jobName":"review-r2-reduce"}`)
	r := &reviewR2Reduce{entered: make(chan struct{}), release: make(chan struct{})}
	masterpool.GetTaskMasterPool().Tasks().Register("review-r2-reduce", r)
	db := m.taskPersistence
	defer db.ClearTasks(1001)
	done := make(chan struct{})
	go func() { defer close(done); m.PostFinish(1001) }()
	waitRetirementTest(t, r.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	db.CreateTask(1, 1001, 0, "current", nil)
	counter := common.NewTaskProgressCounter(constants.ReduceTaskName)
	m.taskProgressMap.Store(constants.ReduceTaskName, counter)
	close(r.release)
	waitRetirementTest(t, done)
	n, err := db.UpdateTaskStatus(1001, []int64{0}, taskstatus.TaskStatusRunning, "w", "a")
	if n != 1 || err != nil || counter.GetSuccess() != 0 {
		t.Fatalf("old Reduce completion touched current cycle: rows=%d err=%v currentReduceSuccess=%d", n, err, counter.GetSuccess())
	}
}

func TestRetiredOwnerDoesNotStartReduce(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetContent(`{"jobName":"review-r2-retired"}`)
	r := &reviewR2Reduce{entered: make(chan struct{}), release: make(chan struct{})}
	close(r.release)
	masterpool.GetTaskMasterPool().Tasks().Register("review-r2-retired", r)
	m.Stop()
	m.PostFinish(1001)
	select {
	case <-r.entered:
		t.Fatal("retired owner executed user Reduce")
	default:
	}
}

func TestCurrentCycleAcceptsLegacyStatus(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	defer m.Stop()
	db := m.taskPersistence
	defer db.ClearTasks(1001)
	db.CreateTask(1, 1001, 0, "active", nil)
	req := attemptTestRequest(nil)
	req.SerialNum = nil
	m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{req})
	if got := db.CheckInstanceStatus(1001); got != processor.InstanceStatusSucceed {
		t.Fatalf("active legacy failed: %v", got)
	}
	_ = proto.Int64(1)
}

type reviewR2ErrorPersistence struct{ persistence.TaskPersistence }

func (*reviewR2ErrorPersistence) UpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest) error {
	return fmt.Errorf("forced local storage error")
}

type reviewR2ErrorLogger struct {
	heldProgressLogger
	count int
	once  sync.Once
}

func (l *reviewR2ErrorLogger) Errorf(format string, _ ...interface{}) {
	if format == "jobInstanceId=%d, persistent batch updateTaskStatus error, err=%s" {
		l.count++
		if l.count == 3 {
			l.once.Do(func() { close(l.entered); <-l.release })
		}
	}
}
func TestPersistenceFailureCannotReportAsNextCycle(t *testing.T) {
	if os.Getenv("REVIEW_R2_ERROR_PROBE") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPersistenceFailureCannotReportAsNextCycle$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "REVIEW_R2_ERROR_PROBE=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated repro: %v\n%s", err, out)
		}
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	sink := &reviewSerialRecorder{serial: make(chan int64, 2)}
	m.statusHandler = sink
	held := &reviewR2ErrorLogger{heldProgressLogger: heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}}
	logger.SetLogger(held)
	m.taskPersistence = &reviewR2ErrorPersistence{m.taskPersistence}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.BatchUpdateTaskStatues([]*schedulerx.ContainerReportTaskStatusRequest{attemptTestRequest(proto.Int32(0))})
	}()
	waitRetirementTest(t, held.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	m.startBatchHandler()
	defer m.Stop()
	close(held.release)
	waitRetirementTest(t, done)
	select {
	case serial := <-sink.serial:
		t.Fatalf("old cycle-0 storage errors invoked failure handler with replacement serial=%d", serial)
	default:
	}
}

type reviewR2ReduceHandoff struct {
	processor.MapReduceJobProcessor
	entered, release chan struct{}
	ran              bool
}

func (r *reviewR2ReduceHandoff) RunReduceIfFail(*jobcontext.JobContext) bool {
	close(r.entered)
	<-r.release
	return true
}
func (r *reviewR2ReduceHandoff) Reduce(*jobcontext.JobContext) (*processor.ProcessResult, error) {
	r.ran = true
	return processor.NewProcessResult(processor.WithSucceed()), nil
}
func TestRetiredReduceHookDoesNotStartProcessor(t *testing.T) {
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	old.GetJobInstanceInfo().SetAllWorkers(nil)
	old.GetJobInstanceInfo().SetContent(`{"jobName":"review-r2-reduce-handoff"}`)
	r := &reviewR2ReduceHandoff{entered: make(chan struct{}), release: make(chan struct{})}
	pool := masterpool.GetTaskMasterPool()
	pool.Tasks().Register("review-r2-reduce-handoff", r)
	pool.Put(1001, old)
	done := make(chan struct{})
	go func() { defer close(done); old.UpdateNewInstanceStatus(0, processor.InstanceStatusSucceed, "") }()
	waitRetirementTest(t, r.entered)
	current := newRetirementGrid(t, 0)
	pool.Put(1001, current)
	close(r.release)
	waitRetirementTest(t, done)
	defer pool.Remove(1001)
	if r.ran {
		t.Fatal("old common status handler dispatched Reduce after replacement became visible")
	}
}

type reviewR2HeartbeatContext struct{ retirementActorContext }

func (c reviewR2HeartbeatContext) RequestFuture(_ *actor.PID, _ interface{}, timeout time.Duration) *actor.Future {
	f := actor.NewFuture(c.system, timeout)
	c.system.Root.Send(f.PID(), &schedulerx.MasterCheckWorkerAliveResponse{Success: proto.Bool(false)})
	return f
}
func TestOldHeartbeatCannotDestroyNextCycle(t *testing.T) {
	if os.Getenv("REVIEW_R2_PROBE") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestOldHeartbeatCannotDestroyNextCycle$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "REVIEW_R2_PROBE=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated repro: %v\n%s", err, out)
		}
		return
	}
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	worker := "worker@" + listener.Addr().String()
	m.jobInstanceInfo.SetAllWorkers([]string{worker})
	m.actorContext = reviewR2HeartbeatContext{retirementActorContext{system: actor.NewActorSystem()}}
	held := &heldProgressLogger{entered: make(chan struct{}), release: make(chan struct{})}
	logger.SetLogger(held)
	done := make(chan struct{})
	go func() { defer close(done); m.checkWorkerAlive(m.cycleCtx) }()
	waitRetirementTest(t, held.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	m.startBatchHandler()
	defer m.Stop()
	m.jobInstanceInfo.SetAllWorkers([]string{worker})
	close(held.release)
	waitRetirementTest(t, done)
	for {
		select {
		case msg := <-actorcomm.AtLeastOnceDeliveryMsgReceiver():
			if w, ok := msg.(*actorcomm.SchedulerWrappedMsg); ok {
				if req, ok := w.Msg.(*schedulerx.MasterDestroyContainerPoolRequest); ok && req.GetSerialNum() == 1 {
					t.Fatalf("old cycle-0 probe emitted destroy for replacement serial=%d", req.GetSerialNum())
				}
			}
		default:
			return
		}
	}
}

func TestDelayedSecondCycleFinalizerCannotClearRestartedCycle(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	m.GetJobInstanceInfo().SetTimeExpression("3600")
	m.GetJobInstanceInfo().SetContent(`{"jobName":"second-finalizer"}`)
	r := &reviewR2Reduce{entered: make(chan struct{}), release: make(chan struct{})}
	masterpool.GetTaskMasterPool().Tasks().Register("second-finalizer", r)
	h := NewSecondJobUpdateInstanceStatusHandler(m.actorCtx, m.MapTaskMaster, m.GetJobInstanceInfo())
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	defer GetTimeScheduler().remove(1001)
	done := make(chan struct{})
	go func() { defer close(done); h.triggerNextCycle("cycle0", 0, processor.InstanceStatusSucceed) }()
	waitRetirementTest(t, r.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	db.CreateTask(1, 1001, 0, "current", nil)
	close(r.release)
	waitRetirementTest(t, done)
	if db.CheckInstanceStatus(1001) != processor.InstanceStatusRunning {
		t.Fatal("old second-job finalizer cleared current cycle")
	}
}

type heldBatchExpansion struct {
	*GridTaskMaster
	entered, release chan struct{}
	once             sync.Once
}

func (h *heldBatchExpansion) UpdateTaskStatus(req *schedulerx.ContainerReportTaskStatusRequest) error {
	h.once.Do(func() { close(h.entered); <-h.release })
	return h.GridTaskMaster.UpdateTaskStatus(req)
}
func TestLegacyBatchExpansionRetainsReceiptCycle(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	h := &heldBatchExpansion{GridTaskMaster: m, entered: make(chan struct{}), release: make(chan struct{})}
	req := &schedulerx.ContainerBatchReportTaskStatuesRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(int32(taskstatus.TaskStatusSucceed))}}}
	done := make(chan struct{})
	go func() { defer close(done); m.TaskMaster.BatchUpdateTaskStatus(h, req) }()
	waitRetirementTest(t, h.entered)
	m.Clear(m)
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	db.CreateTask(1, 1001, 0, "new cycle", nil)
	close(h.release)
	waitRetirementTest(t, done)
	pending := m.taskStatusReqQueue.RetrieveRequests(4)
	statuses := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(pending))
	for _, r := range pending {
		statuses = append(statuses, r.(*schedulerx.ContainerReportTaskStatusRequest))
	}
	m.BatchUpdateTaskStatues(statuses)
	if req.SerialNum != nil {
		t.Fatal("legacy wire request was mutated")
	}
	if db.CheckInstanceStatus(1001) != processor.InstanceStatusRunning {
		t.Fatal("old legacy batch was rebound during expansion")
	}
}
