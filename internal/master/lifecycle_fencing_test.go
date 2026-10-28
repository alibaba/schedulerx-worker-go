package master

import (
	"context"
	"github.com/alibaba/schedulerx-worker-go/config"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"
	"sync"
	"testing"
	"time"
)

func TestRetiredWorkerShutdownPreservesReplacement(t *testing.T) {
	config.NewWorkerConfig(config.WithDisableMapMasterFailover())
	if config.GetWorkerConfig().IsMapMasterFailover() {
		t.Fatal("test requires failover-disabled cleanup branch")
	}
	useRetirementTestDirectory(t)
	db := persistence.GetH2MemoryPersistence()
	db.InitTable()
	const id = int64(1001)
	if err := db.ClearTasks(id); err != nil {
		t.Fatal(err)
	}
	defer db.ClearTasks(id)
	oldBase, _ := attemptTestMaster(0)
	oldBase.aliveCheckWorkerSet = utils.NewConcurrentSet[string]()
	oldBase.jobInstanceInfo.SetAllWorkers([]string{"worker@127.0.0.1:1234"})
	old := &MapTaskMaster{TaskMaster: oldBase, taskPersistence: db, taskDispatchReqHandler: &batch.TaskPushReqHandler{BaseTaskDispatchReqHandler: &batch.BaseTaskDispatchReqHandler{}}}
	currentBase, _ := attemptTestMaster(1)
	current := &MapTaskMaster{TaskMaster: currentBase, taskPersistence: db}
	pool := masterpool.NewTaskMasterPool(tasks.GetTaskMap())
	pool.Put(id, old)
	pool.Put(id, current)
	defer pool.Remove(id)
	if !old.statusWritesRetired {
		t.Fatal("old owner did not retire")
	}
	if err := db.CreateTask(1, id, 0, "replacement", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpdateTaskStatus(id, []int64{0}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234"); err != nil {
		t.Fatal(err)
	}
	if db.CheckInstanceStatus(id) != processor.InstanceStatusRunning {
		t.Fatal("replacement row missing before late callback")
	}
	// Resume the callback reached after an old in-flight worker heartbeat returns.
	old.handleWorkerShutdown("worker@127.0.0.1:1234")
	if got := db.CheckInstanceStatus(id); got != processor.InstanceStatusRunning {
		t.Fatalf("retired worker-shutdown callback deleted replacement task row; instance status=%v", got)
	}
	if pool.Get(id) != current {
		t.Fatal("replacement owner unexpectedly removed")
	}
}

type retirementActorContext struct {
	actor.Context
	system *actor.ActorSystem
}

func (c retirementActorContext) ActorSystem() *actor.ActorSystem { return c.system }

func newRetirementGrid(t *testing.T, attempt int32, modes ...string) *GridTaskMaster {
	t.Helper()
	masterpool.InitTaskMasterPool(masterpool.NewTaskMasterPool(tasks.GetTaskMap()))
	base, _ := attemptTestMaster(attempt)
	info := base.GetJobInstanceInfo()
	info.SetAllWorkers([]string{"worker@127.0.0.1:1234"})
	mode := "pull"
	if len(modes) > 0 {
		mode = modes[0]
	}
	info.SetXattrs(`{"taskDispatchMode":"` + mode + `","consumerSize":5}`)
	return NewGridTaskMaster(info, retirementActorContext{system: actor.NewActorSystem()})
}

func TestOldBatchHandlersCannotRebindToSameAttemptReplacement(t *testing.T) {
	useRetirementTestDirectory(t)
	for _, kind := range []string{"status", "pull"} {
		t.Run(kind, func(t *testing.T) {
			old := newRetirementGrid(t, 0)
			pool := masterpool.GetTaskMasterPool()
			pool.Put(1001, old)
			// Preserve exactly the same protocol identity; the local owner still differs.
			current := newRetirementGrid(t, 0)
			pool.Put(1001, current)
			defer pool.Remove(1001)
			db := current.taskPersistence
			if err := db.ClearTasks(1001); err != nil {
				t.Fatal(err)
			}
			defer db.ClearTasks(1001)
			if err := db.CreateTask(1, 1001, 0, "current", nil); err != nil {
				t.Fatal(err)
			}
			if kind == "status" {
				old.taskStatusReqQueue.SubmitRequest(attemptTestRequest(proto.Int32(0)))
				old.taskStatusReqBatchHandler.AsyncHandleReqs(old.taskStatusReqBatchHandler)
				if got := db.CheckInstanceStatus(1001); got != processor.InstanceStatusRunning {
					t.Fatalf("old handler deleted replacement rows: %v", got)
				}
			} else {
				req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
				old.taskDispatchReqHandler.Process(1001, []interface{}{req}, "worker@127.0.0.1:1234")
				n, err := db.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
				if err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("old handler inserted %d task rows into replacement", n)
				}
				if utils.SyncMapLen(&current.workerProgressMap) != 0 {
					t.Fatal("old handler changed replacement progress")
				}
			}
		})
	}
}

func TestDispatchRejectsStaleAttemptAndSerialBeforePersistence(t *testing.T) {
	useRetirementTestDirectory(t)
	for _, tc := range []struct {
		name    string
		attempt *int32
		serial  *int64
	}{
		{"old attempt", proto.Int32(0), proto.Int64(1)},
		{"old serial including explicit zero", proto.Int32(1), proto.Int64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := newRetirementGrid(t, 1)
			current.AcquireSerialNum()
			db := current.taskPersistence
			db.ClearTasks(1001)
			defer db.ClearTasks(1001)
			req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("old"), Attempt: tc.attempt, SerialNum: tc.serial}
			current.BatchPullTasks([]*schedulerx.MasterStartContainerRequest{req}, "worker@127.0.0.1:1234")
			n, err := db.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 || utils.SyncMapLen(&current.workerProgressMap) != 0 {
				t.Fatal("stale dispatch mutated persistence/progress")
			}
		})
	}
}

type blockedCreatePersistence struct {
	persistence.TaskPersistence
	entered, release chan struct{}
	once             sync.Once
}

func (p *blockedCreatePersistence) CreateTasks(reqs []*schedulerx.MasterStartContainerRequest, worker, addr string) error {
	p.once.Do(func() { close(p.entered); <-p.release })
	return p.TaskPersistence.CreateTasks(reqs, worker, addr)
}
func TestRetirementDrainsInflightTaskCreation(t *testing.T) {
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0)
	db := old.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	blocked := &blockedCreatePersistence{TaskPersistence: db, entered: make(chan struct{}), release: make(chan struct{})}
	old.taskPersistence = blocked
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	writeDone := make(chan struct{})
	retired := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	defer func() { release(); waitRetirementTest(t, writeDone); waitRetirementTest(t, retired) }()
	go func() {
		defer close(writeDone)
		old.batchHandlePersistence("worker", "127.0.0.1:1234", []*schedulerx.MasterStartContainerRequest{req}, false)
	}()
	waitRetirementTest(t, blocked.entered)
	go func() { defer close(retired); old.RetireStatusWrites() }()
	select {
	case <-retired:
		t.Fatal("retirement returned while old CreateTasks can still commit")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	waitRetirementTest(t, writeDone)
	waitRetirementTest(t, retired)
	db.ClearTasks(1001)
	old.batchHandlePersistence("worker", "127.0.0.1:1234", []*schedulerx.MasterStartContainerRequest{req}, false)
	n, err := db.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("retired dispatch created task rows")
	}
}

// Ensure constructors bind the concrete wrapper, including Sharding overrides.

func (c retirementActorContext) RequestFuture(pid *actor.PID, message interface{}, timeout time.Duration) *actor.Future {
	future := actor.NewFuture(c.system, timeout)
	c.system.Root.Send(future.PID(), &schedulerx.MasterBatchStartContainersResponse{Success: proto.Bool(true)})
	return future
}
func TestOldPushHandlerCannotDispatchIntoReplacement(t *testing.T) {
	useRetirementTestDirectory(t)
	old := newRetirementGrid(t, 0, "push")
	pool := masterpool.GetTaskMasterPool()
	pool.Put(1001, old)
	current := newRetirementGrid(t, 0, "push")
	pool.Put(1001, current)
	defer pool.Remove(1001)
	db := current.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	old.taskBlockingQueue.SubmitRequest(req)
	old.taskDispatchReqHandler.AsyncHandleReqs(old.taskDispatchReqHandler)
	deadline := time.Now().Add(3 * time.Second)
	for old.taskDispatchReqHandler.IsActive() {
		if time.Now().After(deadline) {
			t.Fatal("dispatch did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	n, err := db.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || utils.SyncMapLen(&current.workerProgressMap) != 0 {
		t.Fatal("old push handler dispatched/persisted into replacement")
	}
}

func TestLateProbeFromPreviousSerialCannotDeleteCurrentRows(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.AcquireSerialNum()
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	db.CreateTask(1, 1001, 0, "current cycle", nil)
	db.UpdateTaskStatus(1001, []int64{0}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	m.handleWorkerShutdown("worker@127.0.0.1:1234", 0)
	if db.CheckInstanceStatus(1001) != processor.InstanceStatusRunning {
		t.Fatal("old cycle probe deleted current task rows")
	}
}

type blockedPullPersistence struct {
	persistence.TaskPersistence
	entered, release chan struct{}
	once             sync.Once
}

func (p *blockedPullPersistence) Pull(id int64, size int32) ([]*common.TaskInfo, error) {
	p.once.Do(func() { close(p.entered); <-p.release })
	return p.TaskPersistence.Pull(id, size)
}
func TestRetirementJoinsInflightPullWithoutRedispatch(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	db := m.taskPersistence
	db.ClearTasks(1001)
	defer db.ClearTasks(1001)
	db.CreateTask(1, 1001, 0, "old", nil)
	blocked := &blockedPullPersistence{TaskPersistence: db, entered: make(chan struct{}), release: make(chan struct{})}
	m.taskPersistence = blocked
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	retired := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer func() { release(); cancel(); waitRetirementTest(t, done); waitRetirementTest(t, retired) }()
	go func() { defer close(done); m.pullTask(ctx, "1_1001") }()
	waitRetirementTest(t, blocked.entered)
	go func() { defer close(retired); m.RetireStatusWrites() }()
	select {
	case <-retired:
		t.Fatal("retirement returned before Pull mutation finished")
	case <-time.After(30 * time.Millisecond):
	}
	release()
	waitRetirementTest(t, retired)
	waitRetirementTest(t, done)
	if m.taskBlockingQueue.Size() != 0 {
		t.Fatal("retired Pull dispatched old rows")
	}
}

func TestExplicitZeroSerialIsPreservedAndRejectedBeforeProgress(t *testing.T) {
	base, _ := attemptTestMaster(0)
	base.AcquireSerialNum()
	sink := new(attemptPersistenceRecorder)
	m := &MapTaskMaster{TaskMaster: base, taskPersistence: sink}
	req := &schedulerx.ContainerBatchReportTaskStatuesRequest{Attempt: proto.Int32(0), SerialNum: proto.Int64(0), TaskStatues: []*schedulerx.TaskStatusInfo{{TaskId: proto.Int64(0), Status: proto.Int32(int32(taskstatus.TaskStatusSucceed))}}}
	// Expand via the real base method into a Map queue, then flush.
	m.taskStatusReqQueue = batch.NewReqQueue(4)
	if err := base.BatchUpdateTaskStatus(m, req); err != nil {
		t.Fatal(err)
	}
	pending := m.taskStatusReqQueue.RetrieveRequests(4)
	statuses := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(pending))
	for _, r := range pending {
		statuses = append(statuses, r.(*schedulerx.ContainerReportTaskStatusRequest))
	}
	m.BatchUpdateTaskStatues(statuses)
	if len(sink.received) != 0 || utils.SyncMapLen(&m.taskProgressMap) != 0 {
		t.Fatal("explicit old serial zero lost presence and mutated current cycle")
	}
}

func TestClearedCycleRejectsCallbacksUntilRestart(t *testing.T) {
	useRetirementTestDirectory(t)
	m := newRetirementGrid(t, 0)
	m.GetJobInstanceInfo().SetAllWorkers(nil)
	m.Clear(m)
	// Clear-to-next-trigger may take seconds; the old cycle must stay closed.
	req := &schedulerx.MasterStartContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(1001), TaskId: proto.Int64(99), TaskName: proto.String("old"), Attempt: proto.Int32(0), SerialNum: proto.Int64(0)}
	m.BatchPullTasks([]*schedulerx.MasterStartContainerRequest{req}, "worker@127.0.0.1:1234")
	n, err := m.taskPersistence.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || utils.SyncMapLen(&m.workerProgressMap) != 0 {
		t.Fatal("old cycle callback mutated a cleared reusable Master")
	}
	m.AcquireSerialNum()
	if err := m.startBatchHandler(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	req.SerialNum = proto.Int64(1)
	m.BatchPullTasks([]*schedulerx.MasterStartContainerRequest{req}, "worker@127.0.0.1:1234")
	n, err = m.taskPersistence.UpdateTaskStatus(1001, []int64{99}, taskstatus.TaskStatusRunning, "worker", "127.0.0.1:1234")
	if err != nil || n != 1 {
		t.Fatalf("new cycle cannot reuse Master: rows=%d err=%v", n, err)
	}
	m.taskPersistence.ClearTasks(1001)
}

func TestRetiredParallelRetryCannotResetServerTasks(t *testing.T) {
	base, _ := attemptTestMaster(0)
	base.GetJobInstanceInfo().SetJobInstanceId(98765)
	m := &ParallelTaskMaster{MapTaskMaster: &MapTaskMaster{TaskMaster: base}}
	m.RetireStatusWrites()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RetryTasks([]*schedulerx.RetryTaskEntity{{TaskId: proto.Int64(1), TaskName: proto.String("sub")}})
	}()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-done:
			return
		case msg := <-actorcomm.TaskMasterMsgReceiver():
			wrapped, ok := msg.(*actorcomm.SchedulerWrappedMsg)
			if !ok {
				continue
			}
			req, ok := wrapped.Msg.(*schedulerx.WorkerReportTaskListStatusRequest)
			if !ok || req.GetJobInstanceId() != 98765 {
				continue
			}
			actorcomm.WorkerBatchUpdateTaskStatusRespMsgSender() <- &schedulerx.WorkerBatchUpdateTaskStatusResponse{Success: proto.Bool(false)}
			waitRetirementTest(t, done)
			t.Fatal("retired Parallel Master sent a server task reset")
		case <-timer.C:
			t.Fatal("retired retry did not exit")
		}
	}
}
