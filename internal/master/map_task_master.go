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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/tidwall/gjson"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	actorcomm "github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"github.com/alibaba/schedulerx-worker-go/tracer"
)

var (
	_ taskmaster.MapTaskMaster = &MapTaskMaster{}
)

type MapTaskMaster struct {
	*TaskMaster
	actorCtx           actor.Context
	pageSize           int32
	queueSize          int64
	dispatcherSize     int64
	index              int
	taskStatusReqQueue *batch.ReqQueue
	// task batch reporting queue, item: ContainerReportTaskStatusRequest
	taskStatusReqBatchHandler *batch.TMStatusReqHandler
	// Subtask memory cache queue, the push model is pulled and pushed actively through TaskDispatchReqHandler,
	// and the pull model is pulled through PullThread
	taskBlockingQueue      *batch.ReqQueue
	taskDispatchReqHandler batch.TaskDispatchReqHandler
	// Handle task failure separately
	rootTaskResult      string
	taskPersistence     persistence.TaskPersistence
	taskProgressMap     sync.Map // key: string taskName, val: *common.TaskProgressCounter
	workerProgressMap   sync.Map // key: string workerAddr, val: *common.WorkerProgressCounter
	taskResultMap       sync.Map // key: int64 taskId, val: string result
	taskStatusMap       sync.Map // key: int64 taskId, val: taskstatus.TaskStatus
	xAttrs              *common.MapTaskXAttrs
	taskCounter         *atomic.Int64
	localTaskRouterPath string
	once                sync.Once
	cycleCtx            context.Context
	cycleCancel         context.CancelFunc
	cycleTransitionMu   sync.Mutex
	statusWriteMu       sync.RWMutex
	statusWritesRetired bool
	cycleClosed         bool
	closedSerial        int64
	retireCh            chan struct{}
	retireOnce          sync.Once
}

func NewMapTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) taskmaster.TaskMaster {
	ctx, cancel := context.WithCancel(context.Background())
	mapTaskMaster := &MapTaskMaster{
		actorCtx:            actorCtx,
		pageSize:            config.GetWorkerConfig().MapMasterPageSize(),
		queueSize:           int64(config.GetWorkerConfig().MapMasterQueueSize()),
		dispatcherSize:      int64(config.GetWorkerConfig().MapMasterDispatcherSize()),
		cycleCtx:            ctx,
		retireCh:            make(chan struct{}),
		cycleCancel:         cancel,
		taskCounter:         atomic.NewInt64(0),
		localTaskRouterPath: actorCtx.ActorSystem().Address(),
		// taskStatusReqQueue:    batch.NewReqQueue(100000),
		// taskBlockingQueue:     batch.NewReqQueue(100000),
	}

	if utils.IsSecondTypeJob(common.TimeType(jobInstanceInfo.GetTimeType())) {
		secondHandler := NewSecondJobUpdateInstanceStatusHandler(actorCtx, mapTaskMaster, jobInstanceInfo)
		mapTaskMaster.TaskMaster = NewTaskMaster(actorCtx, jobInstanceInfo, secondHandler)
		secondHandler.init()
	} else {
		statusHandler := NewCommonUpdateInstanceStatusHandler(actorCtx, mapTaskMaster, jobInstanceInfo)
		mapTaskMaster.TaskMaster = NewTaskMaster(actorCtx, jobInstanceInfo, statusHandler)
	}

	// mapTaskMaster.taskStatusReqBatchHandler = batch.NewTMStatusReqHandler(jobInstanceInfo.GetJobInstanceId(), 1, 1, 3000, mapTaskMaster.taskStatusReqQueue)
	// if jobInstanceInfo.GetXattrs() != "" {
	//	if err := json.Unmarshal([]byte(jobInstanceInfo.GetXattrs()), mapTaskMaster.xAttrs); err != nil {
	//		logger.Errorf("Unmarshal xAttrs failed, err=%s", err.Error())
	//	}
	// }
	// if mapTaskMaster.xAttrs != nil && mapTaskMaster.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) {
	//	mapTaskMaster.taskDispatchReqHandler = batch.NewTaskPullReqHandler(
	//		jobInstanceInfo.GetJobInstanceId(), 1, 1, int32(mapTaskMaster.pageSize*int64(len(jobInstanceInfo.GetAllWorkers()))),
	//		mapTaskMaster.taskBlockingQueue)
	// } else {
	//	mapTaskMaster.taskDispatchReqHandler = batch.NewTaskPushReqHandler(
	//		jobInstanceInfo.GetJobInstanceId(), 1, 1, int32(mapTaskMaster.pageSize*int64(len(jobInstanceInfo.GetAllWorkers()))),
	//		mapTaskMaster.taskBlockingQueue, 3000)
	// }

	return mapTaskMaster
}

func (m *MapTaskMaster) init(serials ...int64) {
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	if !m.cycleActive(serial) {
		return
	}
	m.once.Do(func() {
		m.TaskMaster.Init()
		m.statusWriteMu.RLock()
		ctx := m.cycleCtx
		m.statusWriteMu.RUnlock()
		jobIdAndInstanceId := strconv.FormatInt(m.GetJobInstanceInfo().GetJobId(), 10) + "_" + strconv.FormatInt(m.GetJobInstanceInfo().GetJobInstanceId(), 10)
		logger.Infof("jobInstanceId=%s, map master config, pageSize:%d, queueSize:%d, dispatcherSize:%d, workerSize:%d",
			jobIdAndInstanceId, m.pageSize, m.queueSize, m.dispatcherSize, len(m.GetJobInstanceInfo().GetAllWorkers()))

		// pull
		go m.pullTask(ctx, jobIdAndInstanceId, serial)

		// status check
		go m.checkInstanceStatus(ctx, serial)

		// job instance progress report
		if !utils.IsSecondTypeJob(common.TimeType(m.GetJobInstanceInfo().GetTimeType())) {
			go m.reportJobInstanceProgress(ctx, serial)
		}

		// worker alive check thread
		go m.checkWorkerAlive(ctx, serial)

		// PULL_MODEL specially
		//		if m.xAttrs != nil && m.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) {
		//			go m.notifyWorkerPull()
		//		}
	})
}

func (m *MapTaskMaster) pullTask(ctx context.Context, jobIdAndInstanceId string, serials ...int64) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	for {
		select {
		case <-ctx.Done():
			logger.Infof("pullTask exit, jobIdAndInstanceId=%s", jobIdAndInstanceId)
			return
		default:
		}
		if m.GetInstanceStatus().IsFinished() {
			return
		}
		jobInstanceId := m.GetJobInstanceInfo().GetJobInstanceId()
		startTime := time.Now()
		taskInfos, admitted, err := m.pullPersistedTasks(serial, jobInstanceId, m.pageSize)
		if !admitted || !m.cycleActive(serial) {
			return
		}
		if err != nil && errors.Is(err, persistence.ErrTimeout) {
			logger.Errorf("pull task timeout, uniqueId: %s", jobIdAndInstanceId)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}
		logger.Debugf("jobInstanceId=%d, pull cost=%dms", jobInstanceId, time.Since(startTime).Milliseconds())
		if len(taskInfos) == 0 {
			logger.Debugf("pull task empty of jobInstanceId=%d, sleep 10s ...", jobInstanceId)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
		} else {

			for _, info := range taskInfos {
				var req *schedulerx.MasterStartContainerRequest
				var convertErr error
				if !m.withCycleState(serial, func() {
					name := info.TaskName()
					if counter, ok := m.taskProgressMap.LoadOrStore(name, common.NewTaskProgressCounter(name)); ok {
						counter.(*common.TaskProgressCounter).DecrementRunning()
					}
					req, convertErr = m.convert2StartContainerRequest(m.GetJobInstanceInfo(), info.TaskId(), name, info.TaskBody(), true)
					if req != nil {
						req.SerialNum = proto.Int64(serial)
					}
				}) {
					return
				}
				if convertErr != nil {
					m.updateNewInstanceStatus(serial, jobInstanceId, processor.InstanceStatusFailed, convertErr.Error())
					return
				}
				if m.taskBlockingQueue != nil && !m.taskBlockingQueue.SubmitRequestUntil(req, ctx.Done()) {
					return
				}
			}

		}
	}
}

func (m *MapTaskMaster) checkInstanceStatus(ctx context.Context, serials ...int64) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	checkInterval := config.GetWorkerConfig().MapMasterStatusCheckInterval()
	for {
		select {
		case <-ctx.Done():
			logger.Infof("checkInstanceStatus exit, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId())
			return
		case <-time.After(checkInterval):
		}
		if m.GetInstanceStatus().IsFinished() {
			return
		}
		newStatus := m.taskPersistence.CheckInstanceStatus(m.GetJobInstanceInfo().GetJobInstanceId())
		if !m.cycleActive(serial) {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if newStatus.IsFinished() && m.taskDispatchReqHandler.IsActive() {
			var (
				failCnt    int64
				successCnt int64
				totalCnt   int64
			)
			m.taskProgressMap.Range(func(key, value any) bool {
				taskProgressCounter := value.(*common.TaskProgressCounter)
				failCnt += taskProgressCounter.GetFailed()
				successCnt += taskProgressCounter.GetSuccess()
				totalCnt += taskProgressCounter.GetTotal()
				return true
			})

			// avoid wrong early finish instance in condition root task was success but sub tasks are still creating.
			select {
			case <-ctx.Done():
				return
			case <-time.After(checkInterval):
			}
			continue
		}
		result := m.GetRootTaskResult()
		if newStatus == processor.InstanceStatusSucceed {
			// if return finish status, we need check counter
			var (
				failCnt    int64
				successCnt int64
				totalCnt   int64
			)
			m.taskProgressMap.Range(func(key, value any) bool {
				taskProgressCounter := value.(*common.TaskProgressCounter)
				failCnt += taskProgressCounter.GetFailed()
				successCnt += taskProgressCounter.GetSuccess()
				totalCnt += taskProgressCounter.GetTotal()
				return true
			})
			if successCnt+failCnt < totalCnt {
				newStatus = processor.InstanceStatusFailed
				logger.Warnf("jobInstanceId=%d turn into finish status, but count isn't correct, successCnt:%d, failCnt:%d, totalCnt:%d", m.GetJobInstanceInfo().GetJobInstanceId(), successCnt, failCnt, totalCnt)
				result = fmt.Sprintf("Turn into finish status, but count is wrong, sucCnt:%d, failCnt:%d, totalCnt:%d", successCnt, failCnt, totalCnt)
			} else {
				if failCnt > 0 {
					newStatus = processor.InstanceStatusFailed
				} else {
					newStatus = processor.InstanceStatusSucceed
				}
			}
		}

		if err := m.updateNewInstanceStatus(serial, m.GetJobInstanceInfo().GetJobInstanceId(), newStatus, result); err != nil {
			logger.Errorf("updateNewInstanceStatus failed, serialNum=%d, jobInstanceId=%d, newStatus=%d, result=%s, err=%s",
				m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), newStatus, result, err.Error())
		}
	}
}

func (m *MapTaskMaster) reportJobInstanceProgress(ctx context.Context, serials ...int64) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	for {
		select {
		case <-ctx.Done():
			logger.Infof("reportJobInstanceProgress exit, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId())
			return
		default:
		}
		if m.GetInstanceStatus().IsFinished() {
			return
		}
		progress, err := m.GetJobInstanceProgress()
		if err != nil {
			logger.Errorf("report status error, uniqueId=%d, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), err.Error())
			continue // FIXME continue or break?
		}

		req := &schedulerx.WorkerReportJobInstanceProgressRequest{
			JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			Progress:      proto.String(progress),
		}

		// Send to server by master
		logger.Infof("MapTaskMaster reportJobInstanceProgress, req=%+v", req)
		if !m.sendCycleMessage(serial, actorcomm.TaskMasterMsgReceiver(), &actorcomm.SchedulerWrappedMsg{Msg: req}, false) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (m *MapTaskMaster) checkWorkerAlive(ctx context.Context, serials ...int64) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	for {
		select {
		case <-ctx.Done():
			logger.Infof("checkWorkerAlive exit, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId())
			return
		default:
		}
		if m.GetInstanceStatus().IsFinished() {
			return
		}
		for _, worker := range m.GetJobInstanceInfo().GetAllWorkers() {
			m.aliveCheckWorkerSet.Add(worker)
		}
		if m.aliveCheckWorkerSet.Len() == 0 {
			logger.Warnf("worker list is empty, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId())
			m.batchUpdatePersistedTasks(serial, m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusFailed, "", "")
			break
		} else {
			for _, workerIdAddr := range m.aliveCheckWorkerSet.Keys() {
				workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)

				times := 0
				for times < 3 {
					conn, err := net.Dial("tcp", workerAddr)
					if err == nil {
						logger.Debugf("socket to %s is reachable, times=%d", workerAddr, times)
						conn.Close()
						break
					} else {
						logger.Warnf("socket to %s is not reachable, times=%d", workerAddr, times)
						select {
						case <-ctx.Done():
							return
						case <-time.After(5 * time.Second):
						}
						times++
					}
				}
				if times >= 3 {
					logger.Warnf("worker[%s] is down, start to remove this worker and failover tasks, jobInstanceId=%d", workerIdAddr, m.GetJobInstanceInfo().GetJobInstanceId())
					m.handleWorkerShutdown(workerIdAddr, serial)
					continue
				}

				request := &schedulerx.MasterCheckWorkerAliveRequest{
					JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
					DispatchMode:  proto.String(m.xAttrs.GetTaskDispatchMode()),
				}
				response, err := m.actorContext.RequestFuture(actorcomm.GetHeartbeatActorPid(workerAddr), request, 10*time.Second).Result()
				if !m.cycleActive(serial) {
					return
				}
				if err != nil {
					logger.Errorf("check worker error, jobInstanceId=%d, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), err.Error())
					break
				}

				if resp := response.(*schedulerx.MasterCheckWorkerAliveResponse); !resp.GetSuccess() {
					logger.Warnf("jobInstanceId=%d of worker=%s is not alive, remote worker resp=%+v", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, resp.GetMessage())
					m.handleWorkerShutdown(workerIdAddr, serial)
					if !m.cycleActive(serial) {
						return
					}

					// destroy containers of worker of PullModel
					destroyContainerPoolRequest := &schedulerx.MasterDestroyContainerPoolRequest{
						JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
						JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
						WorkerIdAddr:  proto.String(workerAddr),
						SerialNum:     proto.Int64(serial),
						Attempt:       proto.Int32(m.GetJobInstanceInfo().GetAttempt()),
					}
					if !m.sendCycleMessage(serial, actorcomm.AtLeastOnceDeliveryMsgReceiver(), &actorcomm.SchedulerWrappedMsg{Msg: destroyContainerPoolRequest}, false) {
						return
					}
				}
			}

			// Worker detection is performed every 10 seconds
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
		}
	}
}

func (m *MapTaskMaster) notifyWorkerPull() {
	for !m.GetInstanceStatus().IsFinished() {
		for _, workerIdAddr := range m.GetJobInstanceInfo().GetAllWorkers() {
			// FIXME
			jobInstanceActorPid := actor.NewPID(workerIdAddr, actorcomm.JobInstancePidId)
			request := &schedulerx.MasterNotifyWorkerPullRequest{
				JobInstanceId:      proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
				PageSize:           proto.Int32(m.xAttrs.GetPageSize()),
				QueueSize:          proto.Int32(m.xAttrs.GetQueueSize()),
				TaskMasterAkkaPath: proto.String(m.GetLocalTaskRouterPath()),
				ConsumerSize:       proto.Int32(m.xAttrs.GetConsumerSize()),
			}
			response, err := m.actorCtx.RequestFuture(jobInstanceActorPid, request, 5*time.Second).Result()
			if err != nil {
				logger.Errorf("notify worker pull error, jobInstanceId=%d, worker=%d, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, err.Error())
				break
			}
			if resp := response.(*schedulerx.MasterNotifyWorkerPullResponse); !resp.GetSuccess() {
				errorMsg := resp.GetMessage()
				logger.Errorf("notify worker pull failed, jobInstanceId=%d, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), errorMsg)
				m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, errorMsg)
			}
		}

		time.Sleep(5 * time.Second)
	}
}

func (m *MapTaskMaster) SubmitInstance(jobInstanceInfo *common.JobInstanceInfo) error {
	serial := m.GetSerialNum()
	var err error
	defer func() {
		if err != nil {
			errMsg := fmt.Sprintf("Submit instance failed, err=%s", err.Error())
			logger.Errorf(errMsg)
			m.updateNewInstanceStatus(serial, m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, errMsg)
		}
	}()

	startTime := time.Now()
	if m.dispatcherSize > constants.MapMasterDispatcherSizeMax {
		m.dispatcherSize = constants.MapMasterDispatcherSizeMax
	}
	if err = m.startBatchHandler(serial); err != nil {
		return err
	}
	if err = m.createRootTask(); err != nil {
		return err
	}
	logger.Infof("jobInstanceId=%d create root task, cost=%dms", jobInstanceInfo.GetJobInstanceId(), time.Since(startTime).Milliseconds())
	m.init(serial)

	return err
}

func (m *MapTaskMaster) UpdateTaskStatus(request *schedulerx.ContainerReportTaskStatusRequest) error {
	serial := m.GetSerialNum()
	done := m.cycleDone()
	if !m.cycleActive(serial) || !m.acceptsTaskGeneration(request.Attempt, request.SerialNum) {
		return nil
	}
	if request.SerialNum == nil {
		request = proto.Clone(request).(*schedulerx.ContainerReportTaskStatusRequest)
		request.SerialNum = proto.Int64(serial)
	}
	m.taskStatusReqQueue.SubmitRequestUntil(request, done)
	return nil
}

func (m *MapTaskMaster) BatchUpdateTaskStatues(requests []*schedulerx.ContainerReportTaskStatusRequest) {
	serial := m.GetSerialNum()
	m.statusWriteMu.RLock()
	if m.statusWritesRetired || m.cycleClosed {
		m.statusWriteMu.RUnlock()
		return
	}
	finalTaskStatus := make(map[int64]*schedulerx.ContainerReportTaskStatusRequest)
	var latestAccepted *schedulerx.ContainerReportTaskStatusRequest
	for _, request := range requests {
		// An old TMStatusReqHandler can resolve a replacement Master at flush
		// time, so validate again before progress, result or persistence updates.
		if !m.acceptsTaskGeneration(request.Attempt, request.SerialNum) {
			continue
		}
		latestAccepted = request
		taskStatus := taskstatus.TaskStatus(request.GetStatus())

		// Filter intermediate states
		if _, ok := finalTaskStatus[request.GetTaskId()]; !ok || taskStatus.IsFinished() {
			finalTaskStatus[request.GetTaskId()] = request
		}
		var (
			workerAddr = request.GetWorkerAddr()
			taskName   = request.GetTaskName()
		)

		logger.Debugf("report task status:%s from worker:%s, uniqueId:%s", taskStatus.Descriptor(), workerAddr,
			utils.GetUniqueId(request.GetJobId(), request.GetJobInstanceId(), request.GetTaskId()))

		m.taskProgressMap.LoadOrStore(taskName, common.NewTaskProgressCounter(taskName))
		if _, ok := m.workerProgressMap.Load(workerAddr); workerAddr != "" && !ok {
			m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		}

		switch taskStatus {
		case taskstatus.TaskStatusRunning:
			if val, ok := m.taskProgressMap.Load(taskName); ok {
				val.(*common.TaskProgressCounter).IncrementRunning()
			}
			if workerAddr != "" {
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).IncrementRunning()
				}
			}
		case taskstatus.TaskStatusSucceed:
			if val, ok := m.taskProgressMap.Load(taskName); ok {
				val.(*common.TaskProgressCounter).IncrementOneSuccess()
			}
			if workerAddr != "" {
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).IncrementSuccess()
				}
			}
		case taskstatus.TaskStatusFailed:
			if val, ok := m.taskProgressMap.Load(taskName); ok {
				val.(*common.TaskProgressCounter).IncrementOneFailed()
			}
			if workerAddr != "" {
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).IncrementOneFailed()
				}
			}
		}

		// update taskResultMap and taskStatusMap
		m.taskResultMap.Store(request.GetTaskId(), request.GetResult())
		m.taskStatusMap.Store(request.GetTaskId(), taskStatus)
	}

	if latestAccepted == nil {
		m.statusWriteMu.RUnlock()
		return
	}
	startTime := time.Now()

	// Return the reason for the failure of the root task node.
	// It is possible to return two root task requests at the same time, which are running and failed statuses, take the last one.
	if latestAccepted.GetStatus() == int32(taskstatus.TaskStatusFailed) && latestAccepted.GetTaskName() == constants.MapTaskRootName {
		m.SetRootTaskResult(latestAccepted.GetResult())
	}

	updateSuccess := false
	allTaskStatus := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(finalTaskStatus))
	for _, value := range finalTaskStatus {
		allTaskStatus = append(allTaskStatus, value)
	}
	m.statusWriteMu.RUnlock()
	for i := 0; i < 3; i++ {
		// try 3 times
		err, written := m.persistTaskStatuses(allTaskStatus)
		if !written {
			return
		}
		if err != nil {
			logger.Errorf("jobInstanceId=%d, persistent batch updateTaskStatus error, err=%s", m.jobInstanceInfo.GetJobInstanceId(), err.Error())
			continue
		}
		updateSuccess = true
		break
	}
	if !updateSuccess && m.cycleActive(serial) {
		err := m.UpdateNewInstanceStatus(serial, processor.InstanceStatusFailed, "persistent batch update TaskStatus error up to 3 times")
		if err != nil {
			logger.Errorf("jobInstanceId=%d, UpdateNewInstanceStatus failed, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), err.Error())
		}
	}
	logger.Debugf("jobInstanceId=%d batch update status db cost %dms", m.GetJobInstanceInfo().GetJobInstanceId(), time.Since(startTime).Milliseconds())
}

func (m *MapTaskMaster) Map(jobCtx *jobcontext.JobContext, taskList [][]byte, taskName string) (bool, error) {
	serial := m.GetSerialNum()
	done := m.cycleDone()
	if !m.acceptsMapContext(jobCtx) {
		return false, nil
	}
	if !m.withCycleState(serial, func() { m.initTaskProgress(taskName, len(taskList)) }) {
		return false, nil
	}
	for _, body := range taskList {
		var req *schedulerx.MasterStartContainerRequest
		var err error
		if !m.withCycleState(serial, func() {
			req, err = m.convert2StartContainerRequest(m.GetJobInstanceInfo(), m.AcquireTaskId(), taskName, body, false)
			if req != nil {
				req.SerialNum = proto.Int64(serial)
			}
		}) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("convert2StartContainerRequest failed: %w", err)
		}
		if !m.taskBlockingQueue.SubmitRequestUntil(req, done) {
			return false, nil
		}
	}
	return m.machineOverload(), nil
}

func (m *MapTaskMaster) machineOverload() bool {
	var (
		memOverload       = false
		loadOverload      = false
		taskQueueOverload = false
	)
	// FIXME golang get heap and cpu metric
	// vmDetail := MetricsCollector.getMetrics()
	// if vmDetail != nil {
	//	memOverload = vmDetail.getHeap1Usage() >= WorkerConstants.USER_MEMORY_PERCENT_MAX
	//	loadOverload = vmDetail.getCpuLoad1() >= vmDetail.getCpuProcessors()
	// }
	return memOverload || loadOverload || taskQueueOverload
}

func (m *MapTaskMaster) clearTasks(jobInstanceId int64) {
	if err, cleared := m.clearPersistedTasks(jobInstanceId); err != nil {
		logger.Errorf("jobInstanceId=%d clearTasks failed: %v", jobInstanceId, err)
	} else if cleared {
		logger.Infof("jobInstanceId=%d clearTasks success.", jobInstanceId)
	}
}

func (m *MapTaskMaster) createRootTask() error {
	taskName := constants.MapTaskRootName
	taskBody, err := json.Marshal(constants.MapTaskRootName)
	if err != nil {
		return err
	}
	m.initTaskProgress(taskName, 1)
	startContainerRequest, err := m.convert2StartContainerRequest(m.GetJobInstanceInfo(), m.AcquireTaskId(), taskName, taskBody, false)
	if err != nil {
		return fmt.Errorf("convert2StartContainerRequest failed, err=%s", err.Error())
	}
	m.BatchDispatchTasks([]*schedulerx.MasterStartContainerRequest{startContainerRequest}, m.GetLocalWorkerIdAddr())
	return nil
}

func (m *MapTaskMaster) batchHandleContainers(workerIdAddr string, reqs []*schedulerx.MasterStartContainerRequest, isFailover bool, dispatchMode common.TaskDispatchMode) {
	parts := strings.Split(workerIdAddr, "@")
	workerId := parts[0]
	workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)

	logger.Debugf("jobInstanceId=%d, batch dispatch, worker:%s, size:%d", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, len(reqs))
	serial := m.GetSerialNum()
	reqs = m.filterDispatchRequests(reqs)
	if len(reqs) == 0 || !m.batchHandlePersistence(workerId, workerAddr, reqs, isFailover) {
		return
	}
	if dispatchMode == common.TaskDispatchModePush {
		startTime := time.Now()
		// FIXME
		// workerAddr = actorcomm.GetRemoteWorkerAddr(workerAddr)

		containerRouterActorPid := actorcomm.GetContainerRouterPid(workerAddr)
		req := &schedulerx.MasterBatchStartContainersRequest{
			JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			StartReqs:     reqs,
		}

		var future *actor.Future
		if !m.withTaskPersistence(serial, func() { future = m.actorCtx.RequestFuture(containerRouterActorPid, req, 15*time.Second) }) {
			return
		}
		result, err := future.Result()
		if !m.cycleActive(serial) {
			return
		}
		if err == nil {
			// Trigger success callback
			resp := result.(*schedulerx.MasterBatchStartContainersResponse)
			if resp.GetSuccess() {
				logger.Infof("jobInstanceId=%d, batch start containers successfully, size:%d, worker=%s, cost=%dms",
					m.GetJobInstanceInfo().GetJobInstanceId(), len(reqs), workerIdAddr, time.Since(startTime).Milliseconds())
			} else {
				logger.Errorf("jobInstanceId=%d, batch start containers failed, worker=%s, response=%s, size:%d",
					m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, resp.GetMessage(), len(reqs))

				// TODO 发送失败应该尝试另一个worker还是直接置为失败？可能要根据返回值进行处理
				// Currently it is set to fail directly
				m.batchUpdateTaskStatus(workerId, workerAddr, reqs)
			}
		} else {
			// Trigger timeout or failure callback
			if errors.Is(err, actor.ErrTimeout) {
				if len(m.GetJobInstanceInfo().GetAllWorkers()) == 1 {
					logger.Errorf("jobInstanceId:%d, batch dispatch tasks failed due to only existed worker[%s] was down, size:%d, error=%s",
						m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, len(reqs), err.Error())
					m.batchUpdateTaskStatus(workerId, workerAddr, reqs)
					return
				}
				logger.Warnf("jobInstanceId=%d, worker[%s] is down, try another worker, size:%d",
					m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, len(reqs))

				// TODO: worker挂了，先移除该worker，再尝试发给另一个worker，这里暂时去掉，探活交给专门的线程去干这里不做判断;
				if !m.withCycleState(serial, func() {
					m.GetJobInstanceInfo().SetAllWorkers(utils.RemoveSliceElem(m.GetJobInstanceInfo().GetAllWorkers(), workerIdAddr))
				}) {
					return
				}

				// Send timeout, fallback to init status
				var taskIds []int64
				for _, req := range reqs {
					taskIds = append(taskIds, req.GetTaskId())
				}

				affectCnt, admitted, err := m.updatePersistedTasks(serial, m.GetJobInstanceInfo().GetJobInstanceId(), taskIds, taskstatus.TaskStatusInit, workerId, workerAddr)
				if !admitted {
					return
				}
				if err != nil {
					logger.Errorf("jobInstanceId=%d, timeout return init error", m.GetJobInstanceInfo().GetJobInstanceId())
					if m.cycleActive(serial) {
						m.UpdateNewInstanceStatus(serial, processor.InstanceStatusFailed, "timeout dispatch return init error")
					}
				}
				m.withCycleState(serial, func() {
					if val, ok := m.workerProgressMap.Load(workerAddr); ok {
						val.(*common.WorkerProgressCounter).DecrementRunning(affectCnt)
					}
				})
			} else {
				// If there are other exceptions (such as serialization failure, worker cannot be found), directly set the task to failure.
				logger.Errorf("jobInstanceId:%d, batch dispatch Tasks error, worker=%s, size:%d, error=%s", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, len(reqs), err.Error())
				m.batchUpdateTaskStatus(workerId, workerAddr, reqs)
			}
		}
	}
}

func (m *MapTaskMaster) batchUpdateTaskStatus(workerId, workerAddr string, reqs []*schedulerx.MasterStartContainerRequest) {
	serial := m.GetSerialNum()
	for _, req := range reqs {
		var failed *schedulerx.ContainerReportTaskStatusRequest
		if !m.withCycleState(serial, func() {
			if !m.acceptsTaskGeneration(req.Attempt, req.SerialNum) {
				return
			}
			if val, ok := m.taskProgressMap.Load(req.GetTaskName()); ok {
				val.(*common.TaskProgressCounter).IncrementOneFailed()
			}
			if val, ok := m.workerProgressMap.Load(workerAddr); ok {
				val.(*common.WorkerProgressCounter).IncrementOneFailed()
			}
			failed = &schedulerx.ContainerReportTaskStatusRequest{Attempt: req.Attempt, SerialNum: req.SerialNum, JobId: proto.Int64(m.jobInstanceInfo.GetJobId()), JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()), TaskId: req.TaskId, Status: proto.Int32(int32(taskstatus.TaskStatusFailed)), WorkerId: proto.String(workerId), WorkerAddr: proto.String(workerAddr), TaskName: req.TaskName}
		}) {
			return
		}
		if failed != nil {
			m.UpdateTaskStatus(failed)
		}
	}
}

func (m *MapTaskMaster) batchHandlePersistence(workerId, workerAddr string, reqs []*schedulerx.MasterStartContainerRequest, isFailover bool) bool {
	serial := m.GetSerialNum()
	hasRequests := false
	admitted := m.withTaskPersistence(serial, func() {
		accepted := m.filterDispatchRequests(reqs)
		if len(accepted) == 0 {
			return
		}
		hasRequests = true
		if !isFailover {
			if err := m.taskPersistence.CreateTasks(accepted, workerId, workerAddr); err != nil {
				logger.Errorf("CreateTasks failed: %s", err)
			}
		} else {
			ids := make([]int64, 0, len(accepted))
			for _, req := range accepted {
				ids = append(ids, req.GetTaskId())
			}
			if _, err := m.taskPersistence.UpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), ids, taskstatus.TaskStatusRunning, workerId, workerAddr); err != nil {
				logger.Errorf("failover UpdateTaskStatus failed: %s", err)
			}
		}
	})
	return admitted && hasRequests
}

// batchHandleRunningProgress is deprecated
func (m *MapTaskMaster) batchHandleRunningProgress(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest,
	worker2ReqsWithNormal map[string][]*schedulerx.MasterStartContainerRequest, worker2ReqsWithFailover map[string][]*schedulerx.MasterStartContainerRequest) {
	for _, request := range masterStartContainerRequests {
		workerIdAddr := m.selectWorker()
		if workerIdAddr == "" {
			m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, "all worker is down!")
			break
		}

		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		if request.GetFailover() {
			if _, ok := worker2ReqsWithFailover[workerIdAddr]; !ok {
				worker2ReqsWithFailover[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithFailover[workerIdAddr] = append(worker2ReqsWithFailover[workerIdAddr], request)
			}
		} else {
			if _, ok := worker2ReqsWithNormal[workerIdAddr]; !ok {
				worker2ReqsWithNormal[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithNormal[workerIdAddr] = append(worker2ReqsWithNormal[workerIdAddr], request)
			}
		}
		if val, ok := m.taskProgressMap.Load(request.GetTaskName()); ok {
			val.(*common.TaskProgressCounter).IncrementRunning()
		}
		if _, ok := m.workerProgressMap.Load(workerAddr); workerAddr != "" && !ok {
			m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		}
		if val, ok := m.workerProgressMap.Load(workerAddr); ok {
			val.(*common.WorkerProgressCounter).IncrementTotal()
			val.(*common.WorkerProgressCounter).IncrementRunning()
		}
	}
}

func (m *MapTaskMaster) BatchHandlePulledProgress(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest,
	remoteWorker string) (map[string][]*schedulerx.MasterStartContainerRequest, map[string][]*schedulerx.MasterStartContainerRequest) {
	serial := m.GetSerialNum()
	m.statusWriteMu.Lock()
	defer m.statusWriteMu.Unlock()
	var (
		worker2ReqsWithNormal   = make(map[string][]*schedulerx.MasterStartContainerRequest)
		worker2ReqsWithFailover = make(map[string][]*schedulerx.MasterStartContainerRequest)
	)
	for _, request := range masterStartContainerRequests {
		if m.statusWritesRetired || m.cycleClosed || !m.acceptsTaskGeneration(request.Attempt, request.SerialNum) {
			continue
		}
		workerIdAddr := remoteWorker
		if workerIdAddr == "" {
			workerIdAddr = m.selectWorker()
		}
		if workerIdAddr == "" {
			m.statusWriteMu.Unlock()
			if err := m.updateNewInstanceStatus(serial, m.GetJobInstanceInfo().GetJobInstanceId(),
				processor.InstanceStatusFailed, "all worker is down!"); err != nil {
				logger.Errorf("updateNewInstanceStatus failed in BatchHandlePulledProgress, err=%s", err.Error())
			}
			m.statusWriteMu.Lock()
			break
		}

		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		if request.GetFailover() {
			if _, ok := worker2ReqsWithFailover[workerIdAddr]; !ok {
				worker2ReqsWithFailover[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithFailover[workerIdAddr] = append(worker2ReqsWithFailover[workerIdAddr], request)
			}
		} else {
			if _, ok := worker2ReqsWithNormal[workerIdAddr]; !ok {
				worker2ReqsWithNormal[workerIdAddr] = []*schedulerx.MasterStartContainerRequest{request}
			} else {
				worker2ReqsWithNormal[workerIdAddr] = append(worker2ReqsWithNormal[workerIdAddr], request)
			}

			// The subtasks of failover do not need to be counted anymore
			if val, ok := m.taskProgressMap.Load(request.GetTaskName()); ok {
				val.(*common.TaskProgressCounter).IncrementOnePulled()
			}
		}

		if _, ok := m.workerProgressMap.Load(workerAddr); workerAddr != "" && !ok {
			m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		}
		if val, ok := m.workerProgressMap.Load(workerAddr); ok {
			val.(*common.WorkerProgressCounter).IncrementTotal()
			val.(*common.WorkerProgressCounter).IncrementPulled()
		}
	}
	return worker2ReqsWithNormal, worker2ReqsWithFailover
}

// BatchDispatchTasks dispatches tasks
func (m *MapTaskMaster) BatchDispatchTasks(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest, remoteWorker string) {
	worker2ReqsWithNormal, worker2ReqsWithFailover := m.BatchHandlePulledProgress(masterStartContainerRequests, remoteWorker)

	// Push model starts subtask normally
	for key, val := range worker2ReqsWithNormal {
		m.batchHandleContainers(key, val, false, common.TaskDispatchModePush)
	}

	// Push model worker hangs up, failover subtask to other workers
	for key, val := range worker2ReqsWithFailover {
		m.batchHandleContainers(key, val, true, common.TaskDispatchModePush)
	}
}

func (m *MapTaskMaster) BatchPullTasks(masterStartContainerRequests []*schedulerx.MasterStartContainerRequest, workerIdAddr string) {
	worker2ReqsWithNormal, worker2ReqsWithFailover := m.BatchHandlePulledProgress(masterStartContainerRequests, workerIdAddr)

	// Pull model persistence tasks
	for key, val := range worker2ReqsWithNormal {
		m.batchHandleContainers(key, val, false, common.TaskDispatchModePull)
	}

	// Pull model update tasks
	for key, val := range worker2ReqsWithFailover {
		m.batchHandleContainers(key, val, true, common.TaskDispatchModePull)
	}
}

func (m *MapTaskMaster) selectWorker() string {
	allWorkers := m.GetJobInstanceInfo().GetAllWorkers()
	size := len(allWorkers)
	if size == 0 {
		return ""
	} else if m.index >= size {
		m.index = m.index % size
	}
	worker := allWorkers[m.index]
	m.index++
	return worker
}

func (m *MapTaskMaster) KillInstance(reason string) error {
	serial := m.GetSerialNum()
	if !m.withCycleControl(serial, func() { _ = m.TaskMaster.KillInstance(reason) }) {
		return nil
	}

	allWorkers := m.GetJobInstanceInfo().GetAllWorkers()
	for _, workerIdAddr := range allWorkers {
		request := &schedulerx.MasterKillContainerRequest{
			JobId:                 proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			JobInstanceId:         proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			MayInterruptIfRunning: proto.Bool(false),
			SerialNum:             proto.Int64(serial),
			Attempt:               proto.Int32(m.GetJobInstanceInfo().GetAttempt()),
		}
		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		if !m.withCycleControl(serial, func() { m.actorCtx.Send(actorcomm.GetContainerRouterPid(workerAddr), request) }) {
			return nil
		}
	}
	return m.updateNewInstanceStatus(serial, m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, reason)
}

func (m *MapTaskMaster) DestroyContainerPool() {
	m.destroyCycleContainers(m.GetSerialNum())
}

func (m *MapTaskMaster) destroyCycleContainers(serial int64) {
	allWorkers := m.GetJobInstanceInfo().GetAllWorkers()
	for _, workerIdAddr := range allWorkers {
		request := &schedulerx.MasterDestroyContainerPoolRequest{
			JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			WorkerIdAddr:  proto.String(workerIdAddr),
			SerialNum:     proto.Int64(serial),
			Attempt:       proto.Int32(m.GetJobInstanceInfo().GetAttempt()),
		}
		if !m.sendCycleMessage(serial, actorcomm.AtLeastOnceDeliveryMsgReceiver(), &actorcomm.SchedulerWrappedMsg{Msg: request}, true) {
			return
		}
	}
}

func (m *MapTaskMaster) KillTask(uniqueId string, workerId string, workerAddr string) {
	var (
		serial       = m.GetSerialNum()
		workerIdAddr = fmt.Sprintf("%v%v%v", workerId, "@", workerAddr)

		jobInstanceId, err1 = utils.ParseId(uniqueId, utils.IdTypeJobInstanceId)
		jobId, err2         = utils.ParseId(uniqueId, utils.IdTypeJobId)
		taskId, err3        = utils.ParseId(uniqueId, utils.IdTypeTaskId)
	)
	if err1 != nil || err2 != nil || err3 != nil {
		logger.Errorf("send kill request exception due to invalid uniqueId=%d, workerIdAddr=%s", uniqueId, workerIdAddr)
		return
	}

	request := &schedulerx.MasterKillContainerRequest{
		JobInstanceId:         proto.Int64(jobInstanceId),
		JobId:                 proto.Int64(jobId),
		TaskId:                proto.Int64(taskId),
		MayInterruptIfRunning: proto.Bool(false),
		SerialNum:             proto.Int64(serial),
		Attempt:               proto.Int32(m.GetJobInstanceInfo().GetAttempt()),
	}
	m.withCycleControl(serial, func() { m.actorCtx.Send(actorcomm.GetContainerRouterPid(workerAddr), request) })
}

func (m *MapTaskMaster) GetJobInstanceProgress() (string, error) {
	m.statusWriteMu.RLock()
	defer m.statusWriteMu.RUnlock()
	detail := common.NewMapTaskProgress()

	var taskProgressCounters []*common.TaskProgressCounter
	m.taskProgressMap.Range(func(_, val interface{}) bool {
		taskProgressCounters = append(taskProgressCounters, val.(*common.TaskProgressCounter))
		return true
	})

	var workerProgressCounters []*common.WorkerProgressCounter
	m.workerProgressMap.Range(func(_, val interface{}) bool {
		workerProgressCounters = append(workerProgressCounters, val.(*common.WorkerProgressCounter))
		return true
	})

	detail.SetTaskProgress(taskProgressCounters)
	detail.SetWorkerProgress(workerProgressCounters)
	progress, err := json.Marshal(detail)
	if err != nil {
		return "", fmt.Errorf("Get jobInstance progress failed, marshal to json error, err=%s ", err.Error())
	}
	return string(progress), nil
}

func (m *MapTaskMaster) CheckProcessor() {
	// FIXME
	// if "java".equalsIgnoreCase(jobInstanceInfo.getJobType()) {
	//	processor := JavaProcessorProfileUtil.getJavaProcessor(jobInstanceInfo.getContent())
	//	if !ok {
	//		throw(NewIOException(processor.getClass().getName() + " must extends MapJobProcessor or MapReduceJobProcessor"))
	//	}
	// }
}

func (m *MapTaskMaster) PostFinish(jobInstanceId int64) *processor.ProcessResult {
	return m.postFinishAtCycle(jobInstanceId, m.GetSerialNum())
}

func (m *MapTaskMaster) postFinishAtCycle(jobInstanceID, serial int64) *processor.ProcessResult {
	return m.postFinish(jobInstanceID, serial)
}

func (m *MapTaskMaster) postFinish(jobInstanceID, serial int64) *processor.ProcessResult {
	jobCtx := new(jobcontext.JobContext)
	var jobProcessor interface{}
	var found bool
	if !m.withCycleState(serial, func() {
		info := m.GetJobInstanceInfo()
		jobCtx.SetJobId(info.GetJobId())
		jobCtx.SetJobInstanceId(jobInstanceID)
		jobCtx.SetJobType(info.GetJobType())
		jobCtx.SetContent(info.GetContent())
		jobCtx.SetScheduleTime(info.GetScheduleTime())
		jobCtx.SetDataTime(info.GetDataTime())
		jobCtx.SetJobParameters(info.GetParameters())
		jobCtx.SetInstanceParameters(info.GetInstanceParameters())
		jobCtx.SetUser(info.GetUser())
		jobCtx.SetTriggerType(info.GetTriggerType())
		jobCtx.SetAttempt(info.GetAttempt())
		jobCtx.SetSerialNum(serial)
		results := make(map[int64]string)
		m.taskResultMap.Range(func(k, v any) bool { results[k.(int64)] = v.(string); return true })
		jobCtx.SetTaskResults(results)
		statuses := make(map[int64]taskstatus.TaskStatus)
		m.taskStatusMap.Range(func(k, v any) bool { statuses[k.(int64)] = v.(taskstatus.TaskStatus); return true })
		jobCtx.SetTaskStatuses(statuses)
		name := gjson.Get(jobCtx.Content(), "jobName").String()
		if jobCtx.JobType() == "java" {
			name = gjson.Get(jobCtx.Content(), "className").String()
		}
		jobProcessor, found = masterpool.GetTaskMasterPool().Tasks().Find(name)
	}) {
		return nil
	}
	if !found {
		return processor.NewProcessResult(processor.WithFailed(), processor.WithResult("job processor not registered for reduce"))
	}
	mp, ok := jobProcessor.(processor.MapReduceJobProcessor)
	if !ok {
		return nil
	}
	runOnFailure := mp.RunReduceIfFail(jobCtx)
	workerAddr := m.actorCtx.ActorSystem().Address()
	if !m.withCycleState(serial, func() {
		if m.GetInstanceStatus() == processor.InstanceStatusFailed && !runOnFailure {
			return
		}
		taskCounter, _ := m.taskProgressMap.LoadOrStore(constants.ReduceTaskName, common.NewTaskProgressCounter(constants.ReduceTaskName))
		taskCounter.(*common.TaskProgressCounter).IncrementOneTotal()
		taskCounter.(*common.TaskProgressCounter).IncrementRunning()
		workerCounter, _ := m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		workerCounter.(*common.WorkerProgressCounter).IncrementTotal()
		workerCounter.(*common.WorkerProgressCounter).IncrementRunning()
	}) {
		return nil
	}
	if m.GetInstanceStatus() == processor.InstanceStatusFailed && !runOnFailure {
		return nil
	}
	tr := tracer.GetTracerOrDefault()
	jobCtx = tr.Start(jobCtx)
	if !m.cycleActive(serial) {
		tr.End(jobCtx, processor.NewProcessResult(processor.WithFailed()))
		return nil
	}
	result, err := mp.Reduce(jobCtx)
	if err != nil {
		result = processor.NewProcessResult(processor.WithFailed(), processor.WithResult("reduce exception: "+err.Error()))
	}
	result = tr.End(jobCtx, result)
	if !m.withCycleState(serial, func() {
		if counter, ok := m.taskProgressMap.Load(constants.ReduceTaskName); ok {
			if result.Status() == processor.InstanceStatusSucceed {
				counter.(*common.TaskProgressCounter).IncrementOneSuccess()
			} else {
				counter.(*common.TaskProgressCounter).IncrementOneFailed()
			}
		}
		if counter, ok := m.workerProgressMap.Load(workerAddr); ok {
			if result.Status() == processor.InstanceStatusSucceed {
				counter.(*common.WorkerProgressCounter).IncrementSuccess()
			} else {
				counter.(*common.WorkerProgressCounter).IncrementOneFailed()
			}
		}
	}) {
		return nil
	}
	return result
}

// RetireStatusWrites fences future status I/O and joins any write already in
// progress before the pool can expose a replacement using the same task rows.
func (m *MapTaskMaster) RetireStatusWrites() {
	m.retireOnce.Do(func() {
		if m.retireCh != nil {
			close(m.retireCh)
		}
	})
	m.statusWriteMu.Lock()
	m.statusWritesRetired = true
	cancel := m.cycleCancel
	m.statusWriteMu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	m.stopCycleHandlers()
}

func (m *MapTaskMaster) stopCycleHandlers() {
	if m.taskDispatchReqHandler != nil {
		m.taskDispatchReqHandler.Stop()
	}
	if m.taskStatusReqBatchHandler != nil {
		m.taskStatusReqBatchHandler.Stop()
	}
	if h, ok := m.statusHandler.(interface{ stop() }); ok {
		h.stop()
	}
}

// A finalizer must not stop the next cycle on the same owner.
func (m *MapTaskMaster) stopCycle(serial int64) bool {
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	if serial != m.GetSerialNum() || m.retirementStarted() {
		return false
	}
	m.retireOnce.Do(func() {
		if m.retireCh != nil {
			close(m.retireCh)
		}
	})
	m.statusWriteMu.Lock()
	m.statusWritesRetired = true
	cancel := m.cycleCancel
	m.statusWriteMu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.stopCycleHandlers()
	return true
}

func (m *MapTaskMaster) persistTaskStatuses(requests []*schedulerx.ContainerReportTaskStatusRequest) (err error, admitted bool) {
	admitted = m.withTaskPersistence(m.GetSerialNum(), func() {
		accepted := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(requests))
		for _, req := range requests {
			if m.acceptsTaskGeneration(req.Attempt, req.SerialNum) {
				accepted = append(accepted, req)
			}
		}
		if len(accepted) > 0 {
			err = m.taskPersistence.UpdateTaskStatues(accepted)
		}
	})
	return
}

func (m *MapTaskMaster) clearPersistedTasks(jobInstanceID int64, serials ...int64) (error, bool) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	// Clear may start the next second-job cycle on the same Master. Serialize
	// its I/O with status writes without permanently retiring that owner.
	m.statusWriteMu.Lock()
	defer m.statusWriteMu.Unlock()
	if m.statusWritesRetired || serial != m.GetSerialNum() {
		return nil, false
	}
	return m.taskPersistence.ClearTasks(jobInstanceID), true
}

func (m *MapTaskMaster) Stop() {
	m.RetireStatusWrites()
	logger.Infof("jobInstanceId:%v, instance master stop succeed.", m.GetJobInstanceInfo().GetJobInstanceId())
}

func (m *MapTaskMaster) startBatchHandler(serials ...int64) error {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	m.statusWriteMu.Lock()
	if m.retirementStarted() || m.statusWritesRetired || serial != m.GetSerialNum() || (m.cycleClosed && serial == m.closedSerial) {
		m.statusWriteMu.Unlock()
		return fmt.Errorf("master cycle is inactive")
	}
	if m.cycleClosed {
		m.cycleCtx, m.cycleCancel = context.WithCancel(context.Background())
	}
	m.cycleClosed = false
	m.statusWriteMu.Unlock()
	// FIXME
	// if m.IsInited() {
	//	return nil
	// }
	// start batch handlers
	if err := m.taskStatusReqBatchHandler.Start(m.taskStatusReqBatchHandler); err != nil {
		return err
	}

	// m.taskBlockingQueue = batch.NewReqQueue(m.queueSize)

	if m.xAttrs != nil && m.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePush) {
		m.taskDispatchReqHandler.SetWorkThreadNum(int(m.dispatcherSize))
		m.taskDispatchReqHandler.SetDispatchSize(int(m.pageSize) * len(m.GetJobInstanceInfo().GetAllWorkers()))
		m.taskDispatchReqHandler.Start(m.taskDispatchReqHandler)
	}
	return nil
}

func (m *MapTaskMaster) getTotalPulledAndRunning() int64 {
	var total int64
	taskCounters := make([]*common.TaskProgressCounter, 0, 10)
	m.taskProgressMap.Range(func(key, value any) bool {
		taskCounters = append(taskCounters, value.(*common.TaskProgressCounter))
		return true
	})
	for _, taskProgressCounter := range taskCounters {
		total += taskProgressCounter.GetPulled()
		total += taskProgressCounter.GetRunning()
	}
	return total
}

func (m *MapTaskMaster) GetRootTaskResult() string {
	return m.rootTaskResult
}

func (m *MapTaskMaster) SetRootTaskResult(rootTaskResult string) {
	m.rootTaskResult = rootTaskResult
}

func (m *MapTaskMaster) initTaskProgress(taskName string, delta int) {
	taskProgressCounter, _ := m.taskProgressMap.LoadOrStore(taskName, common.NewTaskProgressCounter(taskName))
	taskProgressCounter.(*common.TaskProgressCounter).IncrementTotal(int64(delta))
}

func (m *MapTaskMaster) SyncPullTasks(pageSize int32, workerIdAddr string, serials ...int64) []*schedulerx.MasterStartContainerRequest {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	reqs := m.taskDispatchReqHandler.SyncHandleReqsWithAdmission(m.taskDispatchReqHandler, pageSize, workerIdAddr, func(take func()) bool {
		return m.withCycleState(serial, func() {
			if m.getTotalPulledAndRunning() < int64(m.xAttrs.GetGlobalConsumerSize()) {
				take()
			}
		})
	})
	if !m.cycleActive(serial) {
		return nil
	}
	ret := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
	for _, req := range reqs {
		ret = append(ret, req.(*schedulerx.MasterStartContainerRequest))
	}
	return m.filterDispatchRequests(ret)
}

func (m *MapTaskMaster) Clear(taskMaster taskmaster.TaskMaster) {
	m.clearCycle(taskMaster, m.GetSerialNum())
}

func (m *MapTaskMaster) clearCycle(taskMaster taskmaster.TaskMaster, serial int64) bool {
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	// Cancel before joining local sends; transitions serialize the serial check.
	if serial != m.GetSerialNum() || m.retirementStarted() || m.cycleClosed {
		return false
	}
	// cycleCancel is written only under cycleTransitionMu. Do not acquire a
	// read lock here: a pending writer may itself be waiting for the sender
	// which this cancellation must release.
	cancel := m.cycleCancel
	if cancel != nil {
		cancel()
	}
	m.statusWriteMu.Lock()
	if m.statusWritesRetired {
		m.statusWriteMu.Unlock()
		return false
	}
	m.cycleClosed = true
	m.closedSerial = serial
	m.statusWriteMu.Unlock()
	// The gate has joined the canceled cycle's local operations.

	m.TaskMaster.Clear(taskMaster)
	if m.taskStatusReqQueue != nil {
		m.taskStatusReqQueue.Clear()
	}
	if m.taskBlockingQueue != nil {
		m.taskBlockingQueue.Clear()
	}
	// Stop batch handlers to terminate their goroutines, preventing goroutine
	// leaks across second-delay cycles. Stop() sends the stop signal AND clears
	// the queue, so a separate Clear() call is unnecessary.
	if m.taskDispatchReqHandler != nil {
		m.taskDispatchReqHandler.Stop()
	}
	if m.taskStatusReqBatchHandler != nil {
		m.taskStatusReqBatchHandler.Stop()
	}
	m.statusWriteMu.Lock()
	m.taskResultMap = sync.Map{}
	m.taskStatusMap = sync.Map{}
	m.taskProgressMap = sync.Map{}
	m.workerProgressMap = sync.Map{}
	if extra, ok := taskMaster.(interface{ resetCycleState() }); ok {
		extra.resetCycleState()
	}
	m.statusWriteMu.Unlock()
	m.clearTasks(m.GetJobInstanceInfo().GetJobInstanceId())
	m.statusWriteMu.Lock()
	m.taskCounter = atomic.NewInt64(0)
	m.statusWriteMu.Unlock()
	// Reset sync.Once so the next cycle can re-init goroutines
	m.once = sync.Once{}
	// Reset ctx for the next cycle (a new ctx will be created in init())
	m.statusWriteMu.Lock()
	m.cycleCtx = context.Background()
	m.cycleCancel = func() {}
	m.statusWriteMu.Unlock()
	return true
}

func (m *MapTaskMaster) GetTaskProgressMap() *sync.Map {
	return &m.taskProgressMap
}

func (m *MapTaskMaster) handleWorkerShutdown(workerIdAddr string, serials ...int64) {
	serial := m.GetSerialNum()
	if len(serials) > 0 {
		serial = serials[0]
	}
	if !m.withCycleState(serial, func() {
		m.GetAliveCheckWorkerSet().Remove(workerIdAddr)
		m.GetJobInstanceInfo().SetAllWorkers(utils.RemoveSliceElem(m.GetJobInstanceInfo().GetAllWorkers(), workerIdAddr))
		m.taskDispatchReqHandler.SetDispatchSize(m.GetAliveCheckWorkerSet().Len() * int(m.pageSize))
	}) {
		return
	}

	parts := strings.Split(workerIdAddr, "@")
	workerId := parts[0]
	workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
	if config.GetWorkerConfig().IsMapMasterFailover() {
		// If failover is enabled, set it to init state and wait for re-pull.
		affectCnt, admitted := m.batchUpdatePersistedTasks(serial, m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusInit, workerId, workerAddr)
		if !admitted {
			return
		}
		logger.Warnf("jobInstanceId=%d, failover task number:%d, workerId:%s, workerAddr:%s", m.GetJobInstanceInfo().GetJobInstanceId(), affectCnt, workerId, workerAddr)
		m.withCycleState(serial, func() {
			if affectCnt > 0 {
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).DecrementRunning(affectCnt)
				}
			}
		})
	} else {
		// If failover is not enabled, the subtask on this worker will be directly set to failure.
		affectCnt, admitted := m.batchUpdatePersistedTasks(serial, m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusFailed, workerId, workerAddr)
		if !admitted {
			return
		}
		logger.Warnf("jobInstanceId=%d, failover task number:%d, workerId:%s, workerAddr:%s", m.GetJobInstanceInfo().GetJobInstanceId(), affectCnt, workerId, workerAddr)
		m.withCycleState(serial, func() {
			if affectCnt > 0 {
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).IncrementFailed(affectCnt)
				}
			}
		})
	}
}

// AcquireSerialNum joins storage writes and cycle cleanup before advancing.
func (m *MapTaskMaster) AcquireSerialNum() int64 {
	m.cycleTransitionMu.Lock()
	defer m.cycleTransitionMu.Unlock()
	m.statusWriteMu.Lock()
	defer m.statusWriteMu.Unlock()
	return m.TaskMaster.AcquireSerialNum()
}
