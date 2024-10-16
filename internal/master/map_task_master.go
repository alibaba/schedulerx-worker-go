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
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/batch"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/persistence"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/remoting/pool"
	"github.com/alibaba/schedulerx-worker-go/internal/tasks"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var (
	_ taskmaster.MapTaskMaster = &MapTaskMaster{}
)

type MapTaskMaster struct {
	*TaskMaster
	actorCtx              actor.Context
	taskMasterPoolCleaner func(int64)
	tasks                 *tasks.TaskMap
	connpool              pool.ConnPool
	pageSize              int32
	queueSize             int64
	dispatcherSize        int64
	index                 int
	taskStatusReqQueue    *batch.ReqQueue
	// task batch reporting queue, item: ContainerReportTaskStatusRequest
	taskStatusReqBatchHandler *batch.TMStatusReqHandler
	// Subtask memory cache queue, the push model is pulled and pushed actively through TaskDispatchReqHandler,
	// and the pull model is pulled through PullThread
	taskBlockingQueue      *batch.ReqQueue
	taskDispatchReqHandler batch.TaskDispatchReqHandler
	// Handle task failure separately
	rootTaskResult  string
	taskPersistence persistence.TaskPersistence
	// map[string]*common.TaskProgressCounter
	taskProgressMap *sync.Map
	// map[string]*common.WorkerProgressCounter
	workerProgressMap   *sync.Map
	taskResultMap       map[int64]string
	taskStatusMap       map[int64]taskstatus.TaskStatus
	xAttrs              *common.MapTaskXAttrs
	taskCounter         *atomic.Int64
	localTaskRouterPath string
	once                sync.Once
}

func NewMapTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) taskmaster.TaskMaster {
	var (
		connpool              = pool.GetConnPool()
		taskMasterPool        = masterpool.GetTaskMasterPool()
		taskMasterPoolCleaner = func(jobInstanceId int64) {
			taskMasterPool.Get(jobInstanceId).Stop()
			taskMasterPool.Remove(jobInstanceId)
		}
	)
	mapTaskMaster := &MapTaskMaster{
		actorCtx:              actorCtx,
		taskMasterPoolCleaner: taskMasterPoolCleaner,
		tasks:                 taskMasterPool.Tasks(),
		connpool:              connpool,
		pageSize:              config.GetWorkerConfig().MapMasterPageSize(),
		queueSize:             int64(config.GetWorkerConfig().MapMasterQueueSize()),
		dispatcherSize:        int64(config.GetWorkerConfig().MapMasterDispatcherSize()),
		taskProgressMap:       new(sync.Map),
		workerProgressMap:     new(sync.Map),
		taskResultMap:         make(map[int64]string),
		taskStatusMap:         make(map[int64]taskstatus.TaskStatus),
		taskCounter:           atomic.NewInt64(0),
		localTaskRouterPath:   actorCtx.ActorSystem().Address(),
		// taskStatusReqQueue:    batch.NewReqQueue(100000),
		// taskBlockingQueue:     batch.NewReqQueue(100000),
	}

	statusHandler := NewCommonUpdateInstanceStatusHandler(actorCtx, mapTaskMaster, jobInstanceInfo)
	if utils.IsSecondTypeJob(common.TimeType(jobInstanceInfo.GetTimeType())) {
		statusHandler = NewSecondJobUpdateInstanceStatusHandler(actorCtx, mapTaskMaster, jobInstanceInfo)
	}
	mapTaskMaster.TaskMaster = NewTaskMaster(actorCtx, jobInstanceInfo, statusHandler)

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

func (m *MapTaskMaster) init() {
	m.once.Do(func() {
		m.TaskMaster.Init()
		jobIdAndInstanceId := strconv.FormatInt(m.GetJobInstanceInfo().GetJobId(), 10) + "_" + strconv.FormatInt(m.GetJobInstanceInfo().GetJobInstanceId(), 10)
		logger.Infof("jobInstanceId=%d, map master config, pageSize:%d, queueSize:%d, dispatcherSize:%d, workerSize:%d",
			jobIdAndInstanceId, m.pageSize, m.queueSize, m.dispatcherSize, len(m.GetJobInstanceInfo().GetAllWorkers()))

		// pull
		go m.pullTask(jobIdAndInstanceId)

		// status check
		go m.checkInstanceStatus()

		// job instance progress report
		if !utils.IsSecondTypeJob(common.TimeType(m.GetJobInstanceInfo().GetTimeType())) {
			go m.reportJobInstanceProgress()
		}

		// worker alive check thread
		go m.checkWorkerAlive()

		// PULL_MODEL specially
		//		if m.xAttrs != nil && m.xAttrs.GetTaskDispatchMode() == string(common.TaskDispatchModePull) {
		//			go m.notifyWorkerPull()
		//		}
	})
}

func (m *MapTaskMaster) pullTask(jobIdAndInstanceId string) {
	for !m.GetInstanceStatus().IsFinished() {
		jobInstanceId := m.GetJobInstanceInfo().GetJobInstanceId()
		startTime := time.Now()
		taskInfos, err := m.taskPersistence.Pull(jobInstanceId, m.pageSize)
		if err != nil && errors.Is(err, persistence.ErrTimeout) {
			logger.Errorf("pull task timeout, uniqueId: %s", jobIdAndInstanceId)
			time.Sleep(10 * time.Second)
			continue
		}
		logger.Debugf("jobInstanceId=%d, pull cost=%dms", jobInstanceId, time.Since(startTime).Milliseconds())
		if len(taskInfos) == 0 {
			logger.Debugf("pull task empty of jobInstanceId=%d, sleep 10s ...", jobInstanceId)
			time.Sleep(10 * time.Second)
		} else {
			for _, taskInfo := range taskInfos {
				taskName := taskInfo.TaskName()
				if counter, ok := m.taskProgressMap.LoadOrStore(taskName, common.NewTaskProgressCounter(taskName)); ok {
					counter.(*common.TaskProgressCounter).DecrementRunning()
				}
				req, err := m.convert2StartContainerRequest(m.GetJobInstanceInfo(), taskInfo.TaskId(), taskInfo.TaskName(), taskInfo.TaskBody(), true)
				if err != nil {
					errMsg := fmt.Sprintf("mapTaskMaster pull task failed, jobInstanceInfo=%+v, taskId=%d, taskName=%s, err=%s.", m.GetJobInstanceInfo(), taskInfo.TaskId(), taskInfo.TaskName(), err.Error())
					logger.Errorf(errMsg)
					m.updateNewInstanceStatus(m.GetSerialNum(), jobInstanceId, processor.InstanceStatusFailed, errMsg)
					break // FIXME break or continue?
				}
				if m.taskBlockingQueue != nil {
					m.taskBlockingQueue.SubmitRequest(req)
				}
			}
		}
	}
}

func (m *MapTaskMaster) checkInstanceStatus() {
	checkInterval := config.GetWorkerConfig().MapMasterStatusCheckInterval()
	for !m.GetInstanceStatus().IsFinished() {
		time.Sleep(checkInterval)
		newStatus := m.taskPersistence.CheckInstanceStatus(m.GetJobInstanceInfo().GetJobInstanceId())
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
			time.Sleep(checkInterval)
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

		if err := m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), newStatus, result); err != nil {
			logger.Errorf("updateNewInstanceStatus failed, serialNum=%v, jobInstanceId=%v, newStatus=%v, result=%v, err=%s",
				m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), newStatus, result, err.Error())
		}
	}
}

func (m *MapTaskMaster) reportJobInstanceProgress() {
	for !m.GetInstanceStatus().IsFinished() {
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
		actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: req,
		}

		time.Sleep(5 * time.Second)
	}
}

func (m *MapTaskMaster) checkWorkerAlive() {
	for !m.GetInstanceStatus().IsFinished() {
		for _, worker := range m.GetJobInstanceInfo().GetAllWorkers() {
			m.aliveCheckWorkerSet.Add(worker)
		}
		if m.aliveCheckWorkerSet.Len() == 0 {
			logger.Warnf("worker list is empty, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId())
			m.taskPersistence.BatchUpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusFailed, "", "")
			break
		} else {
			for _, workerIdAddr := range m.aliveCheckWorkerSet.ToStringSlice() {
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
						time.Sleep(5 * time.Second)
						times++
					}
				}
				if times >= 3 {
					logger.Warnf("worker[%s] is down, start to remove this worker and failover tasks, jobInstanceId=%d", workerIdAddr, m.GetJobInstanceInfo().GetJobInstanceId())
					m.handleWorkerShutdown(workerIdAddr)
					continue
				}

				request := &schedulerx.MasterCheckWorkerAliveRequest{
					JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
					DispatchMode:  proto.String(m.xAttrs.GetTaskDispatchMode()),
				}
				response, err := m.actorContext.RequestFuture(actorcomm.GetHeartbeatActorPid(workerAddr), request, 10*time.Second).Result()
				if err != nil {
					logger.Errorf("check worker error, jobInstanceId=%d, err=%s", m.GetJobInstanceInfo().GetJobInstanceId(), err.Error())
					break
				}

				if resp := response.(*schedulerx.MasterCheckWorkerAliveResponse); !resp.GetSuccess() {
					logger.Warnf("jobInstanceId=%d of worker=%s is not alive, remote worker resp=%+v", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, resp.GetMessage())
					m.handleWorkerShutdown(workerIdAddr)

					// destroy containers of worker of PullModel
					destroyContainerPoolRequest := &schedulerx.MasterDestroyContainerPoolRequest{
						JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
						JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
						WorkerIdAddr:  proto.String(workerAddr),
						SerialNum:     proto.Int64(m.GetSerialNum()),
					}
					actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
						Msg: destroyContainerPoolRequest,
					}
				}
			}

			// Worker detection is performed every 10 seconds
			time.Sleep(10 * time.Second)
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
				logger.Errorf("notify worker pull failed, jobInstanceId=%d", m.GetJobInstanceInfo().GetJobInstanceId(), errorMsg)
				m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, errorMsg)
			}
		}

		time.Sleep(5 * time.Second)
	}
}

func (m *MapTaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	var err error
	defer func() {
		if err != nil {
			errMsg := fmt.Sprintf("Submit instance failed, err=%s", err.Error())
			logger.Errorf(errMsg)
			m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, errMsg)
		}
	}()

	startTime := time.Now()
	if m.dispatcherSize > constants.MapMasterDispatcherSizeMax {
		m.dispatcherSize = constants.MapMasterDispatcherSizeMax
	}
	if err = m.startBatchHandler(); err != nil {
		return err
	}
	if err = m.createRootTask(); err != nil {
		return err
	}
	logger.Infof("jobInstanceId=%d create root task, cost=%dms", jobInstanceInfo.GetJobInstanceId(), time.Since(startTime).Milliseconds())
	m.init()

	return err
}

func (m *MapTaskMaster) UpdateTaskStatus(request *schedulerx.ContainerReportTaskStatusRequest) error {
	m.taskStatusReqQueue.SubmitRequest(request)
	return nil
}

func (m *MapTaskMaster) BatchUpdateTaskStatues(requests []*schedulerx.ContainerReportTaskStatusRequest) {
	finalTaskStatus := make(map[int64]*schedulerx.ContainerReportTaskStatusRequest)
	for _, request := range requests {
		taskStatus := taskstatus.TaskStatus(request.GetStatus())

		// Filter intermediate states
		if _, ok := finalTaskStatus[request.GetTaskId()]; !ok || taskStatus.IsFinished() {
			finalTaskStatus[request.GetTaskId()] = request
		}
		var (
			workerAddr = request.GetWorkerAddr()
			taskName   = request.GetTaskName()
		)

		logger.Debugf("report task status:%s from worker:%s, uniqueId:%d", taskStatus.Descriptor(), workerAddr,
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
		m.taskResultMap[request.GetTaskId()] = request.GetResult()
		m.taskStatusMap[request.GetTaskId()] = taskStatus
	}

	startTime := time.Now()

	// Return the reason for the failure of the root task node.
	// It is possible to return two root task requests at the same time, which are running and failed statuses, take the last one.
	idx := len(requests) - 1
	if idx >= 0 && requests[idx].GetStatus() == int32(taskstatus.TaskStatusFailed) && requests[idx].GetTaskName() == constants.MapTaskRootName {
		m.SetRootTaskResult(requests[idx].GetResult())
	}

	updateSuccess := false
	allTaskStatus := make([]*schedulerx.ContainerReportTaskStatusRequest, 0, len(finalTaskStatus))
	for _, value := range finalTaskStatus {
		allTaskStatus = append(allTaskStatus, value)
	}
	for i := 0; i < 3; i++ {
		// try 3 times
		// FIXME if need 3 times?
		if err := m.taskPersistence.UpdateTaskStatues(allTaskStatus); err != nil {
			logger.Errorf("jobInstanceId=%d, persistent batch updateTaskStatus error, err=%s", m.jobInstanceInfo.GetJobInstanceId(), err.Error())
			continue
		}
		updateSuccess = true
		break
	}
	if !updateSuccess {
		m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, "persistent batch update TaskStatus error up to 3 times")
	}
	logger.Debugf("jobInstanceId=%d batch update status db cost %dms", m.GetJobInstanceInfo().GetJobInstanceId(), time.Since(startTime).Milliseconds())
}

func (m *MapTaskMaster) Map(jobCtx *jobcontext.JobContext, taskList [][]byte, taskName string) (bool, error) {
	logger.Debugf("map taskName:%s, size:%d", taskName, len(taskList))
	m.initTaskProgress(taskName, len(taskList))
	for _, taskBody := range taskList {
		startContainerRequest, err := m.convert2StartContainerRequest(m.GetJobInstanceInfo(), m.AcquireTaskId(), taskName, taskBody, false)
		if err != nil {
			return false, fmt.Errorf("convert2StartContainerRequest failed, err=%s", err.Error())
		}
		m.taskBlockingQueue.SubmitRequest(startContainerRequest)
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
	m.taskPersistence.ClearTasks(jobInstanceId)
	logger.Infof("jobInstanceId=%d clearTasks success.", jobInstanceId)
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
	m.batchHandlePersistence(workerId, workerAddr, reqs, isFailover)
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

		future := m.actorCtx.RequestFuture(containerRouterActorPid, req, 15*time.Second)
		result, err := future.Result()
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
				m.GetJobInstanceInfo().SetAllWorkers(utils.RemoveSliceElem(m.GetJobInstanceInfo().GetAllWorkers(), workerIdAddr))

				// Send timeout, fallback to init status
				var taskIds []int64
				for _, req := range reqs {
					taskIds = append(taskIds, req.GetTaskId())
				}

				affectCnt, err := m.taskPersistence.UpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), taskIds, taskstatus.TaskStatusInit, workerId, workerAddr)
				if err != nil {
					logger.Errorf("jobInstanceId=%d, timeout return init error", m.GetJobInstanceInfo().GetJobInstanceId())
					m.UpdateNewInstanceStatus(m.GetSerialNum(), processor.InstanceStatusFailed, "timeout dispatch return init error")
				}
				if val, ok := m.workerProgressMap.Load(workerAddr); ok {
					val.(*common.WorkerProgressCounter).DecrementRunning(affectCnt)
				}
			} else {
				// If there are other exceptions (such as serialization failure, worker cannot be found), directly set the task to failure.
				logger.Errorf("jobInstanceId:%d, batch dispatch Tasks error, worker=%s, size:%d, error=%s", m.GetJobInstanceInfo().GetJobInstanceId(), workerIdAddr, len(reqs), err.Error())
				m.batchUpdateTaskStatus(workerId, workerAddr, reqs)
			}
		}
	}
}

func (m *MapTaskMaster) batchUpdateTaskStatus(workerId, workerAddr string, reqs []*schedulerx.MasterStartContainerRequest) {
	for _, req := range reqs {
		if val, ok := m.taskProgressMap.Load(req.GetTaskName()); ok {
			val.(*common.TaskProgressCounter).IncrementOneFailed()
		}
		if val, ok := m.workerProgressMap.Load(workerAddr); ok {
			val.(*common.WorkerProgressCounter).IncrementOneFailed()
		}
		failedStatusRequest := &schedulerx.ContainerReportTaskStatusRequest{
			JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			TaskId:        proto.Int64(req.GetTaskId()),
			Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
			WorkerId:      proto.String(workerId),
			TaskName:      proto.String(req.GetTaskName()),
			WorkerAddr:    proto.String(workerAddr),
		}
		m.UpdateTaskStatus(failedStatusRequest)
	}
}

func (m *MapTaskMaster) batchHandlePersistence(workerId, workerAddr string, reqs []*schedulerx.MasterStartContainerRequest, isFailover bool) {
	startTime := time.Now()
	if !isFailover {
		// first dispatch
		if err := m.taskPersistence.CreateTasks(reqs, workerId, workerAddr); err != nil {
			logger.Errorf("Batch persistence tasks to DB by CreateTasks failed, err=%s, reqs len=%d, workerId=%v, workerAddr=%v", err.Error(), len(reqs), workerId, workerAddr)
		}
	} else {
		// failover, not first dispatch
		taskIds := make([]int64, 0, len(reqs))
		for _, req := range reqs {
			taskIds = append(taskIds, req.GetTaskId())
		}
		_, err := m.taskPersistence.UpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), taskIds, taskstatus.TaskStatusRunning, workerId, workerAddr)
		if err != nil {
			logger.Errorf("Batch persistence tasks to DB by UpdateTaskStatus failed, err=%s, jobInstanceId=%d, tasks len=%d, workerId=%v, workerAddr=%v", err.Error(), m.GetJobInstanceInfo().GetJobInstanceId(), len(taskIds), workerId, workerAddr)
		}
	}
	logger.Debugf("jobInstance=%d, batch dispatch db cost:%dms, size:%d", m.GetJobInstanceInfo().GetJobInstanceId(), time.Since(startTime).Milliseconds(), len(reqs))
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
				worker2ReqsWithNormal[workerIdAddr] = append(worker2ReqsWithFailover[workerIdAddr], request)
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
	var (
		worker2ReqsWithNormal   = make(map[string][]*schedulerx.MasterStartContainerRequest)
		worker2ReqsWithFailover = make(map[string][]*schedulerx.MasterStartContainerRequest)
	)
	for _, request := range masterStartContainerRequests {
		workerIdAddr := remoteWorker
		if workerIdAddr == "" {
			workerIdAddr = m.selectWorker()
		}
		if workerIdAddr == "" {
			if err := m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(),
				processor.InstanceStatusFailed, "all worker is down!"); err != nil {
				logger.Errorf("updateNewInstanceStatus failed in BatchHandlePulledProgress, err=%s", err.Error())
			}
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
	m.TaskMaster.KillInstance(reason)
	allWorkers := m.GetJobInstanceInfo().GetAllWorkers()
	for _, workerIdAddr := range allWorkers {
		request := &schedulerx.MasterKillContainerRequest{
			JobId:                 proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			JobInstanceId:         proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			MayInterruptIfRunning: proto.Bool(false),
		}
		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		m.actorCtx.Send(actorcomm.GetContainerRouterPid(workerAddr), request)
	}
	return m.updateNewInstanceStatus(m.GetSerialNum(), m.GetJobInstanceInfo().GetJobInstanceId(), processor.InstanceStatusFailed, reason)
}

func (m *MapTaskMaster) DestroyContainerPool() {
	allWorkers := m.GetJobInstanceInfo().GetAllWorkers()
	for _, workerIdAddr := range allWorkers {
		request := &schedulerx.MasterDestroyContainerPoolRequest{
			JobInstanceId: proto.Int64(m.GetJobInstanceInfo().GetJobInstanceId()),
			JobId:         proto.Int64(m.GetJobInstanceInfo().GetJobId()),
			WorkerIdAddr:  proto.String(workerIdAddr),
			SerialNum:     proto.Int64(m.GetSerialNum()),
		}
		actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: request,
		}
	}
}

func (m *MapTaskMaster) KillTask(uniqueId string, workerId string, workerAddr string) {
	var (
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
	}
	m.actorCtx.Send(actorcomm.GetContainerRouterPid(workerAddr), request)
}

func (m *MapTaskMaster) GetJobInstanceProgress() (string, error) {
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
	var reduceResult *processor.ProcessResult
	jobCtx := new(jobcontext.JobContext)
	jobCtx.SetJobId(m.GetJobInstanceInfo().GetJobId())
	jobCtx.SetJobInstanceId(jobInstanceId)
	jobCtx.SetJobType(m.GetJobInstanceInfo().GetJobType())
	jobCtx.SetContent(m.GetJobInstanceInfo().GetContent())
	jobCtx.SetScheduleTime(m.GetJobInstanceInfo().GetScheduleTime())
	jobCtx.SetDataTime(m.GetJobInstanceInfo().GetDataTime())
	jobCtx.SetJobParameters(m.GetJobInstanceInfo().GetParameters())
	jobCtx.SetInstanceParameters(m.GetJobInstanceInfo().GetInstanceParameters())
	jobCtx.SetUser(m.GetJobInstanceInfo().GetUser())
	jobCtx.SetTaskResults(m.taskResultMap)
	jobCtx.SetTaskStatuses(m.taskStatusMap)

	jobName := gjson.Get(jobCtx.Content(), "jobName").String()
	// Compatible with the existing Java language configuration mechanism
	if jobCtx.JobType() == "java" {
		jobName = gjson.Get(jobCtx.Content(), "className").String()
	}
	jobProcessor, ok := masterpool.GetTaskMasterPool().Tasks().Find(jobName)
	if !ok {
		reduceResult = processor.NewProcessResult()
		reduceResult.SetFailed()
		reduceResult.SetResult(fmt.Sprintf("job=%s can not cast to MapReduceJobProcessor, must implement MapReduceJobProcessor interface to support reduce operations", jobName))
		return reduceResult
	}

	if mpProcessor, ok := jobProcessor.(processor.MapReduceJobProcessor); ok {
		runReduceIfFail := mpProcessor.RunReduceIfFail(jobCtx)
		if m.GetInstanceStatus() == processor.InstanceStatusFailed && !runReduceIfFail {
			logger.Warnf("jobInstanceId=%d is failed, skip reduce", jobInstanceId)
			return nil
		}
		reduceTaskName := constants.ReduceTaskName
		taskProgressCounter, _ := m.taskProgressMap.LoadOrStore(reduceTaskName, common.NewTaskProgressCounter(reduceTaskName))
		taskProgressCounter.(*common.TaskProgressCounter).IncrementOneTotal()
		taskProgressCounter.(*common.TaskProgressCounter).IncrementRunning()

		workerAddr := m.actorCtx.ActorSystem().Address()
		workerProgressCounter, _ := m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		workerProgressCounter.(*common.WorkerProgressCounter).IncrementTotal()
		workerProgressCounter.(*common.WorkerProgressCounter).IncrementRunning()

		result, err := mpProcessor.Reduce(jobCtx)
		if err != nil {
			result = processor.NewProcessResult()
			result.SetFailed()
			result.SetResult("reduce exception: " + err.Error())
		}

		if result.Status() == processor.InstanceStatusSucceed {
			if val, ok := m.taskProgressMap.Load(reduceTaskName); ok {
				val.(*common.TaskProgressCounter).IncrementOneSuccess()
			}
			if val, ok := m.workerProgressMap.Load(workerAddr); ok {
				val.(*common.WorkerProgressCounter).IncrementSuccess()
			}
		} else {
			if val, ok := m.taskProgressMap.Load(reduceTaskName); ok {
				val.(*common.TaskProgressCounter).IncrementOneFailed()
			}
			if val, ok := m.workerProgressMap.Load(workerAddr); ok {
				val.(*common.WorkerProgressCounter).IncrementOneFailed()
			}
		}
		return result
	}
	return reduceResult
}

func (m *MapTaskMaster) Stop() {
	if m.taskDispatchReqHandler != nil {
		m.taskDispatchReqHandler.Stop()
	}
	if m.taskStatusReqBatchHandler != nil {
		m.taskStatusReqBatchHandler.Stop()
	}
	logger.Infof("jobInstanceId:%v, instance master stop succeed.", m.GetJobInstanceInfo().GetJobInstanceId())
}

func (m *MapTaskMaster) startBatchHandler() error {
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

func (m *MapTaskMaster) SyncPullTasks(pageSize int32, workerIdAddr string) []*schedulerx.MasterStartContainerRequest {
	if m.getTotalPulledAndRunning() >= int64(m.xAttrs.GetGlobalConsumerSize()) {
		return nil
	} else {
		if reqs := m.taskDispatchReqHandler.SyncHandleReqs(m.taskDispatchReqHandler, pageSize, workerIdAddr); reqs != nil {
			ret := make([]*schedulerx.MasterStartContainerRequest, 0, len(reqs))
			for _, req := range reqs {
				ret = append(ret, req.(*schedulerx.MasterStartContainerRequest))
			}
			return ret
		}
		return nil
	}
}

func (m *MapTaskMaster) Clear(taskMaster taskmaster.TaskMaster) {
	m.TaskMaster.Clear(taskMaster)
	if m.taskStatusReqQueue != nil {
		m.taskStatusReqQueue.Clear()
	}
	if m.taskBlockingQueue != nil {
		m.taskBlockingQueue.Clear()
	}
	if m.taskDispatchReqHandler != nil {
		m.taskDispatchReqHandler.Clear()
	}
	if m.taskStatusReqBatchHandler != nil {
		m.taskStatusReqBatchHandler.Clear()
	}
	if m.taskProgressMap != nil {
		m.taskProgressMap = nil
	}
	if m.workerProgressMap != nil {
		m.workerProgressMap = nil
	}
	if m.taskResultMap != nil {
		m.taskResultMap = nil
	}
	if m.taskStatusMap != nil {
		m.taskStatusMap = nil
	}
	m.clearTasks(m.GetJobInstanceInfo().GetJobInstanceId())
	m.taskCounter = atomic.NewInt64(0)
}

func (m *MapTaskMaster) GetTaskProgressMap() *sync.Map {
	return m.taskProgressMap
}

func (m *MapTaskMaster) handleWorkerShutdown(workerIdAddr string) {
	m.GetAliveCheckWorkerSet().Remove(workerIdAddr)
	m.GetJobInstanceInfo().SetAllWorkers(utils.RemoveSliceElem(m.GetJobInstanceInfo().GetAllWorkers(), workerIdAddr))

	// adjust dispatch batch size
	m.taskDispatchReqHandler.SetDispatchSize(m.GetAliveCheckWorkerSet().Len() * int(m.pageSize))

	parts := strings.Split(workerIdAddr, "@")
	workerId := parts[0]
	workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
	if config.GetWorkerConfig().IsMapMasterFailover() {
		// If failover is enabled, set it to init state and wait for re-pull.
		affectCnt := m.taskPersistence.BatchUpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusInit, workerId, workerAddr)
		logger.Warnf("jobInstanceId=%d, failover task number:%d, workerId:%s, workerAddr:%s", m.GetJobInstanceInfo().GetJobInstanceId(), affectCnt, workerId, workerAddr)
		if affectCnt > 0 {
			if val, ok := m.workerProgressMap.Load(workerAddr); ok {
				val.(*common.WorkerProgressCounter).DecrementRunning(affectCnt)
			}
		}
	} else {
		// If failover is not enabled, the subtask on this worker will be directly set to failure.
		affectCnt := m.taskPersistence.BatchUpdateTaskStatus(m.GetJobInstanceInfo().GetJobInstanceId(), taskstatus.TaskStatusFailed, workerId, workerAddr)
		logger.Warnf("jobInstanceId=%d, failover task number:%d, workerId:%s, workerAddr:%s", m.GetJobInstanceInfo().GetJobInstanceId(), affectCnt, workerId, workerAddr)
		if affectCnt > 0 {
			if val, ok := m.workerProgressMap.Load(workerAddr); ok {
				val.(*common.WorkerProgressCounter).IncrementFailed(affectCnt)
			}
		}
	}
}
