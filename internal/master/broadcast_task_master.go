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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/masterpool"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
)

var _ taskmaster.TaskMaster = &BroadcastTaskMaster{}

type BroadcastTaskMaster struct {
	*TaskMaster
	worker2uniqueIdMap *sync.Map // Map<String, String>
	workerProgressMap  *sync.Map // Map<String, WorkerProgressCounter>
	running            bool
	monitor            bool
	taskIdResultMap    *sync.Map // Map<Long, String>
	taskIdStatusMap    *sync.Map // Map<Long, TaskStatus>
	allWorkers         []string
	lock               sync.RWMutex
}

func NewBroadcastTaskMaster(jobInstanceInfo *common.JobInstanceInfo, actorCtx actor.Context) taskmaster.TaskMaster {
	broadcastTaskMaster := &BroadcastTaskMaster{
		worker2uniqueIdMap: new(sync.Map),
		workerProgressMap:  new(sync.Map),
		running:            false,
		monitor:            false,
		taskIdResultMap:    new(sync.Map),
		taskIdStatusMap:    new(sync.Map),
		allWorkers:         []string{},
	}

	statusHandler := NewCommonUpdateInstanceStatusHandler(actorCtx, broadcastTaskMaster, jobInstanceInfo)
	if utils.IsSecondTypeJob(common.TimeType(jobInstanceInfo.GetTimeType())) {
		statusHandler = NewSecondJobUpdateInstanceStatusHandler(actorCtx, broadcastTaskMaster, jobInstanceInfo)
	}
	broadcastTaskMaster.TaskMaster = NewTaskMaster(actorCtx, jobInstanceInfo, statusHandler)

	return broadcastTaskMaster
}

func (m *BroadcastTaskMaster) SubmitInstance(ctx context.Context, jobInstanceInfo *common.JobInstanceInfo) error {
	if err := m.preProcess(jobInstanceInfo); err != nil {
		logger.Errorf("BroadcastTaskMaster.preProcess failed, jobInstanceId=%d, err=%s", jobInstanceInfo.GetJobInstanceId(), err.Error())
		if e := m.TaskMaster.updateNewInstanceStatus(m.GetSerialNum(), m.jobInstanceInfo.GetJobInstanceId(), processor.InstanceStatusFailed, "Preprocess failed. "+err.Error()); e != nil {
			logger.Errorf("updateNewInstanceStatus failed after BroadcastTaskMaster.preProcess, jobInstanceId=%v, serialNum=%v, status=%v, err=%s", e.Error())
		}
	}

	m.allWorkers = jobInstanceInfo.GetAllWorkers()
	// The master node does not execute task
	if !config.GetWorkerConfig().BroadcastMasterExecEnable() {
		m.allWorkers = utils.RemoveSliceElem(jobInstanceInfo.GetAllWorkers(), m.GetLocalWorkerIdAddr())
	}

	// Set initialized state before sending. The second-level task is forced to stop the entire task instance
	// due to the failure of a single machine during the first broadcast process.
	m.Init()

	// Firstly build all the status maps to prevent the statusMap from being judged as completed
	// before the task is broadcast and sent during the broadcast processing.
	taskIdMap := make(map[string]int64)
	for _, workerIdAddr := range m.allWorkers {
		workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
		taskId := m.AcquireTaskId()
		uniqueId := utils.GetUniqueId(jobInstanceInfo.GetJobId(), jobInstanceInfo.GetJobInstanceId(), taskId)
		m.taskStatusMap.Store(uniqueId, taskstatus.TaskStatusInit)
		counter, _ := m.workerProgressMap.LoadOrStore(workerAddr, common.NewWorkerProgressCounter(workerAddr))
		counter.(*common.WorkerProgressCounter).IncrementTotal()
		taskIdMap[workerIdAddr] = taskId
	}
	for _, workerIdAddr := range m.allWorkers {
		m.dispatchTask(jobInstanceInfo, workerIdAddr, taskIdMap)
	}

	// After the task distribution is completed, the broadcast task detection thread is started to prevent misjudgment
	// caused by judging the processing status of each node during the broadcast distribution process.
	m.startMonitorThreads()

	return nil
}

// dispatchTask distribute broadcast tasks
func (m *BroadcastTaskMaster) dispatchTask(jobInstanceInfo *common.JobInstanceInfo, workerIdAddr string, taskIdMap map[string]int64) {
	var (
		err        error
		workerAddr string
		uniqueId   string
		taskId     int64
		workerId   string
	)
	defer func() {
		if err != nil {
			logger.Errorf("broadcast taskMaster submitTask=%s to worker=%s error, errMsg=%s", uniqueId, workerAddr, err.Error())
			m.existInvalidWorker = true
			failedReq := &schedulerx.ContainerReportTaskStatusRequest{
				JobId:         proto.Int64(jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(jobInstanceInfo.GetJobInstanceId()),
				TaskId:        proto.Int64(taskId),
				Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
				WorkerId:      proto.String(workerId),
				WorkerAddr:    proto.String(workerAddr),
				SerialNum:     proto.Int64(m.serialNum.Load()),
			}
			m.UpdateTaskStatus(failedReq)
		}
	}()

	workerInfo := strings.Split(workerIdAddr, "@")
	workerId = workerInfo[0]
	workerAddr = actorcomm.GetRealWorkerAddr(workerIdAddr)

	taskId = taskIdMap[workerIdAddr]
	uniqueId = utils.GetUniqueId(jobInstanceInfo.GetJobId(), jobInstanceInfo.GetJobInstanceId(), taskId)
	req, e := m.convert2StartContainerRequest(jobInstanceInfo, taskId, "", nil, false)
	if e != nil {
		err = fmt.Errorf("convert2StartContainerRequest failed, jobInstanceInfo=%+v, taskId=%+v, err=%s", jobInstanceInfo, taskId, e.Error())
		return
	}
	req.ShardingNum = proto.Int32(int32(len(m.allWorkers)))

	m.taskIdStatusMap.Store(taskId, taskstatus.TaskStatusInit)
	maxRetryTimes := int(config.GetWorkerConfig().BroadcastDispatchRetryTimes())
	for retryTimes := 0; retryTimes < maxRetryTimes; retryTimes++ {
		response, e := m.actorContext.RequestFuture(actorcomm.GetContainerRouterPid(workerAddr), req, 5*time.Second).Result()
		if e != nil {
			err = fmt.Errorf("start container failed, worker=%v, uniqueId=%v, serialNum=%v, err=%v",
				workerAddr, m.GetSerialNum(), uniqueId, e.Error())
			continue
		}
		resp, ok := response.(*schedulerx.MasterStartContainerResponse)
		if !ok {
			err = fmt.Errorf("start container failed, worker=%v, uniqueId=%v, serialNum=%v, response is not MasterStartContainerResponse, resp=%+v",
				workerAddr, m.GetSerialNum(), uniqueId, response)
			continue
		}
		if resp.GetSuccess() {
			m.worker2uniqueIdMap.Store(workerIdAddr, uniqueId)
			logger.Infof("broadcast taskMaster init succeed, worker addr is %s, uniqueId=%s", workerIdAddr, uniqueId)
			return
		} else {
			err = fmt.Errorf("broadcast submitTask=%v serialNum=%v to worker=%v failed, err=%v",
				uniqueId, m.GetSerialNum(), workerAddr, resp.GetMessage())
			time.Sleep(2 * time.Millisecond)
			continue
		}
	}
	return
}

func (m *BroadcastTaskMaster) KillInstance(reason string) error {
	m.TaskMaster.KillInstance(reason)

	for _, workerIdAddr := range m.allWorkers {
		uniqueId, ok := m.worker2uniqueIdMap.Load(workerIdAddr)

		// FIXME 这块没跟 java 对齐，但是因为有些 required 字段没赋值，会导致 actor 解码失败
		if ok {
			workerInfo := strings.Split(workerIdAddr, "@")
			workerId := workerInfo[0]
			workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
			tokens := strings.Split(uniqueId.(string), utils.SplitterToken)
			taskId, _ := strconv.Atoi(tokens[2])

			req := &schedulerx.ContainerReportTaskStatusRequest{
				JobId:         proto.Int64(m.jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
				TaskId:        proto.Int64(int64(taskId)),
				Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
				WorkerId:      proto.String(workerId),
				WorkerAddr:    proto.String(workerAddr),
			}

			m.actorContext.Send(actorcomm.GetContainerRouterPid(workerAddr), req)
		}
	}
	if err := m.TaskMaster.updateNewInstanceStatus(m.GetSerialNum(), m.jobInstanceInfo.GetJobInstanceId(), processor.InstanceStatusFailed, reason); err != nil {
		return fmt.Errorf("updateNewInstanceStatus failed, err=%s", err.Error())
	}

	// Clear the taskStatusMap, and directly end the task.
	m.taskStatusMap = sync.Map{}

	return nil
}

func (m *BroadcastTaskMaster) DestroyContainerPool() {
	for _, workerIdAddr := range m.allWorkers {
		req := &schedulerx.MasterDestroyContainerPoolRequest{
			JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
			JobId:         proto.Int64(m.jobInstanceInfo.GetJobId()),
			WorkerIdAddr:  proto.String(workerIdAddr),
			SerialNum:     proto.Int64(m.GetSerialNum()),
		}
		actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: req,
		}
	}
}

func (m *BroadcastTaskMaster) UpdateTaskStatus(request *schedulerx.ContainerReportTaskStatusRequest) error {
	if request.GetSerialNum() != m.GetSerialNum() {
		errMsg := fmt.Sprintf("ignore ContainerReportTaskStatusRequest, current serialNum=%v, but request serialNum=%v.", m.GetSerialNum(), request.GetSerialNum())
		return fmt.Errorf(errMsg)
	}
	var (
		jobId         = request.GetJobId()
		jobInstanceId = request.GetJobInstanceId()
		taskId        = request.GetTaskId()
		workerAddr    = request.GetWorkerAddr()

		taskStatus = taskstatus.TaskStatus(request.GetStatus())
		uniqueId   = utils.GetUniqueId(jobId, jobInstanceId, taskId)
	)
	logger.Infof("update task status serialNum=%v, uniqueId=%v, status=%v, workerAddr=%v", request.GetSerialNum(), uniqueId, taskStatus.Descriptor(), workerAddr)
	if val, ok := m.taskStatusMap.Load(uniqueId); ok {
		if val.(taskstatus.TaskStatus) == taskStatus {
			logger.Warnf("duplicated ContainerReportTaskStatusRequest, uniqueId=%v, taskStatus=%v", uniqueId, taskStatus)
		} else {
			if taskStatus == taskstatus.TaskStatusSucceed {
				// If a machine is finished running, it is directly removed from taskStatusMap.
				m.taskStatusMap.Delete(uniqueId)
			} else {
				// Update status to running
				m.taskStatusMap.Store(uniqueId, taskStatus)
			}
			if _, ok := m.workerProgressMap.Load(workerAddr); !ok {
				m.workerProgressMap.Store(workerAddr, common.NewWorkerProgressCounter(workerAddr))
			}

			counter, _ := m.workerProgressMap.Load(workerAddr)
			workerProgressCounter := counter.(*common.WorkerProgressCounter)
			switch taskStatus {
			case taskstatus.TaskStatusRunning:
				workerProgressCounter.IncrementRunning()
			case taskstatus.TaskStatusSucceed:
				workerProgressCounter.IncrementSuccess()
			case taskstatus.TaskStatusFailed:
				workerProgressCounter.IncrementOneFailed()
			}

			// update taskResultMap and taskStatusMap
			m.taskIdResultMap.Store(request.GetTaskId(), request.GetResult())
			m.taskIdStatusMap.Store(request.GetTaskId(), taskStatus)

			m.updateNewInstanceStatus(request.GetSerialNum(), jobInstanceId, request.GetResult())
		}
	}

	return nil
}

func (m *BroadcastTaskMaster) updateNewInstanceStatus(serialNum int64, jobInstanceId int64, result string) {
	newStatus := processor.InstanceStatusSucceed
	if m.IsKilled() {
		newStatus = processor.InstanceStatusFailed
	}

	if utils.SyncMapLen(&m.taskStatusMap) > 0 {
		if !m.IsJobInstanceFinished() {
			newStatus = processor.InstanceStatusRunning
		} else {
			newStatus = processor.InstanceStatusSucceed
			// as long as one subtask status is FAILED, then return FAILED
			m.taskStatusMap.Range(func(_, status interface{}) bool {
				if status.(taskstatus.TaskStatus) == taskstatus.TaskStatusFailed {
					newStatus = processor.InstanceStatusFailed
					return false
				}
				return true
			})
		}
	}

	logger.Infof("update serialNum=%v, jobInstanceId=%v status=%v", serialNum, jobInstanceId, newStatus.Descriptor())
	m.TaskMaster.updateNewInstanceStatus(serialNum, jobInstanceId, newStatus, result)
}

func (m *BroadcastTaskMaster) GetJobInstanceProgress() (string, error) {
	detail := common.NewMapTaskProgress()
	counters := make([]*common.WorkerProgressCounter, 0, utils.SyncMapLen(m.workerProgressMap))

	m.workerProgressMap.Range(func(_, val interface{}) bool {
		counters = append(counters, val.(*common.WorkerProgressCounter))
		return true
	})

	detail.SetWorkerProgress(counters)
	data, err := json.Marshal(detail)
	if err != nil {
		return "", fmt.Errorf("Marshal workerProgressDetail failed, err=%s ", err.Error())
	}

	return string(data), nil
}

// startMonitorThreads turns on the execution status monitoring of this round.
// During the execution of second-level tasks, the monitor is used to control the status detection
// after each round of broadcast task distribution is completed to prevent abnormal task detection during the distribution process.
func (m *BroadcastTaskMaster) startMonitorThreads() {
	m.setMonitor(true)
	if m.running {
		return
	}
	//jobIdAndInstanceId := fmt.Sprintf("%v_%v", m.jobInstanceInfo.GetJobId(), m.jobInstanceInfo.GetJobInstanceId())

	// check if worker is alive
	go m.checkWorkerAlive()

	// report job instance progress
	go m.reportJobInstanceProgress()

	// check instance status
	go m.checkInstanceStatus()

	m.running = true
}

func (m *BroadcastTaskMaster) checkWorkerAlive() {
	for !m.isInstanceStatusFinished() {
		if !m.isMonitor() {
			continue
		}

		for _, worker := range m.allWorkers {
			m.aliveCheckWorkerSet.Add(worker)
		}

		for _, workerIdAddr := range m.aliveCheckWorkerSet.ToStringSlice() {
			req := &schedulerx.MasterCheckWorkerAliveRequest{
				JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
			}

			workerAddr := actorcomm.GetRealWorkerAddr(workerIdAddr)
			_, err := m.actorContext.RequestFuture(actorcomm.GetHeartbeatActorPid(workerAddr), req, 10*time.Second).Result()
			if err != nil {
				m.existInvalidWorker = true
				uniqueId, ok := m.worker2uniqueIdMap.Load(workerIdAddr)
				if ok {
					workerInfo := strings.Split(workerIdAddr, "@")
					workerId := workerInfo[0]
					workerAddr = actorcomm.GetRealWorkerAddr(workerIdAddr)
					tokens := strings.Split(uniqueId.(string), utils.SplitterToken)
					jobId, _ := strconv.Atoi(tokens[0])
					jobInstanceId, _ := strconv.Atoi(tokens[1])
					taskId, _ := strconv.Atoi(tokens[2])

					req := &schedulerx.ContainerReportTaskStatusRequest{
						JobId:         proto.Int64(int64(jobId)),
						JobInstanceId: proto.Int64(int64(jobInstanceId)),
						TaskId:        proto.Int64(int64(taskId)),
						Status:        proto.Int32(int32(taskstatus.TaskStatusFailed)),
						WorkerId:      proto.String(workerId),
						WorkerAddr:    proto.String(workerAddr),
						SerialNum:     proto.Int64(m.GetSerialNum()),
					}
					if err := m.UpdateTaskStatus(req); err != nil {
						logger.Warnf("worker=%v is down, set=%v to failed status error, err=%s", workerAddr, uniqueId, err.Error())
					} else {
						logger.Warnf("worker=%v is down, set=%v to failed", workerAddr, uniqueId)
					}
				} else {
					logger.Errorf("can't found workerAddr of uniqueId=%v", uniqueId)
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (m *BroadcastTaskMaster) reportJobInstanceProgress() {
	for !m.isInstanceStatusFinished() {
		progress, err := m.GetJobInstanceProgress()
		if err != nil {
			logger.Errorf("reportJobInstanceProgress failed, err=%s", err.Error())
			return
		}
		req := &schedulerx.WorkerReportJobInstanceProgressRequest{
			JobId:         proto.Int64(m.jobInstanceInfo.GetJobId()),
			JobInstanceId: proto.Int64(m.jobInstanceInfo.GetJobInstanceId()),
			Progress:      proto.String(progress),
		}

		logger.Infof("BroadcastTaskMaster reportJobInstanceProgress, progress=%s", progress)
		// Send to server by master
		actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
			Msg: req,
		}

		time.Sleep(5 * time.Second)
	}
}

func (m *BroadcastTaskMaster) checkInstanceStatus() {
	for !m.isInstanceStatusFinished() {
		time.Sleep(5 * time.Second)

		if !m.isMonitor() {
			continue
		}

		if utils.SyncMapLen(&m.taskStatusMap) < 10 {
			logger.Infof("taskStatusMap=%+v", m.taskStatusMap)
		}
		m.updateNewInstanceStatus(m.GetSerialNum(), m.jobInstanceInfo.GetJobInstanceId(), "")
	}
}

func (m *BroadcastTaskMaster) GetWorkerProgressMap() *sync.Map {
	return m.workerProgressMap
}

func (m *BroadcastTaskMaster) Clear(taskMaster taskmaster.TaskMaster) {
	m.TaskMaster.Clear(taskMaster)
	m.worker2uniqueIdMap = new(sync.Map)
	m.workerProgressMap = new(sync.Map)
	m.taskIdResultMap = new(sync.Map)
	m.taskIdStatusMap = new(sync.Map)
	m.monitor = false
}

func (m *BroadcastTaskMaster) preProcess(jobInstanceInfo *common.JobInstanceInfo) error {
	jobCtx := m.convertJobInstance2JobContext(jobInstanceInfo)

	jobName := gjson.Get(jobCtx.Content(), "jobName").String()
	// Compatible with the existing Java language configuration mechanism
	if jobCtx.JobType() == "java" {
		jobName = gjson.Get(jobCtx.Content(), "className").String()
	}
	task, ok := masterpool.GetTaskMasterPool().Tasks().Find(jobName)
	if !ok {
		return fmt.Errorf("preProcess broadcast task=%s failed, because it's unregistered. ", jobName)
	}

	if p, ok := task.(processor.BroadcastProcessor); ok {
		startTime := time.Now().UnixMilli()
		if err := p.PreProcess(jobCtx); err != nil {
			return fmt.Errorf("preProcess broadcast task=%s failed, jobInstanceId=%v, taskName=%s, serialNum=%v, err=%s ", jobName, jobInstanceInfo.GetJobInstanceId(), jobCtx.TaskName(), jobCtx.SerialNum(), err.Error())
		}
		logger.Infof("preProcess broadcast task=%s finished, jobInstanceId=%v, taskName=%s, cost=%vms", jobName, jobInstanceInfo.GetJobInstanceId(), jobCtx.TaskName(), time.Now().UnixMilli()-startTime)
	}
	return nil
}

func (m *BroadcastTaskMaster) CheckProcessor() {
	// do nothing
}

func (m *BroadcastTaskMaster) PostFinish(jobInstanceId int64) *processor.ProcessResult {
	jobCtx := m.convertJobInstance2JobContext(m.jobInstanceInfo)
	defaultRet := processor.NewProcessResult(processor.WithSucceed())

	jobName := gjson.Get(jobCtx.Content(), "jobName").String()
	// Compatible with the existing Java language configuration mechanism
	if jobCtx.JobType() == "java" {
		jobName = gjson.Get(jobCtx.Content(), "className").String()
	}
	task, ok := masterpool.GetTaskMasterPool().Tasks().Find(jobName)
	if !ok {
		logger.Errorf("PostFinish broadcast task=%s failed, because it's unregistered. ", jobName)
		return defaultRet
	}

	if p, ok := task.(processor.BroadcastProcessor); ok {
		startTime := time.Now().UnixMilli()
		result, err := p.PostProcess(jobCtx)
		logger.Infof("PostFinish broadcast task=%s finished, jobInstanceId=%v, taskName=%s, cost=%vms", jobName, jobInstanceId, jobCtx.TaskName(), time.Now().UnixMilli()-startTime)
		if err != nil {
			logger.Errorf("PostFinish broadcast task=%s failed, jobInstanceId=%v, taskName=%s, serialNum=%v, err=%s ", jobName, jobInstanceId, jobCtx.TaskName(), jobCtx.SerialNum(), err.Error())
			return defaultRet
		}
		return result
	}

	return defaultRet
}

func (m *BroadcastTaskMaster) convertJobInstance2JobContext(jobInstanceInfo *common.JobInstanceInfo) *jobcontext.JobContext {
	jobCtx := new(jobcontext.JobContext)
	jobCtx.SetJobId(jobInstanceInfo.GetJobId())
	jobCtx.SetJobInstanceId(jobInstanceInfo.GetJobInstanceId())
	jobCtx.SetJobType(jobInstanceInfo.GetJobType())
	jobCtx.SetContent(jobInstanceInfo.GetContent())
	jobCtx.SetScheduleTime(jobInstanceInfo.GetScheduleTime())
	jobCtx.SetDataTime(jobInstanceInfo.GetDataTime())
	jobCtx.SetJobParameters(jobInstanceInfo.GetParameters())
	jobCtx.SetInstanceParameters(jobInstanceInfo.GetInstanceParameters())
	jobCtx.SetUser(jobInstanceInfo.GetUser())

	taskResults := make(map[int64]string)
	m.taskIdResultMap.Range(func(key, value any) bool {
		taskId := key.(int64)
		taskResult := value.(string)
		taskResults[taskId] = taskResult
		return true
	})
	jobCtx.SetTaskResults(taskResults)

	taskStatuses := make(map[int64]taskstatus.TaskStatus)
	m.taskIdStatusMap.Range(func(key, value any) bool {
		taskId := key.(int64)
		taskStatus := value.(taskstatus.TaskStatus)
		taskStatuses[taskId] = taskStatus
		return true
	})
	jobCtx.SetTaskStatuses(taskStatuses)

	jobCtx.SetSerialNum(m.GetSerialNum())
	return jobCtx
}

func (m *BroadcastTaskMaster) isMonitor() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.monitor
}

func (m *BroadcastTaskMaster) setMonitor(isRunning bool) {
	m.lock.Lock()
	m.monitor = isRunning
	m.lock.Unlock()
}

func (m *BroadcastTaskMaster) isInstanceStatusFinished() bool {
	return m.GetInstanceStatus().IsFinished()
}
