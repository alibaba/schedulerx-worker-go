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
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/config"
	"github.com/alibaba/schedulerx-worker-go/internal/actor/common"
	"github.com/alibaba/schedulerx-worker-go/internal/common"
	"github.com/alibaba/schedulerx-worker-go/internal/constants"
	"github.com/alibaba/schedulerx-worker-go/internal/master/taskmaster"
	"github.com/alibaba/schedulerx-worker-go/internal/openapi"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
	"github.com/alibaba/schedulerx-worker-go/processor"
)

const missServerKillTime = 30 // seconds

type secondJobUpdateInstanceStatusHandler struct {
	*baseUpdateInstanceStatusHandler
	actorCtx              actor.Context
	secondProgressDetail  *common.SecondProgressDetail
	cycleStartTime        int64
	triggerTimes          int32
	triggerCus            int32
	enableCycleIntervalMs bool
	recentProgressHistory *utils.LimitedQueue
}

func NewSecondJobUpdateInstanceStatusHandler(actorCtx actor.Context, taskMaster taskmaster.TaskMaster, jobInstanceInfo *common.JobInstanceInfo) UpdateInstanceStatusHandler {
	h := &secondJobUpdateInstanceStatusHandler{
		baseUpdateInstanceStatusHandler: NewBaseUpdateInstanceStatusHandler(jobInstanceInfo, taskMaster),
		actorCtx:                        actorCtx,
		cycleStartTime:                  time.Now().UnixMilli(),
		enableCycleIntervalMs:           config.GetWorkerConfig().IsSecondDelayIntervalMS(),
		secondProgressDetail:            common.NewSecondProgressDetail(),
		recentProgressHistory:           utils.NewLimitedQueue(10),
	}
	h.init()
	return h
}

func (h *secondJobUpdateInstanceStatusHandler) init() {
	GetTimeScheduler().init()

	// job instance progress report thread.
	go h.reportJobInstanceProgress()
}

func (h *secondJobUpdateInstanceStatusHandler) reportJobInstanceProgress() {
	intervalTimes := 0
	jobIdAndInstanceId := utils.GetUniqueIdWithoutTaskId(h.jobInstanceInfo.GetJobId(), h.jobInstanceInfo.GetJobInstanceId())
	for !h.taskMaster.IsKilled() {
		time.Sleep(1 * time.Second)
		intervalTimes++
		if intervalTimes > 10 {
			progress, err := h.getJobInstanceProgress()
			if err != nil {
				logger.Errorf("report status failed due to getJobInstanceProgress failed, err=%v, jobIdAndInstanceId=%v.", err, jobIdAndInstanceId)
				continue
			}
			req := &schedulerx.WorkerReportJobInstanceProgressRequest{
				JobId:         proto.Int64(h.jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(h.jobInstanceInfo.GetJobInstanceId()),
				Progress:      proto.String(progress),
				TriggerTimes:  proto.Int32(h.triggerTimes),
			}

			// FIXME ServerDiscovery serverDiscovery = ServerDiscoveryFactory.GetDiscovery(jobInstanceInfo.GetGroupId());
			actorcomm.TaskMasterMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
				Msg: req,
			}

			intervalTimes = 0
			h.triggerTimes = 0
			h.triggerCus = 0
		}

		if err := h.need2KillSelf(); err != nil {
			logger.Errorf("report status error, err=%v, jobIdAndInstanceId=%v.", err, jobIdAndInstanceId)
		}
	}
}

// Kill self is required if any of the following conditions are met:
// 1. Lost contact with the server for more than 30 seconds
// 2. The grid task has no available worker
func (h *secondJobUpdateInstanceStatusHandler) need2KillSelf() error {
	if !h.taskMaster.IsInited() {
		return nil
	}
	jobIdAndInstanceId := utils.GetUniqueIdWithoutTaskId(h.jobInstanceInfo.GetJobId(), h.jobInstanceInfo.GetJobInstanceId())
	if utils.GetHealthTimeHolder().IsServerHeartbeatHealthTimeout(missServerKillTime) {
		if err := h.taskMaster.KillInstance("killed, because of worker missed active server."); err != nil {
			return err
		}
		logger.Warnf("Missed server timeout=%vms, kill jobIdAndInstanceId=%v.", utils.GetHealthTimeHolder().GetServerHeartbeatMsInterval(), jobIdAndInstanceId)

	}
	if h.taskMaster.GetAliveCheckWorkerSet().Len() == 0 && len(h.taskMaster.GetJobInstanceInfo().GetAllWorkers()) == 0 {
		if err := h.taskMaster.KillInstance("killed, because of missed useful worker list."); err != nil {
			return err
		}
		logger.Warnf("Missed useful worker list, kill jobIdAndInstanceId=%v.", jobIdAndInstanceId)
	}
	return nil
}

func (h *secondJobUpdateInstanceStatusHandler) getJobInstanceProgress() (string, error) {
	progress, err := h.getJobInstanceProgress()
	if err != nil {
		return "", err
	}
	h.secondProgressDetail.SetRunningProgress(progress)
	h.secondProgressDetail.SetRunningStartTime(h.cycleStartTime)
	h.secondProgressDetail.SetRecentProgressHistory(h.recentProgressHistory.Convert2Slice())
	data, err := json.Marshal(h.secondProgressDetail)
	if err != nil {
		return "", err
	}
	h.secondProgressDetail.SetRunningProgress("")
	return string(data), nil
}

// Get the latest worker list
func (h *secondJobUpdateInstanceStatusHandler) getAllWorkers(appGroupId, jobId int64) (*utils.Set, error) {
	url := fmt.Sprintf("http://%s/app/getAllUsefulWorkerList.json?appGroupId=%d&jobId=%d", openapi.GetOpenAPIClient().Domain(), appGroupId, jobId)
	resp, err := openapi.GetOpenAPIClient().HttpClient().Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request getAllWorkers failed, appGroupId:%s, err:%s", appGroupId, err.Error())
	}
	defer resp.Body.Close()
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read http http response failed, err=%s ", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Read http post response failed, statusCode=%s ", resp.Status)
	}
	jsonResp := new(openapi.JSONResult)
	if err := json.Unmarshal(respData, jsonResp); err != nil {
		return nil, fmt.Errorf("Unmarshal response body failed, respData=%+v ", respData)
	}

	set := new(utils.Set)
	if jsonResp.Data != nil && reflect.TypeOf(jsonResp.Data).Kind() == reflect.Slice {
		sliceValue := reflect.ValueOf(jsonResp.Data)
		for i := 0; i < sliceValue.Len(); i++ {
			item := sliceValue.Index(i).Interface()
			set.Add(item)
		}
	}

	return set, nil
}

func (h *secondJobUpdateInstanceStatusHandler) Handle(serialNum int64, instanceStatus processor.InstanceStatus, result string) error {
	cycleId := utils.GetUniqueId(h.jobInstanceInfo.GetJobId(), h.jobInstanceInfo.GetJobInstanceId(), h.taskMaster.GetSerialNum())
	logger.Infof("cycleId: %v instanceStatus=%v cycle update status.", cycleId, instanceStatus)

	// if init failed, instance status finished and master has not been killed, so second job kill self
	if !h.taskMaster.IsInited() && instanceStatus.IsFinished() && !h.taskMaster.IsKilled() {
		h.taskMaster.KillInstance("killed, because of worker init failed.")
		logger.Warnf("Init failed need to kill self, cycleId=%v", cycleId)
		return nil
	}

	// if instance is killed, need to report to server
	// From a logical point of view, you only need to judge whether the master has been killed.
	// There is no need to judge whether the result contains the specified information.
	// However, history says that we should not delete it in the short term.
	if h.taskMaster.IsKilled() &&
		(strings.Contains(result, "killed") || strings.Contains(result, "Worker master shutdown")) {
		h.taskMaster.SetInstanceStatus(processor.InstanceStatusFailed)
		h.taskMaster.Stop()
		h.masterPool.Remove(h.jobInstanceInfo.GetJobInstanceId())

		if result != "killed from server" {
			// There is no status feedback for the server-side forced stop operation.
			req := &schedulerx.WorkerReportJobInstanceStatusRequest{
				JobId:         proto.Int64(h.jobInstanceInfo.GetJobId()),
				JobInstanceId: proto.Int64(h.jobInstanceInfo.GetJobInstanceId()),
				Status:        proto.Int32(h.jobInstanceInfo.GetStatus()),
				GroupId:       proto.String(h.jobInstanceInfo.GetGroupId()),
			}
			if result != "" {
				req.Result = proto.String(result)
			}
			progress, err := h.getJobInstanceProgress()
			if err != nil {
				return fmt.Errorf("cycleId: %v instanceStatus=%v cycle update status failed due to getJobInstanceProgress failed, err=%s", cycleId, instanceStatus, err.Error())
			}
			if progress != "" {
				req.Progress = proto.String(progress)
			}
			actorcomm.AtLeastOnceDeliveryMsgReceiver() <- &actorcomm.SchedulerWrappedMsg{
				Msg: req,
			}

			logger.Infof("report cycleId=%v, status=%v to AtLeastDeliveryRoutingActor", cycleId, instanceStatus)
		}

		// If the instance terminates no further action is required
		return nil
	}

	// If job instance is finished, remove from TaskMasterPool
	if instanceStatus.IsFinished() {
		h.triggerNextCycle(cycleId, serialNum, instanceStatus)
	}
	return nil
}

func (h *secondJobUpdateInstanceStatusHandler) triggerNextCycle(cycleId string, serialNum int64, instanceStatus processor.InstanceStatus) {
	if serialNum != h.taskMaster.GetSerialNum() {
		logger.Infof("triggerNextCycle=%v ignore, current serialNum=%v, but trigger serialNum=%v, status=%v, killed=%v.",
			cycleId, h.taskMaster.GetSerialNum(), serialNum, instanceStatus, h.taskMaster.IsKilled())
		return
	}
	postResult := h.taskMaster.PostFinish(h.jobInstanceInfo.GetJobInstanceId())
	if postResult != nil {
		logger.Infof("cycleId: %v cycle post status, result=%v.", cycleId, postResult.Status(), postResult.Result())
	}

	logger.Infof("cycleId: %v cycle end.", cycleId)

	h.setHistory(h.taskMaster.GetSerialNum(), h.cycleStartTime, instanceStatus)

	if !h.taskMaster.IsKilled() {
		//TODO: 先清理这次迭代的资源，未来可以优化不需要每次清理
		h.taskMaster.Clear(h.taskMaster)

		// The current node is offline
		// FIXME implement it
		//if (!SchedulerxWorker.INITED) {
		//	LOGGER.info("Current worker is not running. To shutdown this master JobInstanceId={}", jobInstanceInfo.getJobInstanceId());
		//	taskMaster.killInstance(true,"Worker master shutdown.");
		//	return;
		//}

		// Calculate the next scheduling time and add it to the time scheduler
		delayTime, err := strconv.Atoi(h.jobInstanceInfo.GetTimeExpression())
		if err != nil {
			h.taskMaster.KillInstance("killed, because of cycle submit failed.")
			logger.Errorf("cycleId=%v cycle submit failed, need to kill, err=%s", cycleId, err.Error())
			return
		}

		if !h.enableCycleIntervalMs {
			delayTime = 1000 * delayTime
		}
		h.cycleStartTime = time.Now().UnixMilli() + int64(delayTime)
		planEntry := NewTimePlanEntry(h.jobInstanceInfo.GetJobInstanceId(), h.cycleStartTime, h)
		GetTimeScheduler().add(planEntry)
	} else {
		h.taskMaster.AcquireSerialNum()
	}
}

func (h *secondJobUpdateInstanceStatusHandler) setHistory(serialNum int64, loopStartTime int64, status processor.InstanceStatus) {
	if status == processor.InstanceStatusSucceed {
		h.secondProgressDetail.GetTodayProgressCounter().IncrementOneSuccess()
	} else {
		h.secondProgressDetail.GetTodayProgressCounter().IncrementOneFailed()
	}

	if !h.taskMaster.IsKilled() {
		h.secondProgressDetail.GetTodayProgressCounter().IncrementRunning()
		h.secondProgressDetail.GetTodayProgressCounter().IncrementOneTotal()
	}

	// reset today progress counter
	todayBeginTime, err := time.Parse(constants.TimeFormat, h.secondProgressDetail.GetTodayBeginTime())
	if err != nil {
		logger.Errorf("setHistory failed, getTodayBeginTime from secondProgressDetail failed, todayBeginTime=%s", todayBeginTime)
		return
	}
	if time.Now().Day() != todayBeginTime.Day() {
		h.secondProgressDetail.SetYesterdayProgressCounter(h.secondProgressDetail.GetTodayProgressCounter())
		h.secondProgressDetail.SetTodayBeginTime(time.Now().Format(constants.TimeFormat))
		h.secondProgressDetail.SetTodayProgressCounter(common.NewTaskProgressCounter(h.secondProgressDetail.GetTodayBeginTime()))
	}

	taskProgressMap := make(map[string]*common.TaskProgressCounter)
	switch h.taskMaster.(type) {
	case taskmaster.MapTaskMaster:
		h.taskMaster.(*MapTaskMaster).GetTaskProgressMap().Range(func(key, value any) bool {
			taskProgressMap[key.(string)] = value.(*common.TaskProgressCounter)
			return true
		})
	case *BroadcastTaskMaster:
		workerProgressCounterMap := h.taskMaster.(*BroadcastTaskMaster).GetWorkerProgressMap()
		if utils.SyncMapLen(workerProgressCounterMap) == 0 {
			return
		}

		workerProgressCounterMap.Range(func(key, value any) bool {
			counter := value.(*common.WorkerProgressCounter)

			newCounter := new(common.TaskProgressCounter)
			newCounter.IncrementSuccess(counter.GetSuccess())
			newCounter.IncrementFailed(counter.GetFailed())
			newCounter.IncrementTotal(counter.GetTotal())
			taskProgressMap[counter.GetWorkerAddr()] = newCounter
			return true
		})
	case *StandaloneTaskMaster:
		ipAndPort := h.taskMaster.(*StandaloneTaskMaster).GetCurrentSelection()
		counter := common.NewTaskProgressCounter(ipAndPort)
		counter.IncrementOneTotal()
		if status == processor.InstanceStatusSucceed {
			counter.IncrementOneSuccess()
		} else {
			counter.IncrementOneFailed()
		}
		taskProgressMap[ipAndPort] = counter
	}

	if len(taskProgressMap) == 0 {
		return
	}
	history := common.NewProgressHistory()
	history.SetSerialNum(serialNum)
	history.SetStartTime(loopStartTime)
	history.SetEndTime(time.Now().UnixMilli())
	history.SetCostTime(history.EndTime() - history.StartTime())
	history.SetTaskProgressMap(taskProgressMap)
	if status == processor.InstanceStatusSucceed {
		history.SetSuccess(true)
	} else {
		history.SetSuccess(false)
	}
	h.recentProgressHistory.Enqueue(history)
}

// Schedule a new iteration
func (h *secondJobUpdateInstanceStatusHandler) triggerNewCycle() {
	cycleId := utils.GetUniqueId(h.jobInstanceInfo.GetJobId(), h.jobInstanceInfo.GetJobInstanceId(), h.taskMaster.GetSerialNum())
	logger.Infof("cycleId: %v cycle begin.", cycleId)
	h.cycleStartTime = time.Now().UnixMilli()

	h.jobInstanceInfo.SetScheduleTime(time.Duration(time.Now().Nanosecond()))
	// If existed invalid worker nodes, re-obtain the latest list
	if h.taskMaster.ExistInvalidWorker() {
		freeWorkers, err := h.getAllWorkers(h.jobInstanceInfo.GetAppGroupId(), h.jobInstanceInfo.GetJobId())
		if err != nil {
			h.taskMaster.KillInstance("killed, because of cycle submit failed.")
			logger.Errorf("cycleId=%d cycle submit failed, need to kill.", cycleId, err.Error())
		}
		h.taskMaster.RestJobInstanceWorkerList(freeWorkers)
	}
	h.taskMaster.SubmitInstance(context.Background(), h.jobInstanceInfo)

	h.triggerTimes++
	// If it is a standalone task, cu+1
	// If it is a distributed task, cu+workers
	if h.jobInstanceInfo.GetExecuteMode() == string(common.StandaloneExecuteMode) {
		h.triggerCus++
	} else {
		h.triggerCus += int32(len(h.jobInstanceInfo.GetAllWorkers()))
	}
}
