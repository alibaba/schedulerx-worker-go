schedulerx-worker-go 使用文档
---------------------

## 背景
[SchedulerX](https://www.aliyun.com/aliware/schedulerx) 是阿里云提供的分布式任务调度服务（兼容开源 XXL-JOB/ElasticJob/K8s Job/Spring Schedule），支持 Cron 定时、一次性任务、任务编排、分布式数据处理，具有高可用、可视化、可运维、低延时等能力。

schedulerx-worker-go 是 SchedulerX Go 版本的 SDK，该 SDK 由高德贡献。


## 功能
* 单机任务 （已支持）
* 广播任务 （已支持）
* Map任务（已支持）
* MapReduce任务 （已支持）

## 使用说明

### 1. 登录 SchedulerX 控制台创建应用，返回配置信息

```
endpoint=xxxx
namespace=xxxx
groupId=xxx
appKey=xxx
```

### 2. 拉取 Go 版本 SDK

```
go get github.com/alibaba/schedulerx-worker-go@{最新的tag}
```

### 3. 实现接口，编写业务代码

#### 3.1 单机任务
接口如下：

```go
type Processor interface {
	Process(ctx *processor.JobContext) (*ProcessResult, error)
}
```
实现接口，参考Demo ：

``` Go
//HelloWorld.go
package main

import (
	"fmt"
	"time"

 	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
)

var _ processor.Processor = &HelloWorld{}

type HelloWorld struct{}

func (h *HelloWorld) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	fmt.Println("[Process] Start process my task: Hello world!")
	// mock execute task
	time.Sleep(3 * time.Second)
	ret := new(processor.ProcessResult)
	ret.SetStatus(processor.InstanceStatusSucceed)
	fmt.Println("[Process] End process my task: Hello world!")
	return ret, nil
}

```

#### 3.2 广播任务
接口如下：

```go
type BroadcastProcessor interface {
	Processor
	PreProcess(ctx *jobcontext.JobContext) error
	PostProcess(ctx *jobcontext.JobContext) (*ProcessResult, error)
}
```
接口描述：

* PreProcess：所有worker节点执行Process之前，由master节点执行一次PreProcess
* Process：所有worker节点执行Process，可以返回结果
* PostProcess：所有worker节点执行Process结束后，由master节点执行一次PostProcess，可以获取所有节点Process的结果

实现接口, 参考Demo:

``` Go
// TestBroadcast.go
package main

import (
	"fmt"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"math/rand"
	"strconv"
)

type TestBroadcast struct{}

// Process all machines would execute it.
func (t TestBroadcast) Process(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	value := rand.Intn(10)
	fmt.Printf("Total sharding num=%d, sharding=%d, value=%d\n", ctx.ShardingNum(), ctx.ShardingId(), value)
	ret := new(processor.ProcessResult)
	ret.SetSucceed()
	ret.SetResult(strconv.Itoa(value))
	return ret, nil
}

// PreProcess only one machine will execute it.
func (t TestBroadcast) PreProcess(ctx *jobcontext.JobContext) error {
	fmt.Println("TestBroadcastJob PreProcess")
	return nil
}

// PostProcess only one machine will execute it.
func (t TestBroadcast) PostProcess(ctx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	fmt.Println("TestBroadcastJob PostProcess")
	allTaskResults := ctx.TaskResults()
	allTaskStatuses := ctx.TaskStatuses()
	num := 0

	for key, val := range allTaskResults {
		fmt.Printf("%v:%v\n", key, val)
		if allTaskStatuses[key] == taskstatus.TaskStatusSucceed {
			valInt, _ := strconv.Atoi(val)
			num += valInt
		}
	}

	fmt.Printf("TestBroadcastJob PostProcess(), num=%d\n", num)
	ret := new(processor.ProcessResult)
	ret.SetSucceed()
	ret.SetResult(strconv.Itoa(num))
	return ret, nil
}

```

#### 3.3 Map任务
接口如下：

```go
type MapJobProcessor interface {
	Processor
	Map(jobCtx *jobcontext.JobContext, taskList []interface{}, taskName string) (*ProcessResult, error)
	Kill(jobCtx *jobcontext.JobContext) error
}
```
接口描述：

* Process: 每次子任务执行的业务逻辑，第一次进来的是根任务
* Map: 在根任务中构造taskList，通过map方法可以平均分发给其他worker节点分布式执行

实现接口，参考Demo:

```go
// TestMapJob.go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob"
	"strconv"
	"time"
)

type TestMapJob struct {
	*mapjob.MapJobProcessor
}

func (mr *TestMapJob) Kill(jobCtx *jobcontext.JobContext) error {
	//TODO implement me
	panic("implement me")
}

// Process the MapReduce model is used to distributed scan orders for timeout confirmation
func (mr *TestMapJob) Process(jobCtx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	var (
		num = 100
		err error
	)
	taskName := jobCtx.TaskName()

	if jobCtx.JobParameters() != "" {
		num, err = strconv.Atoi(jobCtx.JobParameters())
		if err != nil {
			return nil, err
		}
	}

	if mr.IsRootTask(jobCtx) {
		fmt.Println("start root task")
		var messageList []interface{}
		for i := 1; i <= num; i++ {
			var str = fmt.Sprintf("id_%d", i)
			messageList = append(messageList, str)
		}
		fmt.Println(messageList)
		return mr.Map(jobCtx, messageList, "Level1Dispatch")
	} else if taskName == "Level1Dispatch" {
		var task []byte = jobCtx.Task()
		var str string
		err = json.Unmarshal(task, &str)
		fmt.Printf("str=%s\n", str)
		time.Sleep(100 * time.Millisecond)
		fmt.Println("Finish Process...")
		if str == "id_5" {
			return processor.NewProcessResult(
				processor.WithFailed(),
				processor.WithResult(str),
			), errors.New("test")
		}
		return processor.NewProcessResult(
			processor.WithSucceed(),
			processor.WithResult(str),
		), nil
	}
	return processor.NewProcessResult(processor.WithFailed()), nil
}


```

#### 3.4 MapReduce任务
接口如下：

```go
type MapReduceJobProcessor interface {
	MapJobProcessor
	Reduce(jobCtx *jobcontext.JobContext) (*ProcessResult, error)
	RunReduceIfFail(jobCtx *jobcontext.JobContext) bool
}
```
继承Map接口，新增接口如下：

* Reduce: 所有子任务执行完成后会执行一次reduce方法，在reduce中可以拿到所有子任务的状态和结果
* RunReduceIfFail: 如果有子任务执行失败，是否执行reduce方法

实现接口，参考Demo:

```go
// TestMapReduceJob.go
package main

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/schedulerx-worker-go/processor"
	"github.com/alibaba/schedulerx-worker-go/processor/jobcontext"
	"github.com/alibaba/schedulerx-worker-go/processor/mapjob"
	"github.com/alibaba/schedulerx-worker-go/processor/taskstatus"
	"strconv"
	"time"
)

type OrderInfo struct {
	Id    string `json:"id"`
	Value int    `json:"value"`
}

func NewOrderInfo(id string, value int) *OrderInfo {
	return &OrderInfo{Id: id, Value: value}
}

type TestMapReduceJob struct {
	*mapjob.MapReduceJobProcessor
}

func (mr *TestMapReduceJob) Kill(jobCtx *jobcontext.JobContext) error {
	//TODO implement me
	panic("implement me")
}

// Process the MapReduce model is used to distributed scan orders for timeout confirmation
func (mr *TestMapReduceJob) Process(jobCtx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	var (
		num = 1000
		err error
	)
	taskName := jobCtx.TaskName()

	if jobCtx.JobParameters() != "" {
		num, err = strconv.Atoi(jobCtx.JobParameters())
		if err != nil {
			return nil, err
		}
	}

	if mr.IsRootTask(jobCtx) {
		fmt.Println("start root task, taskId=%d", jobCtx.TaskId())
		var orderInfos []interface{}
		for i := 1; i <= num; i++ {
			orderInfos = append(orderInfos, NewOrderInfo(fmt.Sprintf("id_%d", i), i))
		}
		return mr.Map(jobCtx, orderInfos, "OrderInfo")
	} else if taskName == "OrderInfo" {
		orderInfo := new(OrderInfo)
		if err := json.Unmarshal(jobCtx.Task(), orderInfo); err != nil {
			fmt.Printf("task is not OrderInfo, task=%+v\n", jobCtx.Task())
		}
		fmt.Printf("taskId=%d, orderInfo=%+v\n", jobCtx.TaskId(), orderInfo)
		time.Sleep(1 * time.Millisecond)
		return processor.NewProcessResult(
			processor.WithSucceed(),
			processor.WithResult(strconv.Itoa(orderInfo.Value)),
		), nil
	}
	return processor.NewProcessResult(processor.WithFailed()), nil
}

func (mr *TestMapReduceJob) Reduce(jobCtx *jobcontext.JobContext) (*processor.ProcessResult, error) {
	allTaskResults := jobCtx.TaskResults()
	allTaskStatuses := jobCtx.TaskStatuses()
	count := 0
	fmt.Printf("reduce: all task count=%d\n", len(allTaskResults))
	for key, val := range allTaskResults {
		if key == 0 {
			continue
		}
		if allTaskStatuses[key] == taskstatus.TaskStatusSucceed {
			num, err := strconv.Atoi(val)
			if err != nil {
				return nil, err
			}
			count += num
		}
	}
	fmt.Printf("reduce: succeed task count=%d\n", count)
	return processor.NewProcessResult(
		processor.WithSucceed(),
		processor.WithResult(strconv.Itoa(count)),
	), nil
}
```

### 4. 注册 client 和 job

```
package main

import (
	"github.com/alibaba/schedulerx-worker-go"
)

func main() {
	// This is just an example, the real configuration needs to be obtained from the platform
	cfg := &schedulerx.Config{
		Endpoint:  "acm.aliyun.com",
		Namespace: "433d8b23-06e9-408c-aaaa-90d4d1b9a4af",
		GroupId:   "gojob-test",
		AppKey:    "xxxxxxx",
	}
	client, err := schedulerx.GetClient(cfg)
	if err != nil {
		panic(err)
	}
	task1 := &HelloWorld{}
	task2 := &TestBroadcast{}
	task3 := &TestMapJob{
		mapjob.NewMapJobProcessor(),
	}
	task4 := &TestMapReduceJob{
		mapjob.NewMapReduceJobProcessor(),
	}

	// The name HelloWorld registered here must be consistent with the configured on the platform
	client.RegisterTask("HelloWorld", task1)
	client.RegisterTask("TestBroadcast", task2)
	client.RegisterTask("TestMapJob", task3)
	client.RegisterTask("TestMapReduceJob", task4)
	select {}
}

```

### 4. 通过控制台创建任务

任务类型选择 golang，任务名称写第 4 步的任务名，比如 HelloWorld


## 示例

参考 example 目录
