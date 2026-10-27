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

Push 模式下，任务配置 `xattrs.consumerSize` 控制每个任务实例在单个 Worker 上同时执行的任务数，默认值为 5。超出的任务在 Worker 内排队，等待执行的任务仍可被取消；不同任务实例使用各自的并发额度。这个配置不代表跨 Worker 的总并发，也不限制业务代码内部自行启动的 goroutine 或 API 请求速率。

取消为协作式：SDK 会先发布取消结果，再在锁外调用业务 `Kill`；已进入 `Process` 的任务在真正退出前仍占用并发额度和去重标记。重试或下一轮复用同一实例额度，不能通过销毁并重建绕过限制；业务不响应取消时，新一轮可能等待旧执行退出。等待中的取消任务会立即移出执行队列，终态入队后释放去重标记。已完成任务仍可按相同 ID 显式重投。

Worker 的实例准入记录最多 65,536 条，已关闭记录保留 1 小时防止迟到消息重新开启旧轮次。开放记录也有 1 小时闲置回收期限；只有执行器已退出且状态队列及发送批次均为空才回收，避免 Master 漏发 Destroy 后永久耗尽容量。容量耗尽时新准入失败；批量推送保持原有批次 ACK，由异步失败状态向真实 Master 回报具体原因。ACK 只表示收到批次，不表示所有子任务已获准执行；状态传递沿用 Actor 通道，没有新增持久投递保证。

### 兼容性说明

本修复基于上游 v1.0.13。相对该基线，公开 Processor、Client、JobContext 等 API、配置和依赖均未修改；变化位于 internal 实现及可选 protobuf 字段。直接维护 SDK internal 代码的分支需适配 Pool 接口变化。

旧版协议可解码新增可选字段，旧消息也可由新版读取，但这不等于混部时拥有完整跨轮次保护。旧端会忽略新增 Attempt/Serial 元数据；旧清理消息未指定 Attempt 时按当前轮次处理，其中 SerialNum=0 保持旧通配语义。迟到的旧消息仍可能影响新轮次，同一实例的 Master/Worker 全部升级后才能利用新增的清理代次校验。handler 绑定只隔离退休后的入队；已进入传输层的旧状态仍没有 Attempt，不能保证端到端跨重试状态隔离。

从 v1.0.10 升级还包含上游 v1.0.13 的独立变化：最低 Go 声明由 1.20 提升到 1.21、连接池与 Tracing 实现变化、sqlite3 依赖升级。本补丁未新增这些变化，但 v1.0.10 使用方仍需验证升级，不能视为完全无行为变化的替换。

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
	"os"
	"os/signal"
	"syscall"
	"time"

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
	
	// wait for the stop signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)
	<-c
	time.Sleep(time.Second * 5)
}

```

### 4. 通过控制台创建任务

任务类型选择 golang，任务名称写第 4 步的任务名，比如 HelloWorld


## 示例

参考 example 目录


## Push Map 任务并发与生命周期

Push 模式下，`parallel`、`grid` 和 `batch` 任务按任务实例在每个 Worker 内共享
`consumerSize` 个执行名额；缺省或非正值使用默认值 5。该配置限制同时执行的
`Process`，不阻塞 Actor 接收后续批次或取消消息，也不是跨 Worker 的全局并发限制。
单机和广播任务沿用原来的执行方式。

- 同一任务 ID 在排队或执行期间重复推送只保留一份；执行完成后的显式重试仍允许提交。
- 排队任务取消时立即移除队列节点和载荷引用。已取得执行名额的任务属于在途任务，
  使用现有 `KillProcessor` 协作取消，SDK 无法强制结束阻塞的用户代码。SDK 自己的重试等待
  可被取消，换轮次或销毁也会停止旧重试，但不会提前归还仍在运行的用户代码额度。
- 任务 panic 或 `runtime.Goexit` 后回收执行名额并继续消费队列；Goexit 会报告失败状态。
- 实例清理后保存仅含 ID 和代次的关闭标记，阻止迟到批次重新创建同一轮任务。
  标记保留 1 小时、访问时清理；状态总量上限 65,536，满时拒绝新实例，避免静默淘汰
  仍有效的标记。开放记录在 1 小时后仅当执行器和待发送状态均空闲时回收，
  同时停止闲置状态 handler，避免缺失清理消息导致永久占用容量。
- 新版 Master 的 Kill/Destroy 消息携带可选 `Attempt` 和 `SerialNum`，按完整代次处理，
  防止旧重试的清理影响新重试。容器绑定所属代次的状态上报队列，旧任务晚结束时
  不会新入队到新一轮 handler；已发到传输层的旧状态没有 Attempt，不能保证端到端隔离。
  兼容旧消息缺少 JobId/代次字段；混合版本中，
  未携带 Attempt 的消息只能按旧协议的 SerialNum 判断，无法保证跨重试隔离。

队列和执行器在空闲后释放，不为等待 Master 清理而长期保留任务载荷或执行 goroutine。
关闭标记在不同路由 Actor 和 Actor 重启之间共享，但不跨 Worker 进程重启持久化。
