schedulerx-worker-go 使用文档
---------------------

## 背景
[SchedulerX](https://www.aliyun.com/aliware/schedulerx) 是阿里云提供的分布式任务调度服务（兼容开源 XXL-JOB/ElasticJob/K8s Job/Spring Schedule），支持 Cron 定时、一次性任务、任务编排、分布式数据处理，具有高可用、可视化、可运维、低延时等能力。

schedulerx-worker-go 是 SchedulerX Go 版本的 SDK，该 SDK 由高德贡献。


## 功能
* 单机任务 （已支持）
* 广播任务 （开发中）
* MapReduce 任务 （开发中）

## 使用说明

1、 登录 SchedulerX 控制台创建应用，返回配置信息

```
endpoint=xxxx
namespace=xxxx
groupId=xxx
appKey=xxx
```

2、拉取 Go 版本 SDK

```
go get github.com/alibaba/schedulerx-worker-go@{最新的tag}
```

3、实现接口，编写业务代码

```
type Processor interface {
	Process(ctx *processor.JobContext) (*ProcessResult, error)
}
```
举个例子

```
package main

import (
	"fmt"
	"time"

	"github.com/alibaba/schedulerx-worker-go/processor"
)

var _ processor.Processor = &HelloWorld{}

type HelloWorld struct{}

func (h *HelloWorld) Process(ctx *processor.JobContext) (*processor.ProcessResult, error) {
	fmt.Println("[Process] Start process my task: Hello world!")
	// mock execute task
	time.Sleep(3 * time.Second)
	ret := new(processor.ProcessResult)
	ret.SetStatus(processor.InstanceStatusSucceed)
	fmt.Println("[Process] End process my task: Hello world!")
	return ret, nil
}
```

4、注册client和job

```
package main

import (
	"github.com/alibaba/schedulerx-worker-go"
)

func main() {
	// 可以填写第一步获取的配置
	client, err := schedulerx.GetClient(&schedulerx.Config{
		Endpoint: "xxxx",
		GroupId:    "xxxxx",
		Namespace:  "xxxx",
		AppKey:     "xxxx",
	})
	if err != nil {
		panic(err)
	}
	task := &HelloWorld{}

	// 给你的任务取一个名字，并注册到client中
	client.RegisterTask("HelloWorld", task)
	select {}
}

```

5、通过控制台创建任务

任务类型选择 golang，任务名称写第 4 步的任务名，比如 HelloWorld


## 示例

参考 example 目录