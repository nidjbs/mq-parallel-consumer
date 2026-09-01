# mq-parallel-consumer

一个 MQ 无关的泳道并发消费 SDK。核心引擎（`swimlane`）不依赖任何具体 MQ，通过 `Backend` SPI 适配不同消息队列；当前内置 Kafka（franz-go）适配，后续可平滑接入 RocketMQ 等。

解决单 partition 消费的并行度瓶颈：消费线程负责拉取与提交，消息按 key 路由到固定泳道，**同 key 串行、异 key 并发**，offset 提交采用**最大连续已完成**位置，杜绝"提交了但中间消息没处理"造成的消息丢失。

## 特性

- **两种顺序模式**：`KeyOrdered`（同 key 串行，异 key 并发）/ `Unordered`（完全并发）
- **MQ 无关**：核心零外部依赖（仅标准库），`Backend` SPI 抽象传输层
- **最大连续 offset 提交**：并发乱序完成下只提交连续区间，崩溃恢复不丢已提交消息
- **背压**：每 partition 在途上限触发 pause/resume，有界队列兜底内存
- **失败处理**：内置重试（退避）+ 可选 `OnDiscard` 回调；默认不重试不静默跳过，失败即致命上报
- **rebalance 优雅收尾**：revoked 时 drain 在途消息并提交最终 offset
- **线程安全**：Consumer 与普通 SDK 无异，`Run`/`Stop`/`Stats` 可并发调用

## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"log"

	"mq-parallel-consumer"
	"mq-parallel-consumer/backend/kafka"
)

func main() {
	ctx := context.Background()
	cfg := swimlane.DefaultConfig()
	cfg.Lanes = 8 // 单 partition 并发度
	cfg.Retry = swimlane.RetryPolicy{MaxAttempts: 3}

	be, err := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Group: "demo"})
	if err != nil {
		log.Fatal(err)
	}

	c, err := swimlane.New(be, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Subscribe([]string{"demo-topic"}, func(ctx context.Context, msg *swimlane.Message) error {
		fmt.Printf("partition=%d offset=%d key=%s value=%s\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := c.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

完整示例见 [`examples/main.go`](examples/main.go)。

## 配置

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Mode` | `KeyOrdered` | 顺序模式 |
| `Lanes` | 8 | KeyOrdered 下单 partition 泳道数 |
| `Concurrency` | 8 | Unordered 下单 partition 并发数 |
| `MaxInFlight` | 并发度 × `QueueSize` | 单 partition 在途上限，触发 pause |
| `QueueSize` | 16 | 单泳道队列深度（内存硬边界） |
| `CommitInterval` | 100ms（`DefaultConfig`） | 提交窗口；`0` = 连续点推进即提交 |
| `PollTimeout` | 100ms | 单次 poll 最长阻塞 |
| `RebalanceTimeout` | 3s | rebalance 收尾超时 |
| `Retry` | 零值（不重试） | 退避重试策略 |
| `OnDiscard` | nil（失败即致命） | 重试耗尽回调，可写 DLQ |
| `KeyExtractor` | nil（用 msg.Key） | 自定义泳道路由 key |

零值语义：除 `CommitInterval`（`0` = 推进即提交）与 `Retry.MaxAttempts`（`0` = 不重试）外，其余字段 `0` 均回落 `DefaultConfig()`。

## 失败处理语义

```
handler 返回 error
  ├─ Retry.MaxAttempts > 0 → 泳道内退避重试
  ├─ 重试耗尽 && OnDiscard != nil → 调 OnDiscard → 跳过，offset 推进
  └─ 重试耗尽 && OnDiscard == nil → 致命：offset 不提交，Run() 返回错误（重启重新消费）
```

## 接入其他 MQ

实现 `swimlane.Backend` SPI 即可（见 `backend.go`）：`Poll`/`Commit`/`Pause`/`Resume`/`Subscribe`/`SetRebalanceHandler`。新增 `backend/rocketmq/` 目录，core 无需任何改动。

## 测试

```bash
go test ./... -race
```

核心逻辑全部基于 fake Backend 测试，无需真实 MQ。
