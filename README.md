# mq-parallel-consumer

MQ 无关的泳道并发消费 SDK：单 partition 内**同 key 串行、异 key 并发**，offset 按**最大连续已完成**位置提交。核心零外部依赖，经 `Backend` SPI 适配 MQ，内置 Kafka（franz-go）。

## 能力

- **两种顺序模式**：`KeyOrdered` / `Unordered`
- **最大连续 offset 提交**：并发乱序完成下只提交连续区间，崩溃恢复零丢失
- **背压**：单 partition 在途上限 pause/resume + 有界队列
- **失败处理**：内置退避重试 + `OnDiscard` 回调；默认失败即致命上报
- **rebalance 优雅收尾**：revoked 时 drain 在途并提交最终 offset
- **线程安全**：`New`/`Subscribe`/`Run`/`Stop`/`Stats`

## 原理

一个 poll 循环拉取，按 key 哈希路由到固定泳道；同 key 恒落同一泳道（串行），异 key 跨泳道并发；完成即推进连续指针。

```
                    ┌────────────────────────────────────┐
                    │        poll 循环（单 goroutine）      │
                    │   Backend.Poll() 拉取一批消息          │
                    │   按 key 哈希 → 路由到泳道             │
                    │   定期提交「最大连续 offset」          │
                    └─────────────────┬──────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        ▼                             ▼                             ▼
  ┌────────────┐                ┌────────────┐              ┌────────────┐
  │  泳道 0     │                │  泳道 1     │    ···      │  泳道 N-1   │
  │ q→worker0  │                │ q→worker1  │              │ q→workerN  │
  │ 同 key 串行 │                │ 同 key 串行 │              │ 同 key 串行 │
  └─────┬──────┘                └─────┬──────┘              └─────┬──────┘
        │                             │                           │
        └───────────────┬─────────────┴─────────────┬─────────────┘
                        ▼                           ▼
             ┌───────────────────────────────────────────────┐
             │        offsetTracker（每 partition 一个）       │
             │  完成 → complete(offset)，向前扫描推进连续指针     │
             │  base = 最大连续已完成 + 1                      │
             └───────────────────┬───────────────────────────┘
                                 ▼
                    Commit(base)：值为「下一个待消费 offset」
                    崩溃恢复从 base 继续，at-least-once 零丢失
```

## 性能

IO 密集 handler（阻塞 1ms）+ 2000 条消息，`go run ./bench` 实测：

```
config                elapsed        msg/s  speedup
sequential             2.33s          858     1.0x
swimlane L=1           2.32s          865     1.0x
swimlane L=4           570ms         3508     4.1x
swimlane L=8           286ms         7004     8.2x
swimlane L=16          150ms        13311    15.6x
swimlane L=32           77ms        26001    30.5x
```

近线性扩展（前提：key 均匀、IO 密集）。

## 快速开始

```go
cfg := swimlane.DefaultConfig()
cfg.Lanes = 8

be, err := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Group: "demo"})
c, err := swimlane.New(be, cfg)
c.Subscribe([]string{"demo-topic"}, func(ctx context.Context, msg *swimlane.Message) error {
    fmt.Printf("partition=%d offset=%d key=%s value=%s\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
    return nil
})
err = c.Run(ctx)
```

## 配置

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Mode` | `KeyOrdered` | 顺序模式 |
| `Lanes` | 8 | KeyOrdered 泳道数（单 partition 并发度） |
| `Concurrency` | 8 | Unordered 并发数 |
| `MaxInFlight` | 并发度 × `QueueSize` | 在途上限，触发 pause |
| `QueueSize` | 16 | 单泳道队列深度 |
| `CommitInterval` | 100ms | 提交窗口；`0` = 推进即提交 |
| `PollTimeout` | 100ms | 单次 poll 最长阻塞 |
| `RebalanceTimeout` | 3s | rebalance 收尾超时 |
| `Retry` | 零值=不重试 | 退避重试策略 |
| `OnDiscard` | nil=失败即致命 | 重试耗尽回调 |
| `KeyExtractor` | nil=用 msg.Key | 自定义路由 key |

## 失败处理

```
handler 返回 error
  ├─ Retry.MaxAttempts > 0 → 泳道内退避重试
  ├─ 重试耗尽 && OnDiscard != nil → 调 OnDiscard → 跳过，offset 推进
  └─ 重试耗尽 && OnDiscard == nil → 致命：offset 不提交，Run() 返回错误
```

## 接入其他 MQ

实现 `Backend` SPI（`backend.go`）即可：`Poll`/`Commit`/`Pause`/`Resume`/`Subscribe`/`SetRebalanceHandler`。新增 `backend/<mq>/` 目录，core 零改动。

## 测试

```bash
go test ./... -race                                      # 引擎测试（内存 backend，无需 MQ）
docker compose -f examples/kafka-e2e/docker-compose.yaml up -d
go run ./examples/kafka-e2e                            # 真实 Kafka 端到端（含断言）
go run ./bench                                         # 并发基准
```
