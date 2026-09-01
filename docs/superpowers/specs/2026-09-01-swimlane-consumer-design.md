# Swimlane Consumer SDK 设计文档

- 日期：2026-09-01
- 模块：`mq-parallel-consumer`（本地占位模块路径，发布前补 `github.com/<user>/` 前缀）
- 核心包名：`swimlane`（模块名描述定位，包名用机制名；使用者 `import "mq-parallel-consumer"` 后以 `swimlane.New` 调用）
- 状态：待评审

## 1. 背景与目标

Kafka 单 partition 的消费并行度受限于 partition 数，且慢消息会队头阻塞整个 partition。本 SDK 在 consumer 端模拟更细粒度分区：**消费线程拉取与提交，消息按 key 哈希路由到固定泳道，同泳道串行、跨泳道并发，offset 提交采用最大连续已完成位置**。

### 目标

- **MQ 无关**：core 不依赖任何具体 MQ/客户端，通过 `Backend` SPI 适配不同 MQ；Kafka（franz-go）是第一个适配实现，后续可平滑接入 RocketMQ 等。
- **轻量**：core 零外部依赖（仅标准库）；无自动 DLQ、无自动重试、无内置 metrics、无额外后台 goroutine。
- **两种顺序模式**：`KeyOrdered`（同 key 串行，异 key 并发）与 `Unordered`（完全并发）。
- **线程安全**：Consumer 对使用者而言就是一个普通线程安全 SDK 对象，内部并发完全封装。
- **可配置化**：每个行为都是显式配置项，有清晰零值语义，可被关闭。

### 非目标（v1 明确不做）

- 内置 DLQ producer（提供 `OnDiscard` 回调，由使用者决定写哪）。
- 内置 metrics 采集（预留 `Stats()` 快照能力）。
- 事务 / Exactly-Once。
- 热点 key 自动识别与隔离。
- 直接 assign 模式（v1 以 group 模式为主）。

## 2. 架构总览

### 模块划分

```
mq-parallel-consumer/       ← 模块根（公开门面，零外部依赖，仅标准库）
├── go.mod
├── message.go                Message / TopicPartition / Offset / Header（转发）
├── backend.go                Backend SPI + RebalanceHandler（转发）
├── config.go                 Config / Mode / RetryPolicy / DefaultConfig（转发）
├── consumer.go               Consumer + New（转发入口）
├── errors.go / stats.go      （转发）
├── internal/
│   └── engine/               ← 实现：Consumer 引擎 / PartitionWorker / lane / offsetTracker / retry，类型真实定义
└── backend/
    └── kafka/                ← adapter（依赖根包 + franz-go，唯一接触 kafka 的地方）
        ├── backend.go        franz-go 实现 Backend
        ├── config.go         brokers / SASL / group 等 kafka 专属配置
        └── backend_test.go
```

后续接入 RocketMQ 即新增 `backend/rocketmq/`，实现同一 SPI，**core 一行不改**。

### 依赖方向

```
core（swimlane） ──无──→
kafka adapter ──依赖──→ core + franz-go
```

单向、无环。core 不知道 kafka；adapter 不知道泳道与 offset 推进。

### 边界原则

- **core**：只认识抽象消息与抽象偏移，负责并发、顺序、连续提交、背压、重试、rebalance 编排。
- **adapter**：只负责传输（poll / commit / pause / resume / rebalance 事件翻译），无业务逻辑。

## 3. Backend SPI

```go
// Mode 消费顺序模式
type Mode int

const (
    KeyOrdered Mode = iota // 同 key 串行，异 key 并发（默认）
    Unordered              // 完全并发，无顺序保证
)

type TopicPartition struct {
    Topic     string
    Partition int32
}
type Offset int64

// Message 与具体 MQ 无关的消息表示
type Message struct {
    Topic     string
    Partition int32
    Offset    Offset
    Key       []byte
    Value     []byte
    Headers   []Header
    Timestamp time.Time
}

// RebalanceHandler 由 core 实现，adapter 在 rebalance 时同步回调
type RebalanceHandler interface {
    // OnRevoked 分区被撤销。adapter 在 poll 线程上同步调用；
    // core 在此 drain 在途消息并提交最终 offset，保证不丢消息。
    // 返回后 adapter 才继续 rejoin。
    OnRevoked(ctx context.Context, revoked []TopicPartition) error
    // OnAssigned 分区被分配。offset 为该 partition 的起始消费位置。
    OnAssigned(ctx context.Context, assigned map[TopicPartition]Offset) error
}

// Backend 是 core 与 MQ 适配层之间的 SPI（传输层抽象）
type Backend interface {
    SetRebalanceHandler(h RebalanceHandler)
    // Poll 拉取一批消息，阻塞至多 maxWait
    Poll(ctx context.Context, maxWait time.Duration) ([]Message, error)
    // Commit 提交 offsets；值为"下一个待消费 offset"（与连续指针语义一致）
    Commit(ctx context.Context, commits map[TopicPartition]Offset) error
    Pause(parts []TopicPartition) error
    Resume(parts []TopicPartition) error
    Close(ctx context.Context) error
}
```

### 设计说明

- **rebalance 通过接口回调而非 core 自行感知**：partition 所有权转移的线程与时机由各 MQ 决定（franz-go 的 `OnPartitionsRevoked` 在 poll 线程同步触发，且要求钩子返回前完成收尾）。adapter 负责把 MQ 钩子翻译成对 `RebalanceHandler` 的调用，core 持有在途状态并决定 drain/提交时机。
- **同步批量提交**：poll 循环每隔 `CommitInterval` 汇总各 partition 的连续 offset 调一次 `Commit`。提交量小，异步化收益低。
- **重复消息保护**：poll 循环发现消息 `Offset < tracker.base` 时直接跳过（rebalance 后的重投递，已被提交过）。

## 4. Core 引擎

### 4.1 Consumer

`Consumer` 拥有一个 **poll 循环 goroutine**（唯一持有 `Backend` 调用方）和一组 **`PartitionWorker`**（每个已分配 partition 一个）。

```
poll 循环 goroutine                    worker goroutines
┌──────────────────────┐              ┌───────────────────────────┐
│ backend.Poll(maxWait) │──消息──→ route│ PartitionWorker            │
│   ↳ 背压检查(pause)    │              │  KeyOrdered: lanes[]        │
│   ↳ rebalance 回调    │              │    lane0 q→worker0 (串行)    │
│   ↳ 定时 Commit        │              │    lane1 q→worker1          │
│   ↳ OnRevoked 时 drain │              │  Unordered: semaphore 池    │
└──────────────────────┘              │  处理完 → tracker.complete() │
                                      └───────────────────────────┘
```

### 4.2 PartitionWorker（泳道组 / 并发池）

```go
type partitionWorker struct {
    tp      TopicPartition
    mode    Mode
    lanes   []*lane            // KeyOrdered：len = cfg.Lanes
    sem     chan struct{}      // Unordered：容量 = cfg.Concurrency
    tracker *offsetTracker
    inflight sync.WaitGroup
    closed  atomic.Bool
}
```

- **KeyOrdered**：`idx := fnv32a(keyExtractor(msg)) % len(lanes)`，阻塞写入有界队列（容量 `QueueSize`）。每个 lane 一个 worker goroutine，按序取消息处理——同 key 必然同 lane → 天然串行；异 key → 完全并行。
- **Unordered**：每条消息从 `sem` 取一个槽，goroutine 处理完释放；不保证顺序，offset 由 tracker 兜底连续提交。
- **空 key / keyExtractor 返回空**：KeyOrdered 下统一路由到 lane 0（空 key 全部串行，符合语义直觉）。
- **重复消息**：`Offset < tracker.base` 直接跳过，不处理。

### 4.3 OffsetTracker —— 最大连续 offset 状态机

并发处理必然乱序完成。用「已完成集合 + 连续指针」实现，**每次推进摊还 O(1)**：

```go
type offsetTracker struct {
    mu   sync.Mutex
    done map[Offset]struct{} // 已完成但未越过连续区间的 offset
    base Offset              // 下一个待提交 offset（最大连续已完成 = base-1）
}

// 消息处理完（含重试耗尽/跳过）后调用；返回新的 base
func (t *offsetTracker) complete(o Offset) Offset {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.done[o] = struct{}{}
    for { // 向前扫描连续区间，摊还 O(1)
        if _, ok := t.done[t.base]; !ok {
            break
        }
        delete(t.done, t.base)
        t.base++
    }
    return t.base
}
```

- `complete()` 在 worker goroutine 调用（加锁）；`base` 读取仅在 poll 循环（单线程，无需锁）。
- 相比"每泳道水位线取最小值"的保守近似，本实现**精确**且实现更简单（无需泳道水位线，也无 skew 导致的提交滞后）。

### 4.4 背压

- 每 partition 统计在途数（已路由未完成）。
- `inFlight >= MaxInFlight` → `Backend.Pause([tp])`；降到 `0.8 * MaxInFlight` → `Backend.Resume([tp])`。
- **有界 lane 队列**（`QueueSize`）为内存硬边界：极端情况下 poll 阻塞在 lane 写通道，不会 OOM。
- `MaxInFlight` 与 `QueueSize` 双保险，防止内存溢出。

### 4.5 重试与失败处理

```go
type RetryPolicy struct {
    MaxAttempts    int           // 0 = 不重试
    InitialBackoff time.Duration
    MaxBackoff     time.Duration // 指数退避 InitialBackoff * 2^n，封顶 MaxBackoff
}
```

失败处理路径（**默认不做多余的事**）：

```
handler 返回 error
  ├─ Retry.MaxAttempts > 0 → lane 内退避重试（KeyOrdered 下阻塞该泳道）
  ├─ 重试耗尽，且 OnDiscard != nil → 调 OnDiscard（使用者决定写 DLQ 或丢弃）→ complete 该 offset
  └─ 重试耗尽，且 OnDiscard == nil → 致命：该 offset 不 complete、不提交
        → Run() 返回该错误，消费终止；重启后从未提交处重新消费（at-least-once，零丢失）
```

默认路径是最后一条：像普通 SDK 一样把错误抛回给调用方，不静默跳过、不隐式重试。

### 4.6 Rebalance 时序

```
OnAssigned(ctx, assigned map[TopicPartition]Offset)
  → 对每个 tp 建 PartitionWorker，tracker.base = 该 partition 起始 offset

OnRevoked(ctx, revoked []TopicPartition)
  → 逐个 worker.drain(ctx)：
      置 closed、关闭 lane 队列 → worker 排空队列后退出 → inflight.Wait()
      (超时 RebalanceTimeout → 强制结束，未完成消息由接管方重新消费，at-least-once)
  → 收集各 tracker 最终 base → Backend.Commit(最终 offsets)
  → 删除 worker
```

kafka adapter 的 `OnPartitionsRevoked`（franz-go 在 poll 线程同步触发）会调用 core 的 `OnRevoked`，**阻塞到收尾完成再返回**——这是不丢消息的保证。

### 4.7 Run 循环

```
for {
    select { case <-ctx.Done(): drain 全部 + 最终提交; return; default: }
    if 到 CommitInterval: commitAll()   // 汇总各 tracker base → backend.Commit
    msgs, err := backend.Poll(ctx, PollTimeout)
    if err != nil: 透传/记录
    for m := range msgs: workers[m.tp].route(m)
}
```

`ctx` 取消或内部致命错误时：drain 全部 partition → 最终提交 → 返回 error（或 nil 表示正常关闭）。

## 5. Config 与默认值

```go
type Config struct {
    Mode Mode // 零值 = KeyOrdered

    // —— 并发度（互相独立，只在其模式下生效）——
    Lanes       int // KeyOrdered：单 partition 泳道数
    Concurrency int // Unordered：单 partition 并发数

    // —— 背压 / 内存 ——
    MaxInFlight int // 单 partition 在途上限，0 = 由并发度推导
    QueueSize   int // 单泳道队列深度

    // —— 时序 ——
    CommitInterval   time.Duration // 0 = 每次连续点推进立即提交
    PollTimeout      time.Duration // 单次 poll 最长阻塞
    RebalanceTimeout time.Duration // rebalance 收尾超时

    // —— 失败处理（零值 = 什么都不做）——
    Retry     RetryPolicy // 零值 = 不重试
    OnDiscard func(ctx context.Context, msg *Message, err error)

    // —— 路由 ——
    KeyExtractor func(*Message) string // nil = 用 msg.Key
}
```

### 默认值

| 字段 | 默认值 | 说明 |
|---|---|---|
| Mode | `KeyOrdered` | SDK 核心能力 |
| Lanes | 8 | KeyOrdered 下单 partition 并发度 |
| Concurrency | 8 | Unordered 下单 partition 并发数 |
| MaxInFlight | `Lanes`（或 `Concurrency`）× `QueueSize` | 在途上限 |
| QueueSize | 16 | 单泳道队列深度 |
| CommitInterval | 100ms | 0 = 推进即提交 |
| PollTimeout | 100ms | — |
| RebalanceTimeout | 3s | drain 收尾超时 |
| Retry | 零值 | 不重试 |
| OnDiscard | nil | 失败即致命 |
| KeyExtractor | nil | 用 `msg.Key` |

`DefaultConfig()` 返回上表值；`New` 时对**零值字段**回落默认，显式非零优先，非法值（负数）返回错误。

## 6. Kafka adapter（`backend/kafka`）

唯一职责：把 franz-go 翻译成 Backend SPI，无业务逻辑。

| SPI 方法 | franz-go 映射 |
|---|---|
| `Poll(ctx, maxWait)` | `PollFetches(ctx, maxWait)`，摊平 `[]*kmsg.FetchTopic` 转 `[]*Message` |
| `Commit(ctx, commits)` | `CommitOffsets(ctx, ...)`，value = 下一个待消费 offset |
| `Pause/Resume` | `PauseFetchTopics` / `ResumeFetchTopics` |
| `SetRebalanceHandler` | `OnPartitionsAssigned/Revoked` 钩子转发；`OnRevoked` 同步阻塞到 core 收尾完成 |
| `Close` | `Client.Close()` |

kafka 专属配置：brokers、SASL/TLS、group、auto-commit 关闭（SDK 完全掌控提交）、`ConsumeResetOffset`（默认 earliest，可配）。

**v1 以 group 模式为主**：SDK 自管提交，多实例可横向扩展消费不同 partition。直接 assign 模式留作后续能力。

## 7. 错误模型

`Run()` 返回的 error 是唯一错误出口，用 `%w` 包装：

- `ErrFatalHandler` —— 包装 handler 原始错误（配置了 Retry/OnDiscard 时不会走到）。
- 后端 SPI 错误（如 `ErrBackendClosed`）透传。

调用方可 `errors.Is/As` 定位。不设错误回调、不设全局状态。

## 8. 线程安全与 API

```go
c, err := swimlane.New(be, cfg) // 校验配置，直接可用
c.Subscribe("topic", handler)   // 订阅（可重复调用）
err := c.Run(ctx)               // 阻塞；内部错误/ctx 取消时返回
c.Stop()                        // 任意 goroutine 调用，触发优雅收尾
s := c.Stats()                  // 任意时刻取快照：在途数、各 partition 连续 offset
```

- 内部并发全部封装：poll 循环 goroutine + worker goroutine 为引擎必要部分；`Stop` 触发 drain + 最终提交。
- 使用者只需关心 `Run()` 返回的 error——与 `http.ListenAndServe` 同款心智。

## 9. 测试策略

core 零 MQ 依赖，用 fake Backend 即可测全引擎：

1. **单测**：
   - OffsetTracker：乱序完成、跨连续区间、重复 offset。
   - lane 路由：同 key 同 lane、空 key 兜底。
   - retry 状态机：重试计数、OnDiscard 触发、致命错误传播。
2. **引擎级**（fake Backend 模拟 poll/rebalance/commit）：
   - 断言提交的 offset 序列恰好是最大连续点。
   - 模拟 revoke，验证 drain 时序与最终提交。
   - 背压验证 pause/resume 调用。
3. **adapter 契约测试**：fetch→Message 转换、offset 映射的纯逻辑单测，不连真 broker。
4. **集成测试**：v1 暂不做（testcontainers 依赖较重，后续按需引入，独立 `//go:build integration` tag）。

## 10. 未来扩展（明确不在 v1）

- `backend/rocketmq` adapter（同一 SPI）。
- 直接 assign 模式。
- 内置 metrics 采集。
- 热点 key 自动识别与隔离、动态泳道。
- 事务 / Exactly-Once。
