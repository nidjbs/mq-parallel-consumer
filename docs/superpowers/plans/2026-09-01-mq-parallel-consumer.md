# mq-parallel-consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个 MQ 无关的泳道并发消费 SDK：`Backend` SPI 抽象传输层，core 引擎支持 `KeyOrdered`（同 key 串行）与 `Unordered`（完全并发）两种模式，按最大连续 offset 提交。

**Architecture:** 单模块两包：根包 `swimlane`（core，零外部依赖）只依赖一个抽象 `Backend` 接口；`backend/kafka` 用 franz-go 实现该接口。core 内部：一个 poll 循环 goroutine 拉取/提交/处理 rebalance，每 partition 一个 `PartitionWorker`（KeyOrdered 用固定泳道数组、Unordered 用信号量池），每个 worker 持一个 `offsetTracker` 维护最大连续 offset。

**Tech Stack:** Go 1.26+；franz-go（仅 kafka adapter）；标准库 `context/sync/hash/fnv`。

**Spec:** `docs/superpowers/specs/2026-09-01-swimlane-consumer-design.md`

## Global Constraints

- **core 零外部依赖**：根包 `swimlane` 只允许标准库。franz-go 只出现在 `backend/kafka`。
- **模块路径占位**：go.mod 用 `module mq-parallel-consumer`，发布前补前缀；根包名 `swimlane`。
- **`CommitInterval == 0` = 推进即提交**（合法语义，不做默认回填）；想要定时提交就显式设值或从 `DefaultConfig()` 起步。
- **`Retry.MaxAttempts == 0` = 不重试**；**`OnDiscard == nil` 且重试耗尽 = 致命**：该 offset 不 complete、不提交，`Run()` 返回错误。绝不静默丢消息。
- **重复消息**：`offset < tracker.base` 的 message 不处理直接跳过。
- **未 seed 的 partition 不提交**：partition 一个消息都未路由到时，不得对其提交（避免把真实起始 offset 冲成 0）。
- **测试**：每个任务必须 `go test ./...` 通过后再提交；kafka adapter 不连真 broker。
- 代码注释极简英文；API 与命名以 spec 为准。

---

### Task 1: 模块脚手架 + 核心类型 + Config

**Files:**
- Create: `go.mod`
- Create: `message.go`
- Create: `backend.go`
- Create: `errors.go`
- Create: `config.go`
- Test: `config_test.go`

**Interfaces:**
- Produces: `Mode`, `KeyOrdered`, `Unordered`, `TopicPartition{Topic,Partition}`, `Offset(int64)`, `Header{Key,Value}`, `Message{Topic,Partition,Offset,Key,Value,Headers,Timestamp}` + `(*Message) TP() TopicPartition`；`RebalanceHandler{OnRevoked,OnAssigned}`；`Backend{SetRebalanceHandler,Subscribe,Poll,Commit,Pause,Resume,Close}`；`RetryPolicy{MaxAttempts,InitialBackoff,MaxBackoff}`；`Config{...}`；`DefaultConfig()`；`ErrInvalidConfig`, `ErrClosed`, `ErrHandlerFatal`。

- [ ] **Step 1: Write the failing test**

`config_test.go`:

```go
package swimlane

import (
    "testing"
    "time"
)

func TestDefaultConfig(t *testing.T) {
    d := DefaultConfig()
    if d.Mode != KeyOrdered {
        t.Fatalf("Mode = %v, want KeyOrdered", d.Mode)
    }
    if d.Lanes != 8 || d.QueueSize != 16 || d.CommitInterval != 100*time.Millisecond {
        t.Fatalf("unexpected defaults: %+v", d)
    }
}

func TestConfigWithDefaults(t *testing.T) {
    c, err := (Config{Mode: Unordered, Concurrency: 4}).withDefaults()
    if err != nil {
        t.Fatal(err)
    }
    if c.Lanes != 8 || c.Concurrency != 4 {
        t.Fatalf("zero fields should fall back, explicit kept: %+v", c)
    }
    if c.MaxInFlight != 4*c.QueueSize {
        t.Fatalf("MaxInFlight = %d, want %d", c.MaxInFlight, 4*c.QueueSize)
    }
    if c.CommitInterval != 100*time.Millisecond {
        t.Fatalf("CommitInterval should default to 100ms, got %v", c.CommitInterval)
    }
}

func TestConfigCommitIntervalZeroKept(t *testing.T) {
    c, err := (Config{Mode: KeyOrdered, CommitInterval: 0}).withDefaults()
    if err != nil {
        t.Fatal(err)
    }
    if c.CommitInterval != 0 {
        t.Fatalf("CommitInterval=0 must mean commit-on-advance, got %v", c.CommitInterval)
    }
}

func TestConfigInvalid(t *testing.T) {
    cases := []Config{
        {Mode: Mode(99)},
        {Mode: KeyOrdered, QueueSize: 0},
        {Mode: KeyOrdered, PollTimeout: 0},
        {Mode: Unordered, Concurrency: -1},
    }
    for _, c := range cases {
        if _, err := c.withDefaults(); err == nil {
            t.Fatalf("expected error for %+v", c)
        }
    }
}

func TestMessageTP(t *testing.T) {
    m := Message{Topic: "t", Partition: 2}
    tp := m.TP()
    if tp != (TopicPartition{Topic: "t", Partition: 2}) {
        t.Fatalf("TP = %+v", tp)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: compile error（`package swimlane: directory ... does not exist` / 无 go.mod）

- [ ] **Step 3: Implement minimal code**

```bash
cd /Users/mac/project/mq-parallel-consumer
go mod init mq-parallel-consumer
```

`message.go`:

```go
package swimlane

import "time"

// TopicPartition identifies a partition/queue.
type TopicPartition struct {
    Topic     string
    Partition int32
}

// Offset is a MQ-agnostic position.
type Offset int64

// Header is a MQ-agnostic message header.
type Header struct {
    Key   string
    Value []byte
}

// Message is the transport-agnostic record.
type Message struct {
    Topic     string
    Partition int32
    Offset    Offset
    Key       []byte
    Value     []byte
    Headers   []Header
    Timestamp time.Time
}

// TP returns the message's topic-partition.
func (m *Message) TP() TopicPartition {
    return TopicPartition{Topic: m.Topic, Partition: m.Partition}
}
```

`backend.go`:

```go
package swimlane

import (
    "context"
    "time"
)

// Mode is the ordering guarantee offered by the engine.
type Mode int

const (
    KeyOrdered Mode = iota // same key serial, different keys concurrent
    Unordered              // fully concurrent, no ordering
)

// RebalanceHandler is implemented by the core; adapters call it on rebalance.
type RebalanceHandler interface {
    // OnRevoked is called synchronously on the poll goroutine; the core
    // drains in-flight messages and commits final offsets before returning.
    OnRevoked(ctx context.Context, revoked []TopicPartition) error
    // OnAssigned is called when partitions are assigned. offset is the
    // partition's start position (may be 0 if unknown).
    OnAssigned(ctx context.Context, assigned map[TopicPartition]Offset) error
}

// Backend is the SPI adapters implement (transport layer).
type Backend interface {
    SetRebalanceHandler(h RebalanceHandler)
    // Subscribe registers topics to consume.
    Subscribe(topics []string) error
    // Poll blocks up to maxWait and returns a batch of messages.
    Poll(ctx context.Context, maxWait time.Duration) ([]Message, error)
    // Commit persists offsets; the value is the "next offset to consume".
    Commit(ctx context.Context, commits map[TopicPartition]Offset) error
    Pause(parts []TopicPartition) error
    Resume(parts []TopicPartition) error
    Close(ctx context.Context) error
}
```

`errors.go`:

```go
package swimlane

import "errors"

var (
    ErrInvalidConfig = errors.New("swimlane: invalid config")
    ErrClosed        = errors.New("swimlane: closed")
    ErrHandlerFatal  = errors.New("swimlane: handler failed")
)
```

`config.go`:

```go
package swimlane

import (
    "context"
    "errors"
    "fmt"
    "time"
)

// Handler processes a single message. It runs on worker goroutines.
type Handler func(ctx context.Context, msg *Message) error

// RetryPolicy controls in-lane retries. Zero value means no retry.
type RetryPolicy struct {
    MaxAttempts    int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
}

// Config is fully configurable; every field has zero-value semantics.
type Config struct {
    Mode             Mode
    Lanes            int // KeyOrdered: lanes per partition
    Concurrency      int // Unordered: concurrency per partition
    MaxInFlight      int // per-partition in-flight cap; 0 = derived
    QueueSize        int // per-lane bounded queue depth
    CommitInterval   time.Duration // 0 = commit on contiguous advance
    PollTimeout      time.Duration
    RebalanceTimeout time.Duration
    Retry            RetryPolicy
    OnDiscard        func(ctx context.Context, msg *Message, err error)
    KeyExtractor     func(*Message) string // nil = use msg.Key
}

// DefaultConfig returns the recommended configuration.
func DefaultConfig() Config {
    return Config{
        Mode:             KeyOrdered,
        Lanes:            8,
        Concurrency:      8,
        QueueSize:        16,
        CommitInterval:   100 * time.Millisecond,
        PollTimeout:      100 * time.Millisecond,
        RebalanceTimeout: 3 * time.Second,
    }
}

// withDefaults applies defaults to zero fields and validates. CommitInterval
// and Retry.MaxAttempts keep 0 as a meaningful value.
func (c Config) withDefaults() (Config, error) {
    if c.Mode != KeyOrdered && c.Mode != Unordered {
        return c, fmt.Errorf("%w: unknown mode %d", ErrInvalidConfig, c.Mode)
    }
    d := DefaultConfig()
    if c.Lanes == 0 {
        c.Lanes = d.Lanes
    }
    if c.Concurrency == 0 {
        c.Concurrency = d.Concurrency
    }
    if c.QueueSize == 0 {
        c.QueueSize = d.QueueSize
    }
    if c.CommitInterval == 0 {
        c.CommitInterval = d.CommitInterval
    }
    if c.PollTimeout == 0 {
        c.PollTimeout = d.PollTimeout
    }
    if c.RebalanceTimeout == 0 {
        c.RebalanceTimeout = d.RebalanceTimeout
    }
    concurrency := c.Concurrency
    if c.Mode == KeyOrdered {
        concurrency = c.Lanes
    }
    if c.MaxInFlight == 0 {
        c.MaxInFlight = concurrency * c.QueueSize
    }
    if c.Lanes < 0 || c.Concurrency < 0 || c.QueueSize <= 0 || c.MaxInFlight <= 0 ||
        c.CommitInterval < 0 || c.PollTimeout <= 0 || c.RebalanceTimeout <= 0 {
        return c, fmt.Errorf("%w: non-positive value in %+v", ErrInvalidConfig, c)
    }
    if c.Retry.MaxAttempts < 0 {
        return c, errors.New("swimlane: Retry.MaxAttempts must be >= 0")
    }
    return c, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add go.mod message.go backend.go errors.go config.go config_test.go
git commit -m "feat: scaffold core types, Backend SPI, and config"
```

---

### Task 2: OffsetTracker（最大连续 offset 状态机）

**Files:**
- Create: `offsettracker.go`
- Test: `offsettracker_test.go`

**Interfaces:**
- Consumes: `Offset`（Task 1）
- Produces: `offsetTracker` with `newOffsetTracker()`, `seed(o Offset)`, `complete(o Offset) Offset`, `baseOffset() Offset`, `seeded() bool`

- [ ] **Step 1: Write the failing test**

`offsettracker_test.go`:

```go
package swimlane

import "testing"

func TestContiguousAdvance(t *testing.T) {
    tr := newOffsetTracker()
    tr.seed(0)
    if got := tr.complete(0); got != 1 {
        t.Fatalf("complete(0) base = %d, want 1", got)
    }
    if got := tr.complete(1); got != 2 {
        t.Fatalf("complete(1) base = %d, want 2", got)
    }
}

func TestOutOfOrderCompletion(t *testing.T) {
    tr := newOffsetTracker()
    tr.seed(0)
    tr.complete(2) // out of order: base stays 0
    if got := tr.complete(1); got != 3 { // 1 completes the run 0,1,2
        t.Fatalf("base = %d, want 3", got)
    }
}

func TestDuplicateIgnored(t *testing.T) {
    tr := newOffsetTracker()
    tr.seed(0)
    tr.complete(0)
    if got := tr.complete(0); got != 1 { // duplicate < base is ignored
        t.Fatalf("duplicate complete returned %d, want 1", got)
    }
}

func TestSeedLazily(t *testing.T) {
    tr := newOffsetTracker()
    if tr.seeded() {
        t.Fatal("tracker should start unseeded")
    }
    tr.seed(10)
    if got := tr.baseOffset(); got != 10 {
        t.Fatalf("base = %d, want 10", got)
    }
    if !tr.seeded() {
        t.Fatal("tracker should be seeded after seed()")
    }
    tr.seed(99) // second seed is a no-op
    if got := tr.baseOffset(); got != 10 {
        t.Fatalf("base = %d, want 10", got)
    }
}

func TestCompleteBeforeSeed(t *testing.T) {
    tr := newOffsetTracker()
    if got := tr.complete(5); got != 6 { // complete seeds implicitly
        t.Fatalf("base = %d, want 6", got)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: FAIL（`undefined: newOffsetTracker`）

- [ ] **Step 3: Implement minimal code**

`offsettracker.go`:

```go
package swimlane

import "sync"

// offsetTracker tracks completed offsets and advances a contiguous commit
// pointer. safe for concurrent use.
type offsetTracker struct {
    mu     sync.Mutex
    seeded bool
    done   map[Offset]struct{} // completed but not yet past the contiguous run
    base   Offset              // next offset to commit; contiguous done = base-1
}

func newOffsetTracker() *offsetTracker {
    return &offsetTracker{done: make(map[Offset]struct{})}
}

// seed initializes base from the first seen offset; later calls are no-ops.
func (t *offsetTracker) seed(o Offset) {
    t.mu.Lock()
    defer t.mu.Unlock()
    if !t.seeded {
        t.base = o
        t.seeded = true
    }
}

// complete marks o as done and returns the new base. Amortized O(1).
func (t *offsetTracker) complete(o Offset) Offset {
    t.mu.Lock()
    defer t.mu.Unlock()
    if !t.seeded {
        t.base = o
        t.seeded = true
    }
    if o < t.base {
        return t.base // duplicate already committed
    }
    t.done[o] = struct{}{}
    for {
        if _, ok := t.done[t.base]; !ok {
            break
        }
        delete(t.done, t.base)
        t.base++
    }
    return t.base
}

// baseOffset returns the next offset to commit.
func (t *offsetTracker) baseOffset() Offset {
    t.mu.Lock()
    defer t.mu.Unlock()
    return t.base
}

// seeded reports whether the tracker has observed at least one offset.
func (t *offsetTracker) seeded() bool {
    t.mu.Lock()
    defer t.mu.Unlock()
    return t.seeded
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add offsettracker.go offsettracker_test.go
git commit -m "feat: add contiguous offset tracker"
```

---

### Task 3: Retry 退避

**Files:**
- Create: `retry.go`
- Test: `retry_test.go`

**Interfaces:**
- Consumes: `RetryPolicy`（Task 1）
- Produces: `retryBackoff(p RetryPolicy, attempt int) time.Duration`（attempt 从 0 开始；`InitialBackoff * 2^attempt`，封顶 `MaxBackoff`）

- [ ] **Step 1: Write the failing test**

`retry_test.go`:

```go
package swimlane

import (
    "testing"
    "time"
)

func TestRetryBackoffExponential(t *testing.T) {
    p := RetryPolicy{MaxAttempts: 4, InitialBackoff: 100 * time.Millisecond}
    if got := retryBackoff(p, 0); got != 100*time.Millisecond {
        t.Fatalf("attempt0 = %v", got)
    }
    if got := retryBackoff(p, 1); got != 200*time.Millisecond {
        t.Fatalf("attempt1 = %v", got)
    }
    if got := retryBackoff(p, 2); got != 400*time.Millisecond {
        t.Fatalf("attempt2 = %v", got)
    }
}

func TestRetryBackoffCapped(t *testing.T) {
    p := RetryPolicy{MaxAttempts: 10, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}
    if got := retryBackoff(p, 5); got != 500*time.Millisecond {
        t.Fatalf("capped = %v", got)
    }
}

func TestRetryBackoffZero(t *testing.T) {
    p := RetryPolicy{}
    if got := retryBackoff(p, 3); got != 0 {
        t.Fatalf("no backoff = %v", got)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: FAIL（`undefined: retryBackoff`）

- [ ] **Step 3: Implement minimal code**

`retry.go`:

```go
package swimlane

import "time"

// retryBackoff computes the backoff for a given attempt (0-based).
func retryBackoff(p RetryPolicy, attempt int) time.Duration {
    if p.InitialBackoff <= 0 {
        return 0
    }
    d := p.InitialBackoff * (1 << uint(attempt))
    if p.MaxBackoff > 0 && d > p.MaxBackoff {
        return p.MaxBackoff
    }
    return d
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add retry.go retry_test.go
git commit -m "feat: add retry backoff calculation"
```

---

### Task 4: Lane + PartitionWorker

**Files:**
- Create: `lane.go`
- Create: `partition.go`
- Test: `partition_test.go`

**Interfaces:**
- Consumes: `Config`, `Handler`, `Mode`, `Message`, `offsetTracker`, `retryBackoff`, `ErrHandlerFatal`（Task 1-3）
- Produces:
  - `lane{idx int, q chan *Message, w *partitionWorker}` + `newLane(idx int, w *partitionWorker, size int) *lane` + `(*lane) run(ctx context.Context)`
  - `partitionWorker{tp TopicPartition, mode Mode, lanes []*lane, sem chan struct{}, tracker *offsetTracker, inflight sync.WaitGroup, inflightCount atomic.Int64, maxInFlight int}` + `newPartitionWorker(tp TopicPartition, cfg Config, fatalCh chan error) *partitionWorker` + `(*partitionWorker) start(ctx)`、`route(ctx, msg) bool`、`drain(ctx)`、`laneIndex(msg) int`、`baseOffset() Offset`、`inFlight() int64`

- [ ] **Step 1: Write the failing test**

`partition_test.go`:

```go
package swimlane

import (
    "context"
    "sync"
    "sync/atomic"
    "testing"
    "time"
)

func newTestConfig() Config {
    c := DefaultConfig()
    c.Lanes = 3
    c.QueueSize = 16
    return c
}

// newTestWorker builds a worker with the given handler and starts it.
func newTestWorker(cfg Config, h Handler) *partitionWorker {
    fatalCh := make(chan error, 1)
    w := newPartitionWorker(TopicPartition{Topic: "t", Partition: 0}, cfg, fatalCh)
    w.setHandler(h)
    w.start(context.Background())
    return w
}

// same key always goes to the same lane; per-key order preserved.
func TestKeyedLaneOrdering(t *testing.T) {
    cfg := newTestConfig()
    var mu sync.Mutex
    perKey := map[string][]int64{}
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
        mu.Lock()
        perKey[string(m.Key)] = append(perKey[string(m.Key)], int64(m.Offset))
        mu.Unlock()
        return nil
    })

    msgs := []*Message{
        {Key: []byte("a"), Offset: 0},
        {Key: []byte("b"), Offset: 1},
        {Key: []byte("a"), Offset: 2},
        {Key: []byte("c"), Offset: 3},
        {Key: []byte("b"), Offset: 4},
        {Key: []byte("a"), Offset: 5},
    }
    for _, m := range msgs {
        w.route(context.Background(), m)
    }
    w.drain(context.Background())

    want := map[string][]int64{
        "a": {0, 2, 5},
        "b": {1, 4},
        "c": {3},
    }
    if len(perKey) != len(want) {
        t.Fatalf("perKey = %+v", perKey)
    }
    for k, seq := range want {
        got, ok := perKey[k]
        if !ok {
            t.Fatalf("key %q missing", k)
        }
        for i := range seq {
            if got[i] != seq[i] {
                t.Fatalf("key %q seq = %v, want %v", k, got, seq)
            }
        }
    }
    // contiguous base must advance to 6
    if got := w.baseOffset(); got != 6 {
        t.Fatalf("base = %d, want 6", got)
    }
}

// empty key routes to lane 0.
func TestEmptyKeyLaneZero(t *testing.T) {
    cfg := newTestConfig()
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
    if idx := w.laneIndex(&Message{Key: nil}); idx != 0 {
        t.Fatalf("empty key idx = %d, want 0", idx)
    }
    w.drain(context.Background())
}

// retry then success: base advances; handler called MaxAttempts times.
func TestRetryThenSuccess(t *testing.T) {
    cfg := newTestConfig()
    cfg.Retry = RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}
    var calls atomic.Int32
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
        if calls.Add(1) < 3 {
            return context.DeadlineExceeded // transient failure
        }
        return nil
    })
    w.route(context.Background(), &Message{Offset: 0})
    w.drain(context.Background())

    if calls.Load() != 3 {
        t.Fatalf("handler calls = %d, want 3", calls.Load())
    }
    if got := w.baseOffset(); got != 1 {
        t.Fatalf("base = %d, want 1", got)
    }
}

// retry exhausted + OnDiscard: base advances, no fatal.
func TestRetryExhaustedOnDiscard(t *testing.T) {
    cfg := newTestConfig()
    cfg.Retry = RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}
    cfg.OnDiscard = func(ctx context.Context, m *Message, err error) {}
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return context.DeadlineExceeded })
    w.route(context.Background(), &Message{Offset: 0})
    w.drain(context.Background())

    if got := w.baseOffset(); got != 1 {
        t.Fatalf("base = %d, want 1", got)
    }
}

// retry exhausted + no OnDiscard: fatal, base does NOT advance.
func TestRetryExhaustedFatal(t *testing.T) {
    cfg := newTestConfig()
    cfg.Retry = RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Millisecond}
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return context.DeadlineExceeded })
    w.route(context.Background(), &Message{Offset: 0})
    w.drain(context.Background())

    if got := w.baseOffset(); got != 0 {
        t.Fatalf("base = %d, want 0 (uncommitted)", got)
    }
    select {
    case <-w.fatalCh:
    default:
        t.Fatal("expected fatal error")
    }
}

// duplicate offset (< base) is skipped without processing.
func TestDuplicateSkipped(t *testing.T) {
    cfg := newTestConfig()
    var calls atomic.Int32
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
        calls.Add(1)
        return nil
    })
    w.route(context.Background(), &Message{Offset: 0})
    w.route(context.Background(), &Message{Offset: 0}) // duplicate
    w.route(context.Background(), &Message{Offset: 1})
    w.drain(context.Background())

    if calls.Load() != 2 {
        t.Fatalf("handler calls = %d, want 2", calls.Load())
    }
}

// Unordered mode: all messages processed, base advances.
func TestUnorderedMode(t *testing.T) {
    cfg := DefaultConfig()
    cfg.Mode = Unordered
    cfg.Concurrency = 4
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
    for i := 0; i < 20; i++ {
        w.route(context.Background(), &Message{Offset: Offset(i)})
    }
    w.drain(context.Background())
    if got := w.baseOffset(); got != 20 {
        t.Fatalf("base = %d, want 20", got)
    }
}

// drain is idempotent.
func TestDrainIdempotent(t *testing.T) {
    cfg := newTestConfig()
    w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
    w.route(context.Background(), &Message{Offset: 0})
    w.drain(context.Background())
    w.drain(context.Background())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: FAIL（`undefined: newPartitionWorker`）

- [ ] **Step 3: Implement minimal code**

`lane.go`:

```go
package swimlane

import "context"

type lane struct {
    idx int
    q   chan *Message
    w   *partitionWorker
}

func newLane(idx int, w *partitionWorker, size int) *lane {
    return &lane{idx: idx, q: make(chan *Message, size), w: w}
}

// run drains the queue serially until the channel is closed.
func (l *lane) run(ctx context.Context) {
    for msg := range l.q {
        l.w.handle(ctx, msg)
    }
}
```

`partition.go`:

```go
package swimlane

import (
    "context"
    "errors"
    "fmt"
    "hash/fnv"
    "sync"
    "sync/atomic"
    "time"
)

// partitionWorker owns one partition's lanes and offset tracker.
type partitionWorker struct {
    tp            TopicPartition
    mode          Mode
    cfg           Config
    handler       Handler
    lanes         []*lane
    sem           chan struct{}
    tracker       *offsetTracker
    inflight      sync.WaitGroup
    inflightCount atomic.Int64
    maxInFlight   int
    fatalCh       chan error
    mu            sync.Mutex
    closed        bool
}

func newPartitionWorker(tp TopicPartition, cfg Config, fatalCh chan error) *partitionWorker {
    w := &partitionWorker{
        tp:          tp,
        mode:        cfg.Mode,
        cfg:         cfg,
        tracker:     newOffsetTracker(),
        maxInFlight: cfg.MaxInFlight,
        fatalCh:     fatalCh,
    }
    if cfg.Mode == Unordered {
        w.sem = make(chan struct{}, cfg.Concurrency)
    } else {
        w.lanes = make([]*lane, cfg.Lanes)
        for i := range w.lanes {
            w.lanes[i] = newLane(i, w, cfg.QueueSize)
        }
    }
    return w
}

// setHandler injects the message handler (set by the consumer on assignment).
func (w *partitionWorker) setHandler(h Handler) {
    w.handler = h
}

// start launches lane workers (no-op for Unordered).
func (w *partitionWorker) start(ctx context.Context) {
    if w.mode == KeyOrdered {
        for _, l := range w.lanes {
            go l.run(ctx)
        }
    }
}

// route hands a message to the partition; returns false if dropped.
// Must be called from the poll goroutine (never concurrent with drain).
func (w *partitionWorker) route(ctx context.Context, msg *Message) bool {
    w.mu.Lock()
    if w.closed {
        w.mu.Unlock()
        return false
    }
    w.mu.Unlock()

    w.tracker.seed(msg.Offset)
    if msg.Offset < w.tracker.baseOffset() {
        return true // duplicate, skip
    }

    w.inflight.Add(1)
    w.inflightCount.Add(1)
    if w.mode == Unordered {
        select {
        case w.sem <- struct{}{}:
        case <-ctx.Done():
            w.inflight.Done()
            w.inflightCount.Add(-1)
            return false
        }
        go func() {
            defer func() { <-w.sem }()
            defer w.inflight.Done()
            defer w.inflightCount.Add(-1)
            w.handle(ctx, msg)
        }()
        return true
    }

    idx := w.laneIndex(msg)
    select {
    case w.lanes[idx].q <- msg:
        return true
    case <-ctx.Done():
        w.inflight.Done()
        w.inflightCount.Add(-1)
        return false
    }
}

// laneIndex hashes the extracted key to a lane; empty key -> lane 0.
func (w *partitionWorker) laneIndex(msg *Message) int {
    if len(w.lanes) <= 1 {
        return 0
    }
    var key string
    if w.cfg.KeyExtractor != nil {
        key = w.cfg.KeyExtractor(msg)
    } else {
        key = string(msg.Key)
    }
    if key == "" {
        return 0
    }
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() % uint32(len(w.lanes)))
}

// drain closes lanes and waits for in-flight work with ctx timeout.
func (w *partitionWorker) drain(ctx context.Context) {
    w.mu.Lock()
    if w.closed {
        w.mu.Unlock()
        return
    }
    w.closed = true
    lanes := w.lanes
    w.mu.Unlock()

    if w.mode == KeyOrdered {
        for _, l := range lanes {
            close(l.q)
        }
    }
    done := make(chan struct{})
    go func() {
        w.inflight.Wait()
        close(done)
    }()
    select {
    case <-done:
    case <-ctx.Done():
        // timeout: force stop; unfinished messages re-consumed by new owner
    }
}

// handle processes a message and advances the tracker, or reports fatal.
func (w *partitionWorker) handle(ctx context.Context, msg *Message) {
    err := w.process(ctx, msg)
    if err == nil {
        w.tracker.complete(msg.Offset)
        return
    }
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
        return // shutdown/drain: leave offset uncommitted
    }
    w.reportFatal(fmt.Errorf("%w: %v", ErrHandlerFatal, err))
}

// process runs the handler with retry, then OnDiscard if configured.
func (w *partitionWorker) process(ctx context.Context, msg *Message) error {
    maxAttempts := w.cfg.Retry.MaxAttempts
    if maxAttempts <= 0 {
        maxAttempts = 1
    }
    var lastErr error
    for attempt := 0; attempt < maxAttempts; attempt++ {
        lastErr = w.handler(ctx, msg)
        if lastErr == nil {
            return nil
        }
        if attempt == maxAttempts-1 {
            break
        }
        backoff := retryBackoff(w.cfg.Retry, attempt)
        select {
        case <-time.After(backoff):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    if w.cfg.OnDiscard != nil {
        w.cfg.OnDiscard(ctx, msg, lastErr)
        return nil
    }
    return lastErr
}

// reportFatal delivers a fatal error to the poll loop (non-blocking).
func (w *partitionWorker) reportFatal(err error) {
    select {
    case w.fatalCh <- err:
    default:
    }
}

func (w *partitionWorker) baseOffset() Offset {
    return w.tracker.baseOffset()
}

func (w *partitionWorker) inFlight() int64 {
    return w.inflightCount.Load()
}

func (w *partitionWorker) isSeeded() bool {
    return w.tracker.seeded()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./... -race`
Expected: PASS（handler 通过 `setHandler` 注入，见 Task 4 Step 3）

- [ ] **Step 5: Commit**

```bash
git add lane.go partition.go partition_test.go
git commit -m "feat: add lane and partition worker with retry/discard/drain"
```

---

### Task 5: Consumer 引擎（Run/Stop/背压/rebalance）+ Stats

**Files:**
- Create: `consumer.go`
- Create: `stats.go`
- Test: `consumer_test.go`（含 fake Backend）

**Interfaces:**
- Consumes: `Backend`, `RebalanceHandler`, `Config`, `Handler`, `partitionWorker`, `offsetTracker`, `ErrInvalidConfig`, `ErrClosed`（Task 1-4）
- Produces:
  - `Consumer` + `New(backend Backend, cfg Config) (*Consumer, error)`、`(*Consumer) Subscribe(topics []string, handler Handler) error`、`(*Consumer) Run(ctx context.Context) error`、`(*Consumer) Stop()`、`(*Consumer) Stats() Stats`
  - `Stats{Mode, Partitions, InFlightTotal, PerPartition map[TopicPartition]PartitionStat}`、`PartitionStat{InFlight int64, BaseOffset Offset, MaxInFlight int}`
  - Consumer 实现 `RebalanceHandler`（`OnRevoked` / `OnAssigned`）

- [ ] **Step 1: Write the failing test**

`consumer_test.go`：

```go
package swimlane

import (
    "context"
    "fmt"
    "sync"
    "testing"
    "time"
)

// fakeBackend drives the engine deterministically without a real MQ.
type fakeBackend struct {
    mu     sync.Mutex
    queue  []Message
    commits []map[TopicPartition]Offset
    paused  map[TopicPartition]bool
    h       RebalanceHandler
    closed  bool
}

func (f *fakeBackend) SetRebalanceHandler(h RebalanceHandler) { f.h = h }
func (f *fakeBackend) Subscribe(topics []string) error        { return nil }
func (f *fakeBackend) Poll(ctx context.Context, maxWait time.Duration) ([]Message, error) {
    f.mu.Lock()
    defer f.mu.Unlock()
    if len(f.queue) == 0 {
        return nil, nil
    }
    batch := f.queue
    f.queue = nil
    return batch, nil
}
func (f *fakeBackend) Commit(ctx context.Context, commits map[TopicPartition]Offset) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    f.commits = append(f.commits, commits)
    return nil
}
func (f *fakeBackend) Pause(parts []TopicPartition) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    for _, p := range parts {
        f.paused[p] = true
    }
    return nil
}
func (f *fakeBackend) Resume(parts []TopicPartition) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    for _, p := range parts {
        delete(f.paused, p)
    }
    return nil
}
func (f *fakeBackend) Close(ctx context.Context) error { f.closed = true; return nil }

func newFakeBackend(msgs ...Message) *fakeBackend {
    return &fakeBackend{queue: msgs, paused: map[TopicPartition]bool{}}
}

// committed offsets must form the max contiguous point, even with skew.
func TestEngineCommitSequence(t *testing.T) {
    tp := TopicPartition{Topic: "t", Partition: 0}
    // key a is slow: offsets 0(a),1(b),2(a) -> b finishes first.
    msgs := []Message{
        {Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
        {Topic: "t", Partition: 0, Offset: 1, Key: []byte("b")},
        {Topic: "t", Partition: 0, Offset: 2, Key: []byte("a")},
    }
    be := newFakeBackend(msgs...)
    cfg := DefaultConfig()
    cfg.Lanes = 2
    cfg.CommitInterval = 5 * time.Millisecond
    handler := func(ctx context.Context, m *Message) error {
        if string(m.Key) == "a" {
            time.Sleep(30 * time.Millisecond)
        }
        return nil
    }
    c, err := New(be, cfg)
    if err != nil {
        t.Fatal(err)
    }
    if err := c.Subscribe([]string{"t"}, handler); err != nil {
        t.Fatal(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
    defer cancel()
    _ = c.Run(ctx)

    if len(be.commits) == 0 {
        t.Fatal("no commits recorded")
    }
    last := be.commits[len(be.commits)-1]
    if off, ok := last[tp]; !ok {
        t.Fatalf("partition %v missing from final commit %+v", tp, last)
    } else if off != 3 {
        t.Fatalf("final committed offset = %d, want 3 (contiguous after all done)", off)
    }
}

// OnRevoked drains and commits final offsets.
func TestEngineRebalanceRevoke(t *testing.T) {
    tp := TopicPartition{Topic: "t", Partition: 0}
    be := newFakeBackend(
        Message{Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
        Message{Topic: "t", Partition: 0, Offset: 1, Key: []byte("a")},
    )
    cfg := DefaultConfig()
    cfg.Lanes = 1
    handler := func(ctx context.Context, m *Message) error { return nil }
    c, err := New(be, cfg)
    if err != nil {
        t.Fatal(err)
    }
    _ = c.Subscribe([]string{"t"}, handler)

    ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
    defer cancel()
    go c.Run(ctx)

    // wait for assignment, then revoke
    deadline := time.Now().Add(200 * time.Millisecond)
    for time.Now().Before(deadline) {
        be.mu.Lock()
        assigned := be.h != nil
        be.mu.Unlock()
        if assigned {
            break
        }
        time.Sleep(5 * time.Millisecond)
    }
    be.mu.Lock()
    h := be.h
    be.mu.Unlock()
    if h == nil {
        t.Fatal("rebalance handler not registered")
    }
    if err := h.OnRevoked(context.Background(), []TopicPartition{tp}); err != nil {
        t.Fatal(err)
    }
    c.Stop()
    <-time.After(20 * time.Millisecond)

    be.mu.Lock()
    defer be.mu.Unlock()
    if len(be.commits) == 0 {
        t.Fatal("no commit on revoke")
    }
    last := be.commits[len(be.commits)-1]
    if off, ok := last[tp]; !ok || off != 2 {
        t.Fatalf("revoke commit = %+v, want offset 2 for %v", last, tp)
    }
}

// backpressure pauses a partition at its in-flight cap.
func TestEngineBackpressure(t *testing.T) {
    tp := TopicPartition{Topic: "t", Partition: 0}
    cfg := DefaultConfig()
    cfg.Lanes = 2
    cfg.MaxInFlight = 2
    cfg.QueueSize = 16
    started := make(chan struct{}, 1)
    release := make(chan struct{})
    var once sync.Once
    handler := func(ctx context.Context, m *Message) error {
        once.Do(func() { close(started) })
        <-release // block all handlers
        return nil
    }
    be := newFakeBackend(
        Message{Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
        Message{Topic: "t", Partition: 0, Offset: 1, Key: []byte("b")},
    )
    c, err := New(be, cfg)
    if err != nil {
        t.Fatal(err)
    }
    _ = c.Subscribe([]string{"t"}, handler)
    ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
    defer cancel()
    go c.Run(ctx)

    select {
    case <-started:
    case <-time.After(200 * time.Millisecond):
        t.Fatal("handler never started")
    }
    deadline := time.Now().Add(200 * time.Millisecond)
    paused := false
    for time.Now().Before(deadline) {
        be.mu.Lock()
        paused = be.paused[tp]
        be.mu.Unlock()
        if paused {
            break
        }
        time.Sleep(5 * time.Millisecond)
    }
    if !paused {
        t.Fatal("partition was not paused at in-flight cap")
    }
    close(release)
    c.Stop()
}

// handler fatal error stops Run with the error.
func TestEngineFatalStops(t *testing.T) {
    be := newFakeBackend(Message{Topic: "t", Partition: 0, Offset: 0})
    cfg := DefaultConfig()
    cfg.Lanes = 1
    handler := func(ctx context.Context, m *Message) error { return fmt.Errorf("boom") }
    c, err := New(be, cfg)
    if err != nil {
        t.Fatal(err)
    }
    _ = c.Subscribe([]string{"t"}, handler)
    ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
    defer cancel()
    err = c.Run(ctx)
    if err == nil {
        t.Fatal("Run should return handler error")
    }
}

func TestStats(t *testing.T) {
    be := newFakeBackend(Message{Topic: "t", Partition: 0, Offset: 0})
    cfg := DefaultConfig()
    cfg.Lanes = 1
    handler := func(ctx context.Context, m *Message) error { return nil }
    c, err := New(be, cfg)
    if err != nil {
        t.Fatal(err)
    }
    _ = c.Subscribe([]string{"t"}, handler)
    ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
    defer cancel()
    _ = c.Run(ctx)
    s := c.Stats()
    if s.Mode != KeyOrdered {
        t.Fatalf("stats mode = %v", s.Mode)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./...`
Expected: FAIL（`undefined: New`）

- [ ] **Step 3: Implement minimal code**

`stats.go`:

```go
package swimlane

// PartitionStat is a snapshot of one partition's worker.
type PartitionStat struct {
    InFlight    int64
    BaseOffset  Offset
    MaxInFlight int
}

// Stats is a point-in-time snapshot of the consumer.
type Stats struct {
    Mode         Mode
    Partitions   int
    InFlightTotal int64
    PerPartition map[TopicPartition]PartitionStat
}
```

`consumer.go`：

```go
package swimlane

import (
    "context"
    "errors"
    "fmt"
    "sync"
    "sync/atomic"
    "time"
)

type Consumer struct {
    backend Backend
    cfg     Config
    handler Handler

    mu        sync.Mutex
    workers   map[TopicPartition]*partitionWorker
    paused    map[TopicPartition]bool
    stopCh    chan struct{}
    stopOnce  sync.Once
    workCtx   context.Context
    workCancel context.CancelFunc

    inflightTotal atomic.Int64
    fatalCh       chan error
}

// New validates the config and builds a consumer around a backend.
func New(backend Backend, cfg Config) (*Consumer, error) {
    if backend == nil {
        return nil, fmt.Errorf("%w: nil backend", ErrInvalidConfig)
    }
    cfg, err := cfg.withDefaults()
    if err != nil {
        return nil, err
    }
    return &Consumer{
        backend: backend,
        cfg:     cfg,
        workers: make(map[TopicPartition]*partitionWorker),
        paused:  make(map[TopicPartition]bool),
        stopCh:  make(chan struct{}),
        fatalCh: make(chan error, 1),
    }, nil
}

// Subscribe registers topics and the handler. May be called repeatedly.
func (c *Consumer) Subscribe(topics []string, handler Handler) error {
    c.mu.Lock()
    if c.handler == nil {
        c.handler = handler
    }
    c.mu.Unlock()
    return c.backend.Subscribe(topics)
}

// Run blocks until ctx is done, Stop is called, or a fatal error occurs.
func (c *Consumer) Run(ctx context.Context) error {
    c.mu.Lock()
    if c.handler == nil {
        c.mu.Unlock()
        return fmt.Errorf("%w: Subscribe must be called before Run", ErrInvalidConfig)
    }
    c.workCtx, c.workCancel = context.WithCancel(ctx)
    c.mu.Unlock()

    c.backend.SetRebalanceHandler(c)

    var lastCommit time.Time
    first := true
    for {
        select {
        case <-ctx.Done():
            return c.shutdown(nil)
        case <-c.stopCh:
            return c.shutdown(nil)
        case err := <-c.fatalCh:
            return c.shutdown(err)
        default:
        }

        if c.cfg.CommitInterval <= 0 || first || time.Since(lastCommit) >= c.cfg.CommitInterval {
            c.commitAll(c.workCtx)
            lastCommit = time.Now()
            first = false
        }

        c.applyBackpressure(c.workCtx)

        msgs, err := c.backend.Poll(c.workCtx, c.cfg.PollTimeout)
        if err != nil {
            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                return c.shutdown(nil)
            }
            return c.shutdown(err)
        }
        for i := range msgs {
            c.route(&msgs[i])
        }
    }
}

// Stop triggers graceful shutdown from any goroutine.
func (c *Consumer) Stop() {
    c.stopOnce.Do(func() {
        close(c.stopCh)
        c.mu.Lock()
        if c.workCancel != nil {
            c.workCancel()
        }
        c.mu.Unlock()
    })
}

// Stats returns a snapshot of the consumer state.
func (c *Consumer) Stats() Stats {
    c.mu.Lock()
    defer c.mu.Unlock()
    s := Stats{
        Mode:          c.cfg.Mode,
        Partitions:    len(c.workers),
        InFlightTotal: c.inflightTotal.Load(),
        PerPartition:  make(map[TopicPartition]PartitionStat, len(c.workers)),
    }
    for tp, w := range c.workers {
        s.PerPartition[tp] = PartitionStat{
            InFlight:    w.inFlight(),
            BaseOffset:  w.baseOffset(),
            MaxInFlight: w.maxInFlight,
        }
    }
    return s
}

// RebalanceHandler implementation -----------------------------------------

func (c *Consumer) OnAssigned(ctx context.Context, assigned map[TopicPartition]Offset) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    for tp := range assigned {
        if _, ok := c.workers[tp]; ok {
            continue
        }
        w := newPartitionWorker(tp, c.cfg, c.fatalCh)
        w.setHandler(c.handler)
        w.start(c.workCtx)
        c.workers[tp] = w
    }
    return nil
}

func (c *Consumer) OnRevoked(ctx context.Context, revoked []TopicPartition) error {
    c.mu.Lock()
    victims := make([]*partitionWorker, 0, len(revoked))
    for _, tp := range revoked {
        if w, ok := c.workers[tp]; ok {
            victims = append(victims, w)
            delete(c.workers, tp)
        }
    }
    c.mu.Unlock()

    drainCtx, cancel := context.WithTimeout(ctx, c.cfg.RebalanceTimeout)
    defer cancel()
    commits := make(map[TopicPartition]Offset)
    for _, w := range victims {
        w.drain(drainCtx)
        if w.isSeeded() {
            commits[w.tp] = w.baseOffset()
        }
    }
    if len(commits) == 0 {
        return nil
    }
    return c.backend.Commit(drainCtx, commits)
}

// internals ------------------------------------------------------------------

func (c *Consumer) route(msg *Message) {
    c.mu.Lock()
    w, ok := c.workers[msg.TP()]
    c.mu.Unlock()
    if !ok {
        return // partition revoked between poll and route
    }
    c.inflightTotal.Add(1)
    if !w.route(c.workCtx, msg) {
        c.inflightTotal.Add(-1)
    }
}

func (c *Consumer) commitAll(ctx context.Context) {
    c.mu.Lock()
    commits := make(map[TopicPartition]Offset, len(c.workers))
    for tp, w := range c.workers {
        if w.isSeeded() {
            commits[tp] = w.baseOffset()
        }
    }
    c.mu.Unlock()
    if len(commits) == 0 {
        return
    }
    // v1: periodic commit errors are non-fatal, retried next tick
    _ = c.backend.Commit(ctx, commits)
}

func (c *Consumer) applyBackpressure(ctx context.Context) {
    c.mu.Lock()
    var pause, resume []TopicPartition
    for tp, w := range c.workers {
        n := w.inFlight()
        if n >= int64(w.maxInFlight) && !c.paused[tp] {
            pause = append(pause, tp)
            c.paused[tp] = true
        } else if n <= int64(float64(w.maxInFlight)*0.8) && c.paused[tp] {
            resume = append(resume, tp)
            delete(c.paused, tp)
        }
    }
    c.mu.Unlock()
    if len(pause) > 0 {
        _ = c.backend.Pause(pause)
    }
    if len(resume) > 0 {
        _ = c.backend.Resume(resume)
    }
}

func (c *Consumer) shutdown(finalErr error) error {
    c.mu.Lock()
    workers := make([]*partitionWorker, 0, len(c.workers))
    for _, w := range c.workers {
        workers = append(workers, w)
    }
    c.mu.Unlock()

    drainCtx, cancel := context.WithTimeout(context.Background(), c.cfg.RebalanceTimeout)
    defer cancel()
    commits := make(map[TopicPartition]Offset)
    for _, w := range workers {
        w.drain(drainCtx)
        if w.isSeeded() {
            commits[w.tp] = w.baseOffset()
        }
    }
    if len(commits) > 0 {
        if err := c.backend.Commit(drainCtx, commits); err != nil && finalErr == nil {
            finalErr = err
        }
    }
    _ = c.backend.Close(drainCtx)
    if finalErr != nil {
        return finalErr
    }
    return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./... -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add consumer.go stats.go consumer_test.go partition.go partition_test.go
git commit -m "feat: add consumer engine with backpressure, rebalance, and stats"
```

---

### Task 6: Kafka adapter（franz-go）

**Files:**
- Create: `backend/kafka/backend.go`
- Create: `backend/kafka/config.go`
- Test: `backend/kafka/backend_test.go`

**Interfaces:**
- Consumes: `swimlane` 根包（`Backend`, `RebalanceHandler`, `Message`, `Header`, `TopicPartition`, `Offset`, `ErrClosed`），franz-go
- Produces: `kafka.Config{Brokers, Group, SASL *sasl.Plain, TLS *tls.Config, ConsumeResetOffset kgo.Offset}`、`kafka.New(cfg Config) (*Backend, error)`（`*Backend` 实现 `swimlane.Backend`）、`toMessage(topic string, partition int32, r *kgo.Record) swimlane.Message`

- [ ] **Step 1: Write the failing test**

`backend/kafka/backend_test.go`（纯逻辑，不连 broker）：

```go
package kafka

import (
    "testing"

    "github.com/twmb/franz-go/pkg/kgo"
    "mq-parallel-consumer"
)

func TestToMessage(t *testing.T) {
    r := &kgo.Record{
        Topic: "t",
        Offset: 7,
        Key: []byte("k"),
        Value: []byte("v"),
        Headers: []kgo.RecordHeader{{Key: "h1", Value: []byte("x")}},
    }
    m := toMessage("t", 3, r)
    if m.Partition != 3 || m.Offset != 7 || string(m.Key) != "k" || string(m.Value) != "v" {
        t.Fatalf("bad message: %+v", m)
    }
    if len(m.Headers) != 1 || m.Headers[0].Key != "h1" || string(m.Headers[0].Value) != "x" {
        t.Fatalf("bad headers: %+v", m.Headers)
    }
}

func TestOffsetMapping(t *testing.T) {
    commits := map[swimlane.TopicPartition]swimlane.Offset{
        {Topic: "t", Partition: 0}: 5,
        {Topic: "t", Partition: 1}: 9,
    }
    // toFranzOffsets 是适配层的私有辅助（见实现），这里直接验证映射
    o := toFranzOffsets(commits)
    if o.Lookup("t", 0) != 5 || o.Lookup("t", 1) != 9 {
        t.Fatalf("bad offsets: %+v", o)
    }
}

func TestToFranzPartitions(t *testing.T) {
    m := toFranzPartitions([]swimlane.TopicPartition{
        {Topic: "t", Partition: 0},
        {Topic: "t", Partition: 2},
        {Topic: "u", Partition: 1},
    })
    if len(m["t"]) != 2 || len(m["u"]) != 1 {
        t.Fatalf("bad partition map: %+v", m)
    }
}

```

（`toFranzOffsets` 返回 `kgo.Offsets`，其 `Lookup(topic, partition) int64` 是 franz-go 方法。）

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./backend/kafka/...`
Expected: FAIL（包不存在 / `undefined: toMessage`）

- [ ] **Step 3: Add franz-go dependency and implement**

```bash
cd /Users/mac/project/mq-parallel-consumer
go get github.com/twmb/franz-go@latest
```

`backend/kafka/config.go`:

```go
package kafka

import (
    "crypto/tls"

    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/twmb/franz-go/pkg/sasl/plain"
)

// Config holds kafka-specific transport settings.
type Config struct {
    Brokers            []string
    Group              string
    SASL               *plain.Auth
    TLS                *tls.Config
    ConsumeResetOffset kgo.Offset
}
```

`backend/kafka/backend.go`:

```go
package kafka

import (
    "context"
    "sync"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"

    "mq-parallel-consumer"
)

// Backend implements swimlane.Backend on top of franz-go.
type Backend struct {
    cli *kgo.Client
    mu  sync.Mutex
    h   swimlane.RebalanceHandler
}

// New builds the franz-go client. Does not connect until Poll is called.
func New(cfg Config) (*Backend, error) {
    b := &Backend{}
    opts := []kgo.Opt{
        kgo.SeedBrokers(cfg.Brokers...),
        kgo.DisableAutoCommit(), // SDK owns commit entirely
    }
    if cfg.Group != "" {
        opts = append(opts, kgo.ConsumerGroup(cfg.Group))
    }
    if cfg.ConsumeResetOffset != nil {
        opts = append(opts, kgo.ConsumeResetOffset(cfg.ConsumeResetOffset))
    } else {
        opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
    }
    if cfg.SASL != nil {
        opts = append(opts, kgo.SASL(plain.Auth(*cfg.SASL).AsMechanism()))
    }
    if cfg.TLS != nil {
        opts = append(opts, kgo.DialTLSConfig(cfg.TLS))
    }
    opts = append(opts,
        kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
            b.mu.Lock()
            h := b.h
            b.mu.Unlock()
            if h == nil {
                return
            }
            m := make(map[swimlane.TopicPartition]swimlane.Offset)
            for topic, parts := range assigned {
                for _, p := range parts {
                    m[swimlane.TopicPartition{Topic: topic, Partition: p}] = 0
                }
            }
            _ = h.OnAssigned(ctx, m)
        }),
        kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
            b.mu.Lock()
            h := b.h
            b.mu.Unlock()
            if h == nil {
                return
            }
            parts := make([]swimlane.TopicPartition, 0)
            for topic, ps := range revoked {
                for _, p := range ps {
                    parts = append(parts, swimlane.TopicPartition{Topic: topic, Partition: p})
                }
            }
            _ = h.OnRevoked(ctx, parts)
        }),
    )
    cli, err := kgo.NewClient(opts...)
    if err != nil {
        return nil, err
    }
    b.cli = cli
    return b, nil
}

func (b *Backend) SetRebalanceHandler(h swimlane.RebalanceHandler) {
    b.mu.Lock()
    b.h = h
    b.mu.Unlock()
}

func (b *Backend) Subscribe(topics []string) error {
    b.cli.AddConsumeTopics(topics...)
    return nil
}

func (b *Backend) Poll(ctx context.Context, maxWait time.Duration) ([]swimlane.Message, error) {
    ctx, cancel := context.WithTimeout(ctx, maxWait)
    defer cancel()
    fetches := b.cli.PollFetches(ctx)
    if fetches.IsClientClosed() {
        return nil, swimlane.ErrClosed
    }
    var msgs []swimlane.Message
    fetches.EachTopic(func(ft kgo.FetchTopic) {
        for _, fp := range ft.Partitions {
            for _, r := range fp.Records {
                msgs = append(msgs, toMessage(ft.Topic, fp.Partition, r))
            }
        }
    })
    if errs := fetches.Errors(); len(errs) > 0 {
        return msgs, errs[0]
    }
    return msgs, nil
}

func (b *Backend) Commit(ctx context.Context, commits map[swimlane.TopicPartition]swimlane.Offset) error {
    return b.cli.CommitOffsets(ctx, toFranzOffsets(commits))
}

func (b *Backend) Pause(parts []swimlane.TopicPartition) error {
    b.cli.PauseFetchTopics(toFranzPartitions(parts))
    return nil
}

func (b *Backend) Resume(parts []swimlane.TopicPartition) error {
    b.cli.ResumeFetchTopics(toFranzPartitions(parts))
    return nil
}

func (b *Backend) Close(ctx context.Context) error {
    b.cli.Close()
    return nil
}

func toFranzPartitions(parts []swimlane.TopicPartition) map[string][]int32 {
    m := make(map[string][]int32)
    for _, p := range parts {
        m[p.Topic] = append(m[p.Topic], p.Partition)
    }
    return m
}

func toFranzOffsets(commits map[swimlane.TopicPartition]swimlane.Offset) kgo.Offsets {
    o := make(kgo.Offsets)
    for tp, off := range commits {
        o.Add(tp.Topic, tp.Partition, kgo.NewOffset().At(int64(off)))
    }
    return o
}

func toMessage(topic string, partition int32, r *kgo.Record) swimlane.Message {
    hdrs := make([]swimlane.Header, 0, len(r.Headers))
    for _, h := range r.Headers {
        hdrs = append(hdrs, swimlane.Header{Key: h.Key, Value: h.Value})
    }
    return swimlane.Message{
        Topic:     topic,
        Partition: partition,
        Offset:    swimlane.Offset(r.Offset),
        Key:       r.Key,
        Value:     r.Value,
        Headers:   hdrs,
        Timestamp: r.Timestamp,
    }
}
```

> 注：`plain.Auth.AsMechanism()` 依赖 franz-go 的 SASL plain 包；若 API 签名与版本有出入，以 `go doc github.com/twmb/franz-go/pkg/sasl/plain` 为准调整。`OnAssigned` 传 0 作为起始 offset，core 会从第一条消息惰性 seed，0 仅作占位。

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./... -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/kafka/ go.mod go.sum
git commit -m "feat: add franz-go kafka backend adapter"
```

---

### Task 7: 示例与 README

**Files:**
- Create: `examples/main.go`
- Create: `README.md`
- Create: `go.mod`（示例若独立目录则独立 module，否则放根模块 `example` 包）

**Interfaces:**
- Consumes: 完整 SDK API（`New`, `Subscribe`, `Run`, `Stop`, `DefaultConfig`, `kafka.New`）

- [ ] **Step 1: Write the example**

`examples/main.go`（放根模块内，包名 `main` 在子目录编译为单独二进制）：

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "mq-parallel-consumer"
    "mq-parallel-consumer/backend/kafka"
)

func main() {
    cfg := swimlane.DefaultConfig()
    cfg.Lanes = 8
    cfg.Retry.MaxAttempts = 3
    cfg.Retry.InitialBackoff = 50 * 1000 * 1000 // 50ms (time.Duration 纳秒)
    cfg.OnDiscard = func(ctx context.Context, msg *swimlane.Message, err error) {
        log.Printf("discard offset=%d key=%s err=%v", msg.Offset, msg.Key, err)
    }

    be, err := kafka.New(kafka.Config{
        Brokers: []string{"localhost:9092"},
        Group:   "swimlane-demo",
    })
    if err != nil {
        log.Fatal(err)
    }

    c, err := swimlane.New(be, cfg)
    if err != nil {
        log.Fatal(err)
    }
    if err := c.Subscribe([]string{"demo-topic"}, func(ctx context.Context, msg *swimlane.Message) error {
        fmt.Printf("topic=%s partition=%d offset=%d key=%s value=%s\n",
            msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
        return nil
    }); err != nil {
        log.Fatal(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    if err := c.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

（`time.Duration` 使用更清晰的写法：`cfg.Retry.InitialBackoff = 50 * time.Millisecond`，记得 import `time`。）

- [ ] **Step 2: Verify it compiles**

Run: `cd /Users/mac/project/mq-parallel-consumer && go build ./... && go vet ./...`
Expected: success

- [ ] **Step 3: Write README**

`README.md`：项目简介（MQ 无关的泳道并发消费 SDK，KeyOrdered/Unordered 两种模式、最大连续 offset 提交）、快速开始（上例）、Config 一览表、Backend 适配说明（如何接入 RocketMQ）。

- [ ] **Step 4: Run full test suite**

Run: `cd /Users/mac/project/mq-parallel-consumer && go test ./... -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add examples/main.go README.md
git commit -m "docs: add usage example and README"
```

---

## Self-Review

**Spec 覆盖核对：**
- Backend SPI（§3）→ Task 1（接口）+ Task 6（实现）✓
- Mode/Config 默认值/零值语义（§5）→ Task 1 ✓
- OffsetTracker 连续提交（§4.3）→ Task 2 ✓
- 重试/OnDiscard/致命语义（§4.5）→ Task 3（退避）+ Task 4（状态机）✓
- 泳道路由/Unordered/背压/rebalance 时序（§4.2/4.4/4.6）→ Task 4（worker）+ Task 5（引擎编排）✓
- 线程安全 API 与 Stats（§8）→ Task 5 ✓
- kafka adapter（§6）→ Task 6 ✓
- 错误模型（§7）→ Task 1（哨兵）+ Task 4/5（包装与传播）✓

**占位符扫描：** 无 TBD/TODO。Task 6 的 `plain.Auth.AsMechanism()` 标注了按实际版本调整（属于外部 API 适配说明，非占位）。

**类型一致性：** `newOffsetTracker()` 在 Task 2 定义、Task 4 使用；`partitionWorker.setHandler` 在 Task 4 Step 3 定义、Task 4 测试与 Task 5 `OnAssigned` 使用；`Backend.Subscribe` 在 Task 1 定义、Task 5/6 使用。签名跨任务一致。
