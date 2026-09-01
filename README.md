# mq-parallel-consumer

An MQ-agnostic swim-lane concurrent consumer SDK. Within a single partition: **same key serial, different keys concurrent**; offsets committed at the **max contiguous completed** position. The core has zero external dependencies and adapts to MQs via the `Backend` SPI, with Kafka (franz-go) built in.

## Features

- **Two ordering modes**: `KeyOrdered` / `Unordered`
- **Contiguous offset commit**: only the contiguous run is committed under out-of-order completion; no loss on crash recovery
- **Backpressure**: per-partition in-flight soft cap pause/resume + bounded queues
- **Failure handling**: built-in exponential + jitter backoff retry + `OnDiscard` callback; defaults to fatal on exhaustion; handler / `OnDiscard` panics are recovered into errors instead of crashing the process
- **Graceful rebalance**: drains in-flight messages and commits final offsets on revoke; a failed final commit is surfaced as a fatal error by `Run()`
- **Thread-safe**: `New`/`Subscribe`/`Run`/`Stop`/`Stats`; a `Consumer` is single-use (`Run` may be called at most once)

## How It Works

One poll loop fetches messages and routes them by key hash to a fixed set of lanes. The same key always lands on the same lane (serial); different keys run concurrently across lanes. Completed offsets advance a contiguous pointer.

```
                    ┌────────────────────────────────────┐
                    │        poll loop (single goroutine) │
                    │   Backend.Poll() fetches a batch     │
                    │   key hash → route to lane           │
                    │   periodically commit max contiguous │
                    └─────────────────┬──────────────────┘
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        ▼                             ▼                             ▼
  ┌────────────┐                ┌────────────┐              ┌────────────┐
  │  lane 0     │                │  lane 1     │    ···      │  lane N-1   │
  │ q→worker0  │                │ q→worker1  │              │ q→workerN  │
  │ same key   │                │ same key   │              │ same key   │
  │  serial    │                │  serial    │              │  serial    │
  └─────┬──────┘                └─────┬──────┘              └─────┬──────┘
        │                             │                           │
        └───────────────┬─────────────┴─────────────┬─────────────┘
                        ▼                           ▼
             ┌───────────────────────────────────────────────┐
             │       offsetTracker (one per partition)        │
             │  done → complete(offset), scan forward pointer  │
             │  base = max contiguous completed + 1            │
             └───────────────────┬───────────────────────────┘
                                 ▼
                 Commit(base): value = "next offset to consume"
                 crash recovery resumes at base, zero loss
```

## Performance

### In-memory engine (`go run ./bench`)

IO-bound handler (1ms block) + 2000 messages, measured via `go run ./bench`:

```
config                elapsed        msg/s  speedup
sequential             2.33s          858     1.0x
swimlane L=1           2.32s          865     1.0x
swimlane L=4           570ms         3508     4.1x
swimlane L=8           286ms         7004     8.2x
swimlane L=16          150ms        13311    15.6x
swimlane L=32           77ms        26001    30.5x
```

### Real Kafka (single partition, `examples/kafka-e2e`)

20k messages, 1ms IO handler, 2000 keys, median of 3 rounds, plus a raw
franz-go sequential baseline (`examples/baseline`, no SDK):

```
config              msg/s   speedup (vs raw baseline)
raw sequential       772     1.0x
swimlane L=1         738     1.0x
swimlane L=4        3109     4.0x
swimlane L=8        6154     8.0x
swimlane L=16      12171    15.8x
```

`Lanes=1` matches the raw sequential baseline (no overhead from the swim
lanes), and throughput scales near-linearly with `Lanes`. Requires evenly
distributed keys + an IO-bound handler; real-world speedups plateau when the
single-partition broker fetch becomes the bottleneck.

## Quick Start

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

## Configuration

| Field | Default | Description |
|---|---|---|
| `Mode` | `KeyOrdered` | Ordering mode |
| `Lanes` | 8 | KeyOrdered lanes (concurrency per partition) |
| `Concurrency` | 8 | Unordered concurrency (hard in-flight cap in this mode) |
| `MaxInFlight` | concurrency × `QueueSize` | In-flight soft cap triggering pause; hard memory bounds are the bounded lane queues (KeyOrdered) and the `Concurrency` semaphore (Unordered) |
| `QueueSize` | 16 | Per-lane queue depth |
| `CommitInterval` | 100ms | Commit window; `0` = commit immediately when the contiguous base advances |
| `PollTimeout` | 100ms | Max block per poll |
| `RebalanceTimeout` | 3s | Rebalance drain timeout |
| `Retry` | zero = no retry | Backoff retry policy |
| `OnDiscard` | nil = fatal | Called after retries exhausted |
| `KeyExtractor` | nil = use msg.Key | Custom routing key |

## Failure Handling

```
handler returns error
  ├─ Retry.MaxAttempts > 0 → retry with backoff in-lane
  ├─ exhausted && OnDiscard != nil → call OnDiscard, skip, advance offset
  └─ exhausted && OnDiscard == nil → fatal: offset not committed, Run() returns error
```

A panic in the handler is treated like a handler error (retry → fatal); a panic
in `OnDiscard` is fatal and leaves the offset uncommitted.

## Kafka Adapter

`backend/kafka.Config` adds transport-side memory bounds on top of the engine
knobs:

| Field | Default | Description |
|---|---|---|
| `MaxPollRecords` | 500 | Caps records returned per `Poll`; bounds single-batch in-flight memory |
| `FetchMaxBytes` | franz-go default | Caps bytes fetched per broker round trip |

## Supporting Other MQs

Implement the `Backend` SPI (`backend.go`): `Poll`/`Commit`/`Pause`/`Resume`/`Subscribe`/`SetRebalanceHandler`. Add a `backend/<mq>/` directory; the core stays untouched.

## Testing

```bash
go test ./... -race                                      # engine tests (in-memory backend, no MQ)
docker compose -f examples/kafka-e2e/docker-compose.yaml up -d
go run ./examples/kafka-e2e [-msgs 20000 -lanes 8 -io 1 -keys 2000 -rounds 3]  # real Kafka e2e + scale runs
go run ./examples/baseline [-msgs 20000 -io 1 -keys 2000]   # raw franz-go sequential baseline (no SDK)
go run ./bench                                         # in-memory concurrency benchmark
```
