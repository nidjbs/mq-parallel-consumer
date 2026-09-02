# mq-parallel-consumer

An MQ-agnostic swim-lane concurrent consumer SDK. Within a single partition: **same key serial, different keys concurrent**; offsets committed at the **max contiguous completed** position. The core has zero external dependencies and adapts to MQs via the `Backend` SPI, with Kafka (franz-go) built in.

## Features

- **Two ordering modes**: `KeyOrdered` / `Unordered`
- **Contiguous offset commit**: only the contiguous run is committed under out-of-order completion; no loss on crash recovery
- **Commits off the poll loop**: a dedicated committer runs on its own schedule, so a slow broker commit cannot stall polling
- **Backpressure**: bounded per-key queues backpressure the poll loop directly (no broker pause needed at default settings); explicit pause/resume is available when `MaxInFlight` is tuned
- **Failure handling**: no in-process retry by default — a failed message goes to `OnDiscard` (DLQ/retry-topic hand-off) or fatal. Optional `Retry` cools a message down off its lane so retries never stall unrelated keys. Handler / `OnDiscard` panics are recovered into errors instead of crashing the process
- **Graceful rebalance**: drains in-flight messages and commits final offsets on revoke; a failed final commit is surfaced as a fatal error by `Run()`
- **Manual pause/resume**: `Pause()`/`Resume()` as a circuit breaker, shielded from automatic backpressure resume
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
| `MaxInFlight` | concurrency × `QueueSize` | In-flight ceiling. At the default (= bounded lane / semaphore capacity) the bounded queues provide backpressure directly; lower pauses the partition earlier, higher buffers more before pausing |
| `QueueSize` | 16 | Per-lane bound on total buffered messages (all keys of a lane share it) |
| `CommitInterval` | 100ms | Commit window; `0` = commit immediately when the contiguous base advances |
| `PollTimeout` | 100ms | Max block per poll |
| `RebalanceTimeout` | 3s | Rebalance drain timeout |
| `Retry` | zero = no retry | Optional in-process retry (see below); recommended default is to leave it off |
| `OnDiscard` | nil = fatal | Called when a message fails (after retries if any) |
| `KeyExtractor` | nil = use msg.Key | Custom routing key |

## Failure Handling

```
handler returns error
  ├─ Retry.MaxAttempts > 0 → cool the message down and retry later (its key
  │     stays serial, but other keys on the lane keep running)
  ├─ failed / exhausted && OnDiscard != nil → call OnDiscard, advance offset
  └─ failed / exhausted && OnDiscard == nil → fatal: offset not committed,
        Run() returns error
```

A panic in the handler is treated like a handler error (retry → fatal); a panic
in `OnDiscard` is fatal and leaves the offset uncommitted.

### On retries

The default (`Retry` unset) is **no in-process retry**: a message is attempted
once and, on failure, goes straight to `OnDiscard` or fatal. When you enable
`Retry.MaxAttempts`, a failed message is taken off its lane and cooled down for
`InitialBackoff` (exponential, jittered), so its retries never stall unrelated
keys of the same lane; per-key order is preserved either way.

**Recommended production pattern:** leave `Retry` off and use `OnDiscard` to
hand the failed message to an MQ-level retry mechanism (DLQ / retry topic),
which replays it independently of the main consumption path. Keep in mind a
message that is left in the contiguous window (failed but not discarded) blocks
offset commits for everything after it — `OnDiscard` closing the hole is what
lets the partition move forward.

> **Warning:** `OnDiscard` returning without moving the message anywhere means
> the message is **dropped** — its offset has already advanced. The callback
> must hand the message to a retry topic / DLQ (or otherwise guarantee it is
> re-queued), otherwise permanent failures silently disappear.

### Handling failures in production (DLQ / retry topic)

A minimal, runnable reference is `examples/dlq-retry` (single-partition topics,
requires Docker Kafka):

```
docker compose -f examples/kafka-e2e/docker-compose.yaml up -d
go run ./examples/dlq-retry [-total 1000 -io-max 5]
```

It wires a realistic chain and asserts every message lands somewhere:

```
producer ──► "pc-demo" ──► main consumer (this SDK)
  ok:        handled once
  transient: 1st attempt fails → recovered by in-process Retry.MaxAttempts=2
  flaky:     fails at round 0 → OnDiscard writes "pc-demo-retry"
  poison:    always fails
OnDiscard ──► "pc-demo-retry" ──► retry worker (plain franz-go consumer)
  round < maxRound  → re-produce back to "pc-demo" (round+1) → handled again
  round >= maxRound → "pc-demo-dlq" (dead letter, retries exhausted)
```

Why this layering instead of "retry forever in the handler"? A failed message
that stays inside the contiguous window blocks offset commits for every message
after it. So:

1. **In-process `Retry`** absorbs transient downstream hiccups at the lowest
   latency, bounded by `MaxAttempts`.
2. **Retry topic + re-produce** retries anything that survived step 1, with a
   real round counter carried in a header — retries survive process restarts
   because they are ordinary messages again.
3. **DLQ** is the terminal stop after `maxRound`, so a poison message can never
   block the partition or spin forever.

The demo simulates a shaky downstream: the handler sleeps a random 0–N ms per
message and a fraction fail once / persistently. Typical output:

```
handled=1425/1425 dlq=75/75 per-key-ordered=true elapsed=1.188s throughput=1263 msg/s
OK: transient failures recovered in-process, flaky via retry topic, poison landed in DLQ
```

## Manual Pause / Resume

`Pause(...)` / `Resume(...)` (empty argument = all assigned partitions) act as a
manual circuit breaker: they stop/resume polling without unsubscribing, and a
manually paused partition is shielded from automatic backpressure resume until
you call `Resume`. Useful when a downstream system is down.

```go
c.Pause()            // stop consuming everything (downstream outage)
// ... recovery ...
c.Resume()           // resume consumption
```

## Observability

`Stats()` returns a point-in-time snapshot plus cumulative counters:

| Field | Meaning |
|---|---|
| `InFlightTotal`, `PerPartition` | in-flight messages now (global + per partition) |
| `PerPartition.HighestSeen`, `BaseOffset` | track the gap: `HighestSeen - BaseOffset` is the number of seen-but-uncommitted messages in the contiguous window — the key "is consumption stuck?" signal |
| `MessagesProcessed` | handler completions |
| `MessagesDiscarded` | messages skipped via `OnDiscard` |
| `HandlerErrors` | fatal handler errors surfaced by `Run()` |
| `Commits`, `CommitErrors` | offset commit attempts / failures |

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
go run ./examples/dlq-retry [-total 1000 -io-max 5]    # failure handling: DLQ/retry-topic demo with assertions
go run ./examples/baseline [-msgs 20000 -io 1 -keys 2000]   # raw franz-go sequential baseline (no SDK)
go run ./bench                                         # in-memory concurrency benchmark
```
