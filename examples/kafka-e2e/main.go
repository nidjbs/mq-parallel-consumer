// End-to-end test against a real Kafka broker (single partition).
//
//	docker compose -f examples/kafka-e2e/docker-compose.yaml up -d
//	go run ./examples/kafka-e2e [-msgs 20000 -lanes 8 -io 1 -keys 2000]
//
// Produces nMsgs to a fresh topic, consumes via the swimlane SDK, then
// asserts: all messages consumed and per-key offset order preserved.
// Flags allow large-message concurrency-scaling runs on a real broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"mq-parallel-consumer"
	"mq-parallel-consumer/backend/kafka"
)

const brokers = "localhost:9092"

var (
	msgs   = flag.Int("msgs", 500, "number of messages to produce")
	lanes  = flag.Int("lanes", 8, "number of lanes")
	ioMS   = flag.Int("io", 1, "handler IO sleep in ms (IO-bound simulation)")
	keys   = flag.Int("keys", 0, "distinct keys; 0 = derive (max(10, msgs/10))")
	rounds = flag.Int("rounds", 1, "repetitions; prints per-round and median")
)

func waitForKafka(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer cl.Close()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := cl.Ping(ctx); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("kafka not ready: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// createTopic creates a single-partition topic before producing, avoiding
// the auto-create metadata race on the first produce. Retries a few times
// to ride out transient controller timeouts on a fresh broker.
func createTopic(ctx context.Context, topic string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
		if err == nil {
			req := &kmsg.CreateTopicsRequest{
				TimeoutMillis: 15000, // explicit; broker default is too short
				Topics: []kmsg.CreateTopicsRequestTopic{
					{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
				},
			}
			var resp kmsg.Response
			resp, err = cl.Request(ctx, req)
			if err == nil {
				for _, t := range resp.(*kmsg.CreateTopicsResponse).Topics {
					if t.ErrorCode != 0 {
						err = fmt.Errorf("create topic %s: %w", t.Topic, kerr.ErrorForCode(t.ErrorCode))
						break
					}
				}
			}
			cl.Close()
			if err == nil {
				return nil
			}
			lastErr = err
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return lastErr
}

func produce(ctx context.Context, topic string, nMsgs, nKeys int) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic(topic))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < nMsgs; i++ {
		key := fmt.Sprintf("user-%d", i%nKeys)
		cl.Produce(ctx, &kgo.Record{
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("msg-%d", i)),
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Fatalf("produce failed: %v", err)
			}
		})
	}
	cl.Flush(ctx)
	log.Printf("produced %d messages", nMsgs)
}

func main() {
	flag.Parse()
	nMsgs := *msgs
	nKeys := *keys
	if nKeys <= 0 {
		nKeys = max(10, nMsgs/10) // enough keys to spread across lanes
	}
	ioSleep := time.Duration(*ioMS) * time.Millisecond

	ctx := context.Background()
	if err := waitForKafka(ctx); err != nil {
		log.Fatal(err)
	}

	throughputs := make([]float64, 0, *rounds)
	for r := 1; r <= *rounds; r++ {
		elapsed, nKeysGot, ordered, consumed := runOnce(ctx, nMsgs, nKeys, ioSleep)
		throughput := float64(nMsgs) / elapsed.Seconds()
		throughputs = append(throughputs, throughput)
		fmt.Printf("round=%d lanes=%d consumed=%d keys=%d per-key-ordered=%v elapsed=%s throughput=%.0f msg/s\n",
			r, *lanes, consumed, nKeysGot, ordered, elapsed.Round(time.Millisecond), throughput)
		if consumed != int64(nMsgs) || !ordered {
			os.Exit(1)
		}
	}
	if *rounds > 1 {
		sort.Float64s(throughputs)
		med := throughputs[len(throughputs)/2]
		fmt.Printf("lanes=%d median throughput=%.0f msg/s (%d rounds)\n", *lanes, med, *rounds)
	}
}

// runOnce produces and consumes nMsgs on a fresh single-partition topic and
// returns the consume elapsed time, distinct keys, ordering result, and count.
func runOnce(ctx context.Context, nMsgs, nKeys int, ioSleep time.Duration) (time.Duration, int, bool, int64) {
	// unique topic + group per run: a fresh consumer group avoids waiting for a
	// previous run's member session to expire before the rebalance completes.
	topic := fmt.Sprintf("swimlane-e2e-%d", time.Now().UnixNano())
	if err := createTopic(ctx, topic); err != nil {
		log.Fatal(err)
	}
	produce(ctx, topic, nMsgs, nKeys)

	var (
		mu    sync.Mutex
		got   = map[string][]swimlane.Offset{}
		count atomic.Int64
	)
	handler := func(_ context.Context, msg *swimlane.Message) error {
		time.Sleep(ioSleep) // simulate IO (DB/RPC)
		mu.Lock()
		got[string(msg.Key)] = append(got[string(msg.Key)], msg.Offset)
		count.Add(1)
		mu.Unlock()
		return nil
	}

	be, err := kafka.New(kafka.Config{Brokers: []string{brokers}, Group: topic})
	if err != nil {
		log.Fatal(err)
	}
	cfg := swimlane.DefaultConfig()
	cfg.Lanes = *lanes
	cfg.PollTimeout = 500 * time.Millisecond
	cfg.RebalanceTimeout = 5 * time.Second
	c, err := swimlane.New(be, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Subscribe([]string{topic}, handler); err != nil {
		log.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	start := time.Now()
	go func() { runErr <- c.Run(runCtx) }()

	deadline := time.Now().Add(120 * time.Second)
	for count.Load() != int64(nMsgs) {
		select {
		case err := <-runErr:
			log.Fatalf("Run returned early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			log.Fatalf("timeout: consumed %d/%d", count.Load(), nMsgs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	c.Stop()

	mu.Lock()
	ordered := true
	for key, offs := range got {
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				ordered = false
				log.Printf("ORDER VIOLATION key=%s offsets=%v", key, offs)
			}
		}
	}
	nKeysGot := len(got)
	mu.Unlock()
	return elapsed, nKeysGot, ordered, count.Load()
}
