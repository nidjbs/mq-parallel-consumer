// Baseline reference: raw franz-go sequential consumption of a single
// partition, WITHOUT the swimlane SDK. Same workload as the SDK e2e
// (nMsgs, same handler sleep) so the SDK's Lanes=1 has a ground truth to
// compare against and the concurrency gain is attributable to the lanes.
//
//	docker compose -f examples/kafka-e2e/docker-compose.yaml up -d
//	go run ./examples/baseline [-msgs 20000 -io 1 -keys 2000]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const brokers = "localhost:9092"

var (
	msgs = flag.Int("msgs", 20000, "number of messages to produce")
	ioMS = flag.Int("io", 1, "handler IO sleep in ms")
	keys = flag.Int("keys", 0, "distinct keys; 0 = derive (max(10, msgs/10))")
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

func createTopic(ctx context.Context, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := &kmsg.CreateTopicsRequest{
		TimeoutMillis: 15000,
		Topics: []kmsg.CreateTopicsRequestTopic{
			{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
		},
	}
	resp, err := cl.Request(ctx, req)
	if err != nil {
		return err
	}
	for _, t := range resp.(*kmsg.CreateTopicsResponse).Topics {
		if t.ErrorCode != 0 {
			return fmt.Errorf("create topic %s: %w", t.Topic, kerr.ErrorForCode(t.ErrorCode))
		}
	}
	return nil
}

func produce(ctx context.Context, topic string, nMsgs, nKeys int) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic(topic))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < nMsgs; i++ {
		key := fmt.Sprintf("user-%d", i%nKeys)
		cl.Produce(ctx, &kgo.Record{Key: []byte(key), Value: []byte(fmt.Sprintf("msg-%d", i))},
			func(_ *kgo.Record, err error) {
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
		nKeys = max(10, nMsgs/10)
	}
	ioSleep := time.Duration(*ioMS) * time.Millisecond

	ctx := context.Background()
	if err := waitForKafka(ctx); err != nil {
		log.Fatal(err)
	}
	topic := fmt.Sprintf("baseline-%d", time.Now().UnixNano())
	if err := createTopic(ctx, topic); err != nil {
		log.Fatal(err)
	}
	produce(ctx, topic, nMsgs, nKeys)

	// raw single-goroutine sequential consumption via assign mode
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	var (
		mu    sync.Mutex
		count int64
		got   = map[string][]int64{}
	)
	start := time.Now()
	for count < int64(nMsgs) {
		fetches := cl.PollRecords(ctx, 500) // same per-poll cap as the SDK adapter
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			for _, fp := range ft.Partitions {
				for _, r := range fp.Records {
					time.Sleep(ioSleep) // simulate IO, same as SDK e2e
					mu.Lock()
					got[string(r.Key)] = append(got[string(r.Key)], r.Offset)
					count++
					mu.Unlock()
				}
			}
		})
	}
	elapsed := time.Since(start)

	ordered := true
	for _, offs := range got {
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				ordered = false
			}
		}
	}
	throughput := float64(nMsgs) / elapsed.Seconds()
	fmt.Printf("baseline consumed=%d keys=%d per-key-ordered=%v elapsed=%s throughput=%.0f msg/s\n",
		count, len(got), ordered, elapsed.Round(time.Millisecond), throughput)
	if count != int64(nMsgs) || !ordered {
		os.Exit(1)
	}
}
