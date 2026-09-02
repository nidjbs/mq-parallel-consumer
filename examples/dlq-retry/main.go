// Production-style failure handling demo on a real broker.
//
//	./dlq-retry [-total 600]
//
// Models a realistic consumer workload and shows how the SDK composes with an
// MQ-level retry mechanism:
//
//	producer ──► topic "pc-demo" ──► main consumer (swimlane SDK)
//	                                    ├─ ok:        handled once
//	                                    ├─ transient: 1st attempt fails, in-process
//	                                    │             Retry recovers it
//	                                    ├─ flaky:     fails every attempt at round 0,
//	                                    │             OnDiscard -> retry topic
//	                                    └─ poison:    always fails
//	OnDiscard ──► topic "pc-demo-retry" ──► retry worker
//	              round < maxRound ──► re-produce back to "pc-demo" (round+1)
//	              round >= maxRound ──► topic "pc-demo-dlq"
//
// Handler time includes random jitter to emulate a shaky downstream IO. The
// run asserts: ok/transient/flaky all end handled exactly once, poison all end
// in the DLQ (never stuck retrying), and per-key order holds.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
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

const (
	roundHeader = "pc-round"
	maxRound    = 4 // retry rounds before a message goes to the DLQ
)

var (
	total  = flag.Int("total", 600, "total messages")
	ioMin  = flag.Int("io-min", 0, "handler IO jitter lower bound (ms)")
	ioMax  = flag.Int("io-max", 3, "handler IO jitter upper bound (ms)")
	poison = flag.Int("poison", 5, "poison messages (always fail, end in DLQ) [%]")
	flaky  = flag.Int("flaky", 10, "flaky messages (need one external retry) [%]")
	trans  = flag.Int("transient", 20, "transient messages (recovered by in-process Retry) [%]")
)

// counters shared across goroutines.
var (
	successCount atomic.Int64 // ok + transient + flaky handled successfully
	dlqCount     atomic.Int64
	mainStopped  atomic.Bool // main consumer shut down; retry worker may exit

	successMu sync.Mutex
	perKey    = map[string][]int64{} // ok messages key -> offsets, for order check
)

func jitter() time.Duration {
	if *ioMax <= *ioMin {
		return time.Duration(*ioMax) * time.Millisecond
	}
	return time.Duration(*ioMin+rand.Intn(*ioMax-*ioMin+1)) * time.Millisecond
}

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

func createTopics(ctx context.Context, names ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := &kmsg.CreateTopicsRequest{
		TimeoutMillis: 15000,
	}
	for _, n := range names {
		req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{Topic: n, NumPartitions: 1, ReplicationFactor: 1})
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

// roundOf reads the pc-round header (0 when absent).
func roundOf(hdrs []swimlane.Header) int {
	for _, h := range hdrs {
		if h.Key == roundHeader {
			if n, err := strconv.Atoi(string(h.Value)); err == nil {
				return n
			}
		}
	}
	return 0
}

// kind classifies a produced value.
func kind(v string) string {
	if strings.HasPrefix(v, "poison:") {
		return "poison"
	}
	if strings.HasPrefix(v, "flaky:") {
		return "flaky"
	}
	if strings.HasPrefix(v, "transient:") {
		return "transient"
	}
	return "ok"
}

// handle is the main consumer handler: ok handled, transient fails once,
// flaky fails until round >= 1, poison always fails.
func handle(_ context.Context, msg *swimlane.Message) error {
	time.Sleep(jitter()) // emulate shaky downstream IO

	v := string(msg.Value)
	round := roundOf(msg.Headers)
	switch kind(v) {
	case "ok":
		recordSuccess(msg.Key, msg.Offset, true)
		return nil
	case "transient":
		if _, loaded := transientSeen.LoadOrStore(string(msg.Key), struct{}{}); !loaded {
			return errors.New("transient downstream hiccup") // recovered by in-process Retry
		}
		recordSuccess(msg.Key, msg.Offset, false)
		return nil
	case "flaky":
		if round < 1 {
			return errors.New("downstream warming up") // needs one external retry
		}
		recordSuccess(msg.Key, msg.Offset, false)
		return nil
	default: // poison
		return errors.New("permanently bad message")
	}
}

var transientSeen sync.Map // transient key already attempted once this run

func recordSuccess(key []byte, off swimlane.Offset, isOK bool) {
	successCount.Add(1)
	if isOK {
		successMu.Lock()
		perKey[string(key)] = append(perKey[string(key)], int64(off))
		successMu.Unlock()
	}
}

// newProducer returns a client that writes to a fixed topic.
func newProducer(ctx context.Context, topic string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic(topic))
}

func main() {
	flag.Parse()

	ctx := context.Background()
	if err := waitForKafka(ctx); err != nil {
		log.Fatal(err)
	}
	stamp := time.Now().UnixNano()
	mainTopic := fmt.Sprintf("pc-demo-%d", stamp)
	retryTopic := mainTopic + "-retry"
	dlqTopic := mainTopic + "-dlq"
	if err := createTopics(ctx, mainTopic, retryTopic, dlqTopic); err != nil {
		log.Fatal(err)
	}

	// ---- produce a realistic mix ----
	nPoison := *total * *poison / 100
	nFlaky := *total * *flaky / 100
	nTrans := *total * *trans / 100
	nOK := *total - nPoison - nFlaky - nTrans

	prod, err := newProducer(ctx, mainTopic)
	if err != nil {
		log.Fatal(err)
	}
	emit := func(kind, key string, round int) {
		val := kind + ":" + key
		hdrs := []kgo.RecordHeader{}
		if round > 0 {
			hdrs = append(hdrs, kgo.RecordHeader{Key: roundHeader, Value: []byte(strconv.Itoa(round))})
		}
		prod.Produce(ctx, &kgo.Record{Key: []byte(key), Value: []byte(val), Headers: hdrs}, func(*kgo.Record, error) {})
	}
	for i := 0; i < nPoison; i++ {
		emit("poison", fmt.Sprintf("poison-%d", i), 0)
	}
	for i := 0; i < nFlaky; i++ {
		emit("flaky", fmt.Sprintf("flaky-%d", i), 0)
	}
	for i := 0; i < nTrans; i++ {
		emit("transient", fmt.Sprintf("t-%d", i), 0)
	}
	for i := 0; i < nOK; i++ {
		emit("ok", fmt.Sprintf("user-%d", i%10), 0) // repeated keys exercise ordering
	}
	prod.Flush(ctx)
	prod.Close()
	log.Printf("produced total=%d (poison=%d flaky=%d transient=%d ok=%d)", *total, nPoison, nFlaky, nTrans, nOK)

	// ---- retry worker: drain the retry topic, re-produce or DLQ ----
	retrier := newRetrier(ctx, mainTopic, retryTopic, dlqTopic)
	go retrier.run()

	// ---- main consumer (the SDK) ----
	be, err := kafka.New(kafka.Config{Brokers: []string{brokers}, Group: mainTopic})
	if err != nil {
		log.Fatal(err)
	}
	retryProd, err := newProducer(ctx, retryTopic) // shared: OnDiscard can run concurrently
	if err != nil {
		log.Fatal(err)
	}
	defer retryProd.Close()
	cfg := swimlane.DefaultConfig()
	cfg.Lanes = 8
	cfg.PollTimeout = 300 * time.Millisecond
	cfg.RebalanceTimeout = 3 * time.Second
	cfg.Retry = swimlane.RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond}
	cfg.OnDiscard = func(ctx context.Context, msg *swimlane.Message, err error) {
		// hand the failure to the MQ-level retry topic
		round := roundOf(msg.Headers)
		retryProd.Produce(ctx, &kgo.Record{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: []kgo.RecordHeader{{Key: roundHeader, Value: []byte(strconv.Itoa(round + 1))}},
		}, func(*kgo.Record, error) {})
		retryProd.Flush(ctx)
	}
	c, err := swimlane.New(be, cfg)
	if err != nil {
		log.Fatal(err)
	}
	_ = c.Subscribe([]string{mainTopic}, handle)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	start := time.Now()
	go func() { runErr <- c.Run(runCtx) }()

	// ---- wait for the expected outcome ----
	wantSuccess := int64(nOK + nFlaky + nTrans)
	deadline := time.Now().Add(90 * time.Second)
	lastLog := time.Now()
	for {
		if successCount.Load() >= wantSuccess && dlqCount.Load() >= int64(nPoison) {
			break
		}
		select {
		case err := <-runErr:
			log.Fatalf("main consumer exited early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			log.Fatalf("timeout: handled=%d want=%d dlq=%d want=%d",
				successCount.Load(), wantSuccess, dlqCount.Load(), nPoison)
		}
		if time.Since(lastLog) > 2*time.Second {
			lastLog = time.Now()
			log.Printf("progress: handled=%d/%d dlq=%d/%d", successCount.Load(), wantSuccess, dlqCount.Load(), nPoison)
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	c.Stop()
	mainStopped.Store(true)
	<-retrier.done // let retrier flush any remaining retry-topic messages
	cancel()

	// ---- assertions ----
	ordered := checkOrder()
	handled := successCount.Load()
	fmt.Printf("handled=%d/%d dlq=%d/%d per-key-ordered=%v elapsed=%s throughput=%.0f msg/s\n",
		handled, wantSuccess, dlqCount.Load(), nPoison, ordered, elapsed.Round(time.Millisecond),
		float64(*total)/elapsed.Seconds())
	if handled != wantSuccess || dlqCount.Load() != int64(nPoison) || !ordered {
		os.Exit(1)
	}
	fmt.Println("OK: transient failures recovered in-process, flaky via retry topic, poison landed in DLQ")
}

// checkOrder verifies ok messages per key saw strictly increasing offsets.
func checkOrder() bool {
	successMu.Lock()
	defer successMu.Unlock()
	for key, offs := range perKey {
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				log.Printf("ORDER VIOLATION key=%s offsets=%v", key, offs)
				return false
			}
		}
	}
	return true
}

// retrier consumes the retry topic; rounds below maxRound go back to the main
// topic (attempting again), rounds at/above maxRound land in the DLQ.
type retrier struct {
	mainTopic  string
	retryTopic string
	dlqTopic   string
	done       chan struct{}
}

func newRetrier(ctx context.Context, mainTopic, retryTopic, dlqTopic string) *retrier {
	return &retrier{mainTopic: mainTopic, retryTopic: retryTopic, dlqTopic: dlqTopic, done: make(chan struct{})}
}

func (r *retrier) run() {
	defer close(r.done)
	ctx := context.Background()
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.retryTopic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		log.Fatalf("retrier: %v", err)
	}
	defer cli.Close()
	out, err := newProducer(ctx, r.mainTopic)
	if err != nil {
		log.Fatalf("retrier producer: %v", err)
	}
	defer out.Close()
	dlq, err := newProducer(ctx, r.dlqTopic)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	idle := 0
	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 150*time.Millisecond)
		fetches := cli.PollRecords(pollCtx, 500)
		pollCancel()
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			for _, fp := range ft.Partitions {
				for _, rec := range fp.Records {
					round := 0
					for _, h := range rec.Headers {
						if h.Key == roundHeader {
							round, _ = strconv.Atoi(string(h.Value))
						}
					}
					if round >= maxRound {
						dlq.Produce(ctx, &kgo.Record{Key: rec.Key, Value: rec.Value}, func(*kgo.Record, error) {})
						dlqCount.Add(1)
						continue
					}
					// re-attempt on the main topic with an incremented round
					out.Produce(ctx, &kgo.Record{
						Key:     rec.Key,
						Value:   rec.Value,
						Headers: []kgo.RecordHeader{{Key: roundHeader, Value: []byte(strconv.Itoa(round + 1))}},
					}, func(*kgo.Record, error) {})
				}
			}
		})
		dlq.Flush(ctx)
		out.Flush(ctx)
		if mainStopped.Load() {
			idle++
			if idle > 5 {
				return // nothing left to feed
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}
