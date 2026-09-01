package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"

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
		opts = append(opts, kgo.ConsumeResetOffset(*cfg.ConsumeResetOffset))
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
					// offset 0 is a placeholder; core seeds lazily from the
					// first polled message.
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
		return msgs, errs[0].Err
	}
	return msgs, nil
}

func (b *Backend) Commit(ctx context.Context, commits map[swimlane.TopicPartition]swimlane.Offset) error {
	var commitErr error
	b.cli.CommitOffsetsSync(ctx, toFranzOffsets(commits), func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
	})
	return commitErr
}

func (b *Backend) Pause(parts []swimlane.TopicPartition) error {
	b.cli.PauseFetchPartitions(toFranzPartitions(parts))
	return nil
}

func (b *Backend) Resume(parts []swimlane.TopicPartition) error {
	b.cli.ResumeFetchPartitions(toFranzPartitions(parts))
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

func toFranzOffsets(commits map[swimlane.TopicPartition]swimlane.Offset) map[string]map[int32]kgo.EpochOffset {
	m := make(map[string]map[int32]kgo.EpochOffset)
	for tp, off := range commits {
		p := m[tp.Topic]
		if p == nil {
			p = make(map[int32]kgo.EpochOffset)
			m[tp.Topic] = p
		}
		p[tp.Partition] = kgo.EpochOffset{Offset: int64(off)}
	}
	return m
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
