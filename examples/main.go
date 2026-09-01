package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mq-parallel-consumer"
	"mq-parallel-consumer/backend/kafka"
)

func main() {
	cfg := swimlane.DefaultConfig()
	cfg.Lanes = 8
	cfg.Retry = swimlane.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}
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
