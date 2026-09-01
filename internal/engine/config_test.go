package engine

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
	// CommitInterval=0 is meaningful (commit-on-advance) and stays 0;
	// periodic default only comes from DefaultConfig().
	if c.CommitInterval != 0 {
		t.Fatalf("CommitInterval should stay 0 for a raw Config, got %v", c.CommitInterval)
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
		{Mode: KeyOrdered, QueueSize: -1},
		{Mode: KeyOrdered, PollTimeout: -1},
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
