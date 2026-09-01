package swimlane

import (
	"testing"
	"time"
)

// The public facade re-exports the engine implementation.
func TestFacadeDefaultConfig(t *testing.T) {
	d := DefaultConfig()
	if d.Mode != KeyOrdered || d.Lanes != 8 || d.CommitInterval != 100*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", d)
	}
}

func TestFacadeMessageTP(t *testing.T) {
	m := Message{Topic: "t", Partition: 2}
	if m.TP() != (TopicPartition{Topic: "t", Partition: 2}) {
		t.Fatalf("TP = %+v", m.TP())
	}
}

func TestFacadeNewNilBackend(t *testing.T) {
	if _, err := New(nil, Config{}); err == nil {
		t.Fatal("nil backend should error")
	}
}
