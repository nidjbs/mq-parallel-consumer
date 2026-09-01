package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"mq-parallel-consumer"
)

func TestToMessage(t *testing.T) {
	r := &kgo.Record{
		Topic:   "t",
		Offset:  7,
		Key:     []byte("k"),
		Value:   []byte("v"),
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
	o := toFranzOffsets(commits)
	if o["t"][0].Offset != 5 || o["t"][1].Offset != 9 {
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
