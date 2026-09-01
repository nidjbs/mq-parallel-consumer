package swimlane

import "time"

// TopicPartition identifies a partition/queue.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Offset is a MQ-agnostic position.
type Offset int64

// Header is a MQ-agnostic message header.
type Header struct {
	Key   string
	Value []byte
}

// Message is the transport-agnostic record.
type Message struct {
	Topic     string
	Partition int32
	Offset    Offset
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// TP returns the message's topic-partition.
func (m *Message) TP() TopicPartition {
	return TopicPartition{Topic: m.Topic, Partition: m.Partition}
}
