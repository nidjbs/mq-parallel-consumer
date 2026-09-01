package swimlane

import engine "mq-parallel-consumer/internal/engine"

// Types are re-exported from the engine package; the engine holds the
// real definitions.
type (
	TopicPartition = engine.TopicPartition
	Offset         = engine.Offset
	Header         = engine.Header
	Message        = engine.Message
)
