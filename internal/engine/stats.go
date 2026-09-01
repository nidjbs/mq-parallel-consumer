package engine

// PartitionStat is a snapshot of one partition's worker.
type PartitionStat struct {
	InFlight    int64
	BaseOffset  Offset
	MaxInFlight int
}

// Stats is a point-in-time snapshot of the consumer.
type Stats struct {
	Mode          Mode
	Partitions    int
	InFlightTotal int64
	PerPartition  map[TopicPartition]PartitionStat
}
