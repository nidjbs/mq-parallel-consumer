package engine

import "sync/atomic"

// counters holds cumulative metrics shared across all workers.
type counters struct {
	processed     atomic.Int64
	discarded     atomic.Int64
	handlerErrors atomic.Int64
	commits       atomic.Int64
	commitErrors  atomic.Int64
}

// PartitionStat is a snapshot of one partition's worker.
type PartitionStat struct {
	InFlight    int64
	BaseOffset  Offset // highest contiguous completed + 1
	HighestSeen Offset // highest offset routed to this partition
	MaxInFlight int
}

// Stats is a point-in-time snapshot of the consumer.
type Stats struct {
	Mode              Mode
	Partitions        int
	InFlightTotal     int64
	PerPartition      map[TopicPartition]PartitionStat
	MessagesProcessed int64
	MessagesDiscarded int64
	HandlerErrors     int64
	Commits           int64
	CommitErrors      int64
}
