package swimlane

import engine "mq-parallel-consumer/internal/engine"

type (
	Mode             = engine.Mode
	RebalanceHandler = engine.RebalanceHandler
	Backend          = engine.Backend
)

const (
	KeyOrdered = engine.KeyOrdered
	Unordered  = engine.Unordered
)
