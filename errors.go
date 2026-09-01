package swimlane

import engine "mq-parallel-consumer/internal/engine"

var (
	ErrInvalidConfig = engine.ErrInvalidConfig
	ErrClosed        = engine.ErrClosed
	ErrHandlerFatal  = engine.ErrHandlerFatal
)
