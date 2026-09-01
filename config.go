package swimlane

import engine "mq-parallel-consumer/internal/engine"

type (
	Handler     = engine.Handler
	RetryPolicy = engine.RetryPolicy
	Config      = engine.Config
)

// DefaultConfig returns the recommended configuration.
func DefaultConfig() Config {
	return engine.DefaultConfig()
}
