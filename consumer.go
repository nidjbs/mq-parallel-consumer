package swimlane

import engine "mq-parallel-consumer/internal/engine"

// Consumer is the parallel consumer. Thread-safe; use it like any SDK object.
type Consumer = engine.Consumer

// New validates the config and builds a consumer around a backend.
func New(backend Backend, cfg Config) (*Consumer, error) {
	return engine.New(backend, cfg)
}
