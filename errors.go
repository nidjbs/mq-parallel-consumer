package swimlane

import "errors"

var (
	ErrInvalidConfig = errors.New("swimlane: invalid config")
	ErrClosed        = errors.New("swimlane: closed")
	ErrHandlerFatal  = errors.New("swimlane: handler failed")
)
