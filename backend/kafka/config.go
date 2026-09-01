package kafka

import (
	"crypto/tls"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Config holds kafka-specific transport settings.
type Config struct {
	Brokers []string
	Group   string
	SASL    *plain.Auth
	TLS     *tls.Config
	// ConsumeResetOffset is where to start when no committed offset exists.
	// nil = start at the beginning.
	ConsumeResetOffset *kgo.Offset
}
