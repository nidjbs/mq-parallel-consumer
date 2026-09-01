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
	// MaxPollRecords caps the number of records returned per Poll; 0 = 500.
	// Bounds the memory a single fetch batch can hold in flight.
	MaxPollRecords int
	// FetchMaxBytes caps bytes fetched per broker round trip; 0 = franz-go
	// default. Bounds the client-side fetch buffer.
	FetchMaxBytes int32
}
