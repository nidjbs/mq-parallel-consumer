package engine

import (
	"math/rand"
	"time"
)

// maxBackoffShifts caps the exponential growth so the shift never overflows
// int64; 20 doubles take 1s up to ~12 days.
const maxBackoffShifts = 20

// retryBackoff computes the base backoff for a given attempt (0-based).
func retryBackoff(p RetryPolicy, attempt int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	if attempt > maxBackoffShifts {
		attempt = maxBackoffShifts
	}
	d := p.InitialBackoff << attempt
	if d < p.InitialBackoff { // overflow wrapped negative
		d = time.Duration(1<<63 - 1)
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// jitteredBackoff adds ±25% randomness to break synchronized retries across
// consumers retrying the same failing message.
func jitteredBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(float64(d) * (0.75 + rand.Float64()*0.5))
}
