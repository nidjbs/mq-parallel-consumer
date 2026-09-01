package swimlane

import "time"

// retryBackoff computes the backoff for a given attempt (0-based).
func retryBackoff(p RetryPolicy, attempt int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	d := p.InitialBackoff * (1 << uint(attempt))
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}
