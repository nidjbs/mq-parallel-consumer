package engine

import (
	"testing"
	"time"
)

func TestRetryBackoffExponential(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 4, InitialBackoff: 100 * time.Millisecond}
	if got := retryBackoff(p, 0); got != 100*time.Millisecond {
		t.Fatalf("attempt0 = %v", got)
	}
	if got := retryBackoff(p, 1); got != 200*time.Millisecond {
		t.Fatalf("attempt1 = %v", got)
	}
	if got := retryBackoff(p, 2); got != 400*time.Millisecond {
		t.Fatalf("attempt2 = %v", got)
	}
}

func TestRetryBackoffCapped(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}
	if got := retryBackoff(p, 5); got != 500*time.Millisecond {
		t.Fatalf("capped = %v", got)
	}
}

func TestRetryBackoffZero(t *testing.T) {
	p := RetryPolicy{}
	if got := retryBackoff(p, 3); got != 0 {
		t.Fatalf("no backoff = %v", got)
	}
}
