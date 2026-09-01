package swimlane

import "testing"

func TestContiguousAdvance(t *testing.T) {
	tr := newOffsetTracker()
	tr.seed(0)
	if got := tr.complete(0); got != 1 {
		t.Fatalf("complete(0) base = %d, want 1", got)
	}
	if got := tr.complete(1); got != 2 {
		t.Fatalf("complete(1) base = %d, want 2", got)
	}
}

func TestOutOfOrderCompletion(t *testing.T) {
	tr := newOffsetTracker()
	tr.seed(0)
	tr.complete(2) // out of order: base stays 0
	tr.complete(1) // still missing 0: base stays 0
	if got := tr.baseOffset(); got != 0 {
		t.Fatalf("base = %d, want 0 (hole at 0)", got)
	}
	if got := tr.complete(0); got != 3 { // 0 closes the run 0,1,2
		t.Fatalf("base = %d, want 3", got)
	}
}

func TestDuplicateIgnored(t *testing.T) {
	tr := newOffsetTracker()
	tr.seed(0)
	tr.complete(0)
	if got := tr.complete(0); got != 1 { // duplicate < base is ignored
		t.Fatalf("duplicate complete returned %d, want 1", got)
	}
}

func TestSeedLazily(t *testing.T) {
	tr := newOffsetTracker()
	if tr.seeded() {
		t.Fatal("tracker should start unseeded")
	}
	tr.seed(10)
	if got := tr.baseOffset(); got != 10 {
		t.Fatalf("base = %d, want 10", got)
	}
	if !tr.seeded() {
		t.Fatal("tracker should be seeded after seed()")
	}
	tr.seed(99) // second seed is a no-op
	if got := tr.baseOffset(); got != 10 {
		t.Fatalf("base = %d, want 10", got)
	}
}

func TestCompleteBeforeSeed(t *testing.T) {
	tr := newOffsetTracker()
	if got := tr.complete(5); got != 6 { // complete seeds implicitly
		t.Fatalf("base = %d, want 6", got)
	}
}
