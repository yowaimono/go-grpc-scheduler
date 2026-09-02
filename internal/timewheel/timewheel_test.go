package timewheel

import (
	"testing"
	"time"
)

func TestWheelDeliversDueEntries(t *testing.T) {
	now := time.Unix(0, 0)
	w := New(time.Second, 8, now)
	w.Add(&Entry{Key: "one", Due: now.Add(2 * time.Second)})
	if got := w.Advance(now.Add(time.Second)); len(got) != 0 {
		t.Fatalf("early entries = %d", len(got))
	}
	got := w.Advance(now.Add(2 * time.Second))
	if len(got) != 1 || got[0].Key != "one" {
		t.Fatalf("due = %#v", got)
	}
}

func TestWheelRounds(t *testing.T) {
	now := time.Unix(0, 0)
	w := New(time.Second, 4, now)
	w.Add(&Entry{Key: "late", Due: now.Add(6 * time.Second)})
	got := w.Advance(now.Add(5 * time.Second))
	if len(got) != 0 {
		t.Fatalf("late entry fired early")
	}
	got = w.Advance(now.Add(6 * time.Second))
	if len(got) != 1 {
		t.Fatalf("late entry not fired: %d", len(got))
	}
}

func TestWheelDefaultsAndBoundaryBehavior(t *testing.T) {
	now := time.Unix(10, 123)
	w := New(0, 1, now)
	if w.tick != time.Second || len(w.slots) != 60 {
		t.Fatalf("defaults tick=%s slots=%d", w.tick, len(w.slots))
	}
	w.Add(&Entry{Key: "past", Due: now.Add(-time.Minute)})
	if got := w.Advance(now.Add(500 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("advanced before tick: %d", len(got))
	}
	if got := w.Advance(now.Add(time.Second)); len(got) != 1 || got[0].Key != "past" {
		t.Fatalf("past due = %#v", got)
	}
}

func TestWheelDoesNotLoseEntriesAfterLargeJump(t *testing.T) {
	now := time.Unix(0, 0)
	w := New(time.Second, 4, now)
	w.Add(&Entry{Key: "far", Due: now.Add(10 * time.Second)})
	if got := w.Advance(now.Add(10 * time.Second)); len(got) != 1 {
		t.Fatalf("large jump due=%d", len(got))
	}
}

func TestWheelSizeTracksEntries(t *testing.T) {
	now := time.Unix(0, 0)
	w := New(time.Second, 8, now)
	if w.Size() != 0 {
		t.Fatal("new wheel not empty")
	}
	w.Add(&Entry{Key: "a", Due: now.Add(time.Second)})
	w.Add(&Entry{Key: "b", Due: now.Add(2 * time.Second)})
	if w.Size() != 2 {
		t.Fatalf("size=%d", w.Size())
	}
	_ = w.Advance(now.Add(time.Second))
	if w.Size() != 1 {
		t.Fatalf("size after advance=%d", w.Size())
	}
}
