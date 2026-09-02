package timewheel

import (
	"container/heap"
	"sync"
	"time"
)

type Entry struct {
	Key      string
	Due      time.Time
	Version  int64
	Rounds   int
	position int
}

type entryHeap []*Entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].Due.Before(h[j].Due) }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(v any)        { *h = append(*h, v.(*Entry)) }
func (h *entryHeap) Pop() any          { old := *h; n := len(old); v := old[n-1]; *h = old[:n-1]; return v }

type Wheel struct {
	mu       sync.Mutex
	tick     time.Duration
	slots    [][]*Entry
	current  int
	lastTick time.Time
	overflow entryHeap
}

func New(tick time.Duration, slotCount int, now time.Time) *Wheel {
	if tick <= 0 {
		tick = time.Second
	}
	if slotCount < 2 {
		slotCount = 60
	}
	w := &Wheel{tick: tick, slots: make([][]*Entry, slotCount), lastTick: now.Truncate(tick)}
	heap.Init(&w.overflow)
	return w
}

func (w *Wheel) Add(e *Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.addLocked(e, w.lastTick)
}

func (w *Wheel) addLocked(e *Entry, now time.Time) {
	delta := e.Due.Sub(now)
	if delta < 0 {
		delta = 0
	}
	ticks := int64(delta / w.tick)
	if ticks == 0 {
		ticks = 1
	}
	slot := (w.current + int(ticks)%len(w.slots)) % len(w.slots)
	e.Rounds = int(ticks / int64(len(w.slots)))
	e.position = slot
	w.slots[slot] = append(w.slots[slot], e)
}

// Advance moves the wheel to now and returns entries whose deadline has arrived.
// A single scheduler goroutine should own this call; the mutex also makes it safe for tests.
func (w *Wheel) Advance(now time.Time) []*Entry {
	w.mu.Lock()
	defer w.mu.Unlock()
	if now.Before(w.lastTick.Add(w.tick)) {
		return nil
	}
	steps := int(now.Sub(w.lastTick) / w.tick)
	var due []*Entry
	for i := 0; i < steps; i++ {
		w.current = (w.current + 1) % len(w.slots)
		bucket := w.slots[w.current]
		w.slots[w.current] = nil
		for _, e := range bucket {
			if e.Rounds > 0 {
				e.Rounds--
				w.slots[w.current] = append(w.slots[w.current], e)
				continue
			}
			if !e.Due.After(now) {
				due = append(due, e)
			} else {
				w.addLocked(e, now)
			}
		}
	}
	w.lastTick = w.lastTick.Add(time.Duration(steps) * w.tick)
	return due
}

func (w *Wheel) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.overflow.Len()
	for _, bucket := range w.slots {
		n += len(bucket)
	}
	return n
}
