package httpapi

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
)

func TestRealSchedulerTickerOneSecondPrecision(t *testing.T) {
	if os.Getenv("SCHEDULER_WALLCLOCK_TEST") != "1" {
		t.Skip("set SCHEDULER_WALLCLOCK_TEST=1 to run real ticker precision test")
	}
	engine := scheduler.New("master", &scheduler.Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), scheduler.Task{ID: "ticker-precision", Name: "ticker", Role: "default", RunAt: time.Now(), ScheduleType: "INTERVAL", Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go RunTicker(stop, engine)
	defer close(stop)
	observed := make([]time.Time, 0, 5)
	deadline := time.After(12 * time.Second)
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for len(observed) < cap(observed) {
		select {
		case now := <-poll.C:
			task, err := engine.Assign("worker", now)
			if err == nil {
				observed = append(observed, now)
				if err := engine.Complete("worker", task.ID, true); err != nil {
					t.Fatal(err)
				}
			}
		case <-deadline:
			t.Fatalf("only observed %d runs", len(observed))
		}
	}
	var maxJitter time.Duration
	for i := 1; i < len(observed); i++ {
		delta := observed[i].Sub(observed[i-1])
		jitter := delta - time.Second
		if jitter < 0 {
			jitter = -jitter
		}
		if jitter > maxJitter {
			maxJitter = jitter
		}
		t.Logf("run=%d interval=%s jitter=%s", i+1, delta, jitter)
	}
	t.Logf("summary runs=%d max_jitter=%s", len(observed), maxJitter)
}
