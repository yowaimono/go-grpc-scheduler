package scheduler

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestIntervalOneSecondWallClockPrecision(t *testing.T) {
	if os.Getenv("SCHEDULER_WALLCLOCK_TEST") != "1" {
		t.Skip("set SCHEDULER_WALLCLOCK_TEST=1 to run wall-clock precision test")
	}
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "precision", Name: "precision", Role: "default", RunAt: time.Now(), ScheduleType: "INTERVAL", Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	const samples = 10
	observed := make([]time.Time, 0, samples)
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for len(observed) < samples {
		select {
		case now := <-ticker.C:
			engine.Tick(now)
			task, err := engine.Assign("worker", now)
			if err == nil {
				observed = append(observed, now)
				if err := engine.Complete("worker", task.ID, true); err != nil {
					t.Fatal(err)
				}
			}
		case <-deadline:
			t.Fatalf("only observed %d/%d runs", len(observed), samples)
		}
	}
	anchor := observed[0]
	var maxLag time.Duration
	for i, actual := range observed {
		expected := anchor.Add(time.Duration(i) * time.Second)
		lag := actual.Sub(expected)
		if lag < 0 {
			lag = -lag
		}
		if lag > maxLag {
			maxLag = lag
		}
		if i > 0 {
			t.Logf("run=%02d interval=%s lag_from_ideal=%s", i+1, actual.Sub(observed[i-1]), lag)
		}
	}
	average := observed[len(observed)-1].Sub(observed[0]) / time.Duration(len(observed)-1)
	t.Logf("summary samples=%d average_interval=%s max_lag=%s", samples, average, maxLag)
	if maxLag > time.Second {
		t.Fatalf("max lag %s exceeds 1 second precision target", maxLag)
	}
}
