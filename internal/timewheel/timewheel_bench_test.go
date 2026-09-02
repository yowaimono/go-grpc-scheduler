package timewheel

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func BenchmarkWheelTenThousandTasks(b *testing.B) {
	now := time.Unix(0, 0)
	count := 10000
	if raw := os.Getenv("TASK_COUNT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			count = parsed
		}
	}
	entries := make([]*Entry, count)
	for i := range entries {
		entries[i] = &Entry{Key: fmt.Sprintf("task-%d", i), Due: now.Add(time.Duration(i%3600) * time.Second)}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wheel := New(time.Second, 3600, now)
		for _, entry := range entries {
			wheel.Add(&Entry{Key: entry.Key, Due: entry.Due})
		}
		wheel.Advance(now.Add(time.Second))
	}
}
