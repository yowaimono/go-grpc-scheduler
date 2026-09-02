package scheduler

import (
	"testing"
	"time"
)

func TestNextCron(t *testing.T) {
	next, err := NextCron("*/5 * * * *", "UTC", time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if next.Minute() != 5 {
		t.Fatalf("next = %v", next)
	}
}

func TestNextCronTimezoneAndInvalidTimezone(t *testing.T) {
	from := time.Date(2026, 1, 1, 23, 59, 0, 0, time.UTC)
	next, err := NextCron("0 0 * * *", "Asia/Shanghai", from)
	if err != nil {
		t.Fatal(err)
	}
	if next.Location().String() != "Asia/Shanghai" {
		t.Fatalf("location=%s", next.Location())
	}
	if _, err := NextCron("0 0 * * *", "Mars/Phobos", from); err == nil {
		t.Fatal("invalid timezone accepted")
	}
	if _, err := NextCron("not cron", "UTC", from); err == nil {
		t.Fatal("invalid cron accepted")
	}
}
