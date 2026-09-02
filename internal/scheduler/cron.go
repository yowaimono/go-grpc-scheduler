package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func NextCron(expr, timezone string, from time.Time) (time.Time, error) {
	loc := time.UTC
	if timezone != "" {
		parsed, err := time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, err
		}
		loc = parsed
	}
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron: %w", err)
	}
	return schedule.Next(from.In(loc)), nil
}
