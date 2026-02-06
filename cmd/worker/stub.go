package main

import (
	"context"
	"time"
)

func runStub(
	ctx context.Context,
	heartbeat time.Duration,
	duration time.Duration,
	emit func(name string, fields any),
) error {
	if heartbeat <= 0 {
		heartbeat = time.Second
	}

	var done <-chan time.Time
	if duration > 0 {
		t := time.NewTimer(duration)
		defer t.Stop()
		done = t.C
	}

	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	start := time.Now()
	count := 0
	for {
		select {
		case <-ticker.C:
			count++
			emit("heartbeat", map[string]any{
				"count":      count,
				"elapsed_ms": time.Since(start).Milliseconds(),
			})
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
