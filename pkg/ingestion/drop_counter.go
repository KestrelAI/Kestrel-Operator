package ingestion

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// DropCounter tracks dropped events for a channel and rate-limits the warning logs.
// Instead of logging per-event, it accumulates a count and logs a summary every interval.
type DropCounter struct {
	channelName string
	logger      *zap.Logger
	interval    time.Duration

	dropped  atomic.Int64
	lastLog  atomic.Int64 // unix nanos of last log
}

// NewDropCounter creates a counter that logs at most once per interval.
func NewDropCounter(channelName string, logger *zap.Logger, interval time.Duration) *DropCounter {
	return &DropCounter{
		channelName: channelName,
		logger:      logger,
		interval:    interval,
	}
}

// RecordDrop increments the drop count and logs a summary if the interval has elapsed.
func (dc *DropCounter) RecordDrop() {
	dc.dropped.Add(1)

	now := time.Now().UnixNano()
	last := dc.lastLog.Load()

	if now-last >= dc.interval.Nanoseconds() {
		// Try to claim the log slot (avoid multiple goroutines logging simultaneously)
		if dc.lastLog.CompareAndSwap(last, now) {
			// Swap atomically so concurrent Add()s between our Add and here are not lost
			count := dc.dropped.Swap(0)
			dc.logger.Warn("Channel full, events dropped",
				zap.String("channel", dc.channelName),
				zap.Int64("dropped_since_last_log", count),
			)
		}
	}
}
