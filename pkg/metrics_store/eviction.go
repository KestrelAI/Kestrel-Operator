package metrics_store

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// StartEviction starts the background eviction goroutine.
func (s *MetricsStore) StartEviction(ctx context.Context) {
	// Guard against zero or negative eviction interval which would cause NewTicker to panic
	if s.evictionInterval <= 0 {
		s.logger.Warn("Invalid eviction interval, skipping eviction goroutine",
			zap.Duration("interval", s.evictionInterval))
		return
	}

	go func() {
		ticker := time.NewTicker(s.evictionInterval)
		defer ticker.Stop()

		s.logger.Info("Started metrics store eviction goroutine",
			zap.Duration("interval", s.evictionInterval),
			zap.Duration("retention", s.retentionDuration))

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Metrics store eviction stopped")
				return
			case <-ticker.C:
				s.evictStaleData()
			}
		}
	}()
}

// evictStaleData removes series that have no data within the retention window.
func (s *MetricsStore) evictStaleData() {
	// Guard against zero or negative retention which would evict everything
	if s.retentionDuration <= 0 {
		s.logger.Warn("Skipping eviction: invalid retention duration",
			zap.Duration("retention", s.retentionDuration))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-s.retentionDuration)
	evicted := 0

	for key, series := range s.seriesMap {
		// Check if the newest data point is older than cutoff
		newest := series.Buffer.NewestTimestamp()
		if newest.IsZero() || newest.Before(cutoff) {
			// Remove from primary storage
			delete(s.seriesMap, key)

			// Remove from all indexes
			s.removeFromIndexes(series)

			evicted++
		}
	}

	if evicted > 0 {
		s.totalEvictions += int64(evicted)
		s.logger.Info("Evicted stale series",
			zap.Int("evicted", evicted),
			zap.Int("remaining", len(s.seriesMap)),
			zap.Duration("retention", s.retentionDuration))
	}
}

// ForceEviction runs an immediate eviction cycle (useful for testing).
func (s *MetricsStore) ForceEviction() {
	s.evictStaleData()
}
