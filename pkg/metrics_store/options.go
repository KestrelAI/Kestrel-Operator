package metrics_store

import "time"

// Option is a functional option for configuring the MetricsStore.
type Option func(*MetricsStore)

// WithRetention sets the retention duration for metrics data.
func WithRetention(d time.Duration) Option {
	return func(s *MetricsStore) {
		s.retentionDuration = d
	}
}

// WithMaxSeries sets the maximum number of unique metric series.
func WithMaxSeries(max int) Option {
	return func(s *MetricsStore) {
		s.maxSeries = max
	}
}

// WithRingBufferSize sets the size of ring buffers for each series.
func WithRingBufferSize(size int) Option {
	return func(s *MetricsStore) {
		s.ringBufferSize = size
	}
}

// WithEvictionInterval sets the interval for background eviction.
func WithEvictionInterval(d time.Duration) Option {
	return func(s *MetricsStore) {
		s.evictionInterval = d
	}
}
