package metrics_store

import (
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

// Configuration defaults
const (
	DefaultRetentionDuration = 30 * time.Minute
	DefaultMaxSeries         = 100000
	DefaultRingBufferSize    = 60 // 30 min at 30s intervals
	DefaultEvictionInterval  = 30 * time.Second
)

// MetricsStore stores time-series metrics in memory with a rolling window.
type MetricsStore struct {
	mu sync.RWMutex

	// Primary storage
	seriesMap map[SeriesKey]*SeriesData

	// Secondary indexes for fast lookup
	byNamespace  map[string]map[SeriesKey]struct{}
	byWorkload   map[string]map[SeriesKey]struct{} // "namespace/kind/name"
	byPod        map[string]map[SeriesKey]struct{} // "namespace/podName"
	byMetricName map[string]map[SeriesKey]struct{}

	// Pod to workload resolver
	podResolver *PodWorkloadResolver

	// Configuration
	retentionDuration time.Duration
	maxSeries         int
	ringBufferSize    int
	evictionInterval  time.Duration

	// Metrics/stats
	totalInserts   int64
	totalEvictions int64
	droppedSeries  int64

	logger *zap.Logger
}

// StoreStats contains statistics about the metrics store.
type StoreStats struct {
	SeriesCount    int
	TotalInserts   int64
	TotalEvictions int64
	DroppedSeries  int64
}

// New creates a new MetricsStore with the given options.
func New(logger *zap.Logger, podResolver *PodWorkloadResolver, opts ...Option) *MetricsStore {
	s := &MetricsStore{
		seriesMap:         make(map[SeriesKey]*SeriesData),
		byNamespace:       make(map[string]map[SeriesKey]struct{}),
		byWorkload:        make(map[string]map[SeriesKey]struct{}),
		byPod:             make(map[string]map[SeriesKey]struct{}),
		byMetricName:      make(map[string]map[SeriesKey]struct{}),
		podResolver:       podResolver,
		retentionDuration: DefaultRetentionDuration,
		maxSeries:         DefaultMaxSeries,
		ringBufferSize:    DefaultRingBufferSize,
		evictionInterval:  DefaultEvictionInterval,
		logger:            logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Insert adds a batch of metrics to the store.
func (s *MetricsStore) Insert(batch *v1.OTelMetricBatch) error {
	if batch == nil || len(batch.Metrics) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, metric := range batch.Metrics {
		s.insertMetricLocked(metric)
	}

	return nil
}

// InsertMetric adds a single metric to the store.
func (s *MetricsStore) InsertMetric(metric *v1.OTelMetric) error {
	if metric == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.insertMetricLocked(metric)
	return nil
}

// insertMetricLocked adds a metric to the store (must be called with lock held).
func (s *MetricsStore) insertMetricLocked(metric *v1.OTelMetric) {
	// Skip metrics with no data points to avoid creating empty series
	if len(metric.DataPoints) == 0 {
		return
	}

	// Resolve workload if missing
	if metric.WorkloadName == "" && metric.PodName != "" && s.podResolver != nil {
		if info, ok := s.podResolver.ResolveWorkload(metric.Namespace, metric.PodName); ok {
			metric.WorkloadName = info.Name
			metric.WorkloadKind = info.Kind
		}
	}

	key := computeSeriesKey(metric)

	series, exists := s.seriesMap[key]
	if !exists {
		// Check capacity
		if len(s.seriesMap) >= s.maxSeries {
			s.droppedSeries++
			s.logger.Warn("Dropping series, max capacity reached",
				zap.String("metric", metric.Name),
				zap.Int("max_series", s.maxSeries))
			return
		}

		// Create new series
		series = createSeriesFromMetric(key, metric, s.ringBufferSize)
		s.seriesMap[key] = series
		s.addToIndexes(series)
	}

	// Add data points to ring buffer
	for _, dp := range metric.DataPoints {
		series.Buffer.Push(dp)
	}
	series.LastSeen = time.Now()
	s.totalInserts++
}

// Query retrieves metrics matching the given request.
func (s *MetricsStore) Query(req *v1.MetricsQueryRequest) (*v1.MetricsQueryResponse, error) {
	if req == nil {
		return &v1.MetricsQueryResponse{
			ErrorMessage: "nil request",
		}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Parse time range
	var startTime, endTime time.Time
	if req.StartTime != nil {
		startTime = req.StartTime.AsTime()
	} else {
		// Default to retention window
		startTime = time.Now().Add(-s.retentionDuration)
	}
	if req.EndTime != nil {
		endTime = req.EndTime.AsTime()
	} else {
		endTime = time.Now()
	}

	// Find candidate series using indexes
	candidates := s.findCandidates(req)

	var results []*v1.OTelMetric
	totalMatched := 0

	for key := range candidates {
		series, exists := s.seriesMap[key]
		if !exists {
			continue
		}

		// Apply metric name filter
		if len(req.MetricNames) > 0 && !containsString(req.MetricNames, series.MetricName) {
			continue
		}

		// Apply label matchers
		if !s.matchLabels(series.Labels, req.LabelMatchers) {
			continue
		}

		totalMatched++

		// Check series limit
		if req.MaxSeries > 0 && len(results) >= int(req.MaxSeries) {
			continue // Count but don't add
		}

		// Extract data points in time range
		dataPoints := series.Buffer.Range(startTime, endTime)
		if len(dataPoints) == 0 {
			continue
		}

		// Apply data point limit
		if req.MaxDataPoints > 0 && len(dataPoints) > int(req.MaxDataPoints) {
			// Keep most recent
			dataPoints = dataPoints[len(dataPoints)-int(req.MaxDataPoints):]
		}

		results = append(results, series.ToProto(dataPoints))
	}

	return &v1.MetricsQueryResponse{
		RequestId:           req.RequestId,
		Metrics:             results,
		TotalSeriesMatched:  int32(totalMatched),
		TotalSeriesReturned: int32(len(results)),
		Truncated:           req.MaxSeries > 0 && totalMatched > int(req.MaxSeries),
	}, nil
}

// Stats returns current store statistics.
func (s *MetricsStore) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return StoreStats{
		SeriesCount:    len(s.seriesMap),
		TotalInserts:   s.totalInserts,
		TotalEvictions: s.totalEvictions,
		DroppedSeries:  s.droppedSeries,
	}
}

// SeriesCount returns the current number of series in the store.
func (s *MetricsStore) SeriesCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.seriesMap)
}

// Clear removes all data from the store.
func (s *MetricsStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seriesMap = make(map[SeriesKey]*SeriesData)
	s.byNamespace = make(map[string]map[SeriesKey]struct{})
	s.byWorkload = make(map[string]map[SeriesKey]struct{})
	s.byPod = make(map[string]map[SeriesKey]struct{})
	s.byMetricName = make(map[string]map[SeriesKey]struct{})
}

// GetRetentionDuration returns the configured retention duration.
func (s *MetricsStore) GetRetentionDuration() time.Duration {
	return s.retentionDuration
}

// GetMaxSeries returns the configured maximum series count.
func (s *MetricsStore) GetMaxSeries() int {
	return s.maxSeries
}
