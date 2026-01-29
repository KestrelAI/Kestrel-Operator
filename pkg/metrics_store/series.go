package metrics_store

import (
	"sort"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"github.com/cespare/xxhash/v2"
)

// SeriesKey is a hash of the metric name and labels.
type SeriesKey uint64

// SeriesData holds metadata and data points for a single series.
type SeriesData struct {
	Key         SeriesKey
	MetricName  string
	Description string
	Unit        string
	Type        v1.OTelMetricType

	// Kubernetes context
	Namespace     string
	WorkloadName  string
	WorkloadKind  string
	PodName       string
	ContainerName string
	NodeName      string

	// Labels (excluding K8s context)
	Labels map[string]string

	// Instrumentation scope
	ScopeName    string
	ScopeVersion string

	// Time-series data
	Buffer *RingBuffer

	// Timestamps for eviction
	FirstSeen time.Time
	LastSeen  time.Time
}

// computeSeriesKey computes a unique key for a metric based on its identity.
// The key is a hash of the metric name and all its labels (sorted for consistency).
func computeSeriesKey(metric *v1.OTelMetric) SeriesKey {
	// Build canonical string representation
	// Format: "name|key1=val1|key2=val2|..."
	canonical := metric.Name

	// Add K8s context to the key for uniqueness
	canonical += "|ns=" + metric.Namespace
	canonical += "|pod=" + metric.PodName
	canonical += "|wl=" + metric.WorkloadName
	canonical += "|wlk=" + metric.WorkloadKind
	canonical += "|cn=" + metric.ContainerName
	canonical += "|node=" + metric.NodeName

	// Sort and add labels for consistent hashing
	if len(metric.Labels) > 0 {
		sortedLabels := sortLabels(metric.Labels)
		for _, kv := range sortedLabels {
			canonical += "|" + kv.key + "=" + kv.value
		}
	}

	// Hash to fixed-size key using xxhash (fast, non-cryptographic)
	return SeriesKey(xxhash.Sum64String(canonical))
}

// labelKV represents a key-value pair for sorting.
type labelKV struct {
	key   string
	value string
}

// sortLabels sorts labels by key for consistent hashing.
func sortLabels(labels map[string]string) []labelKV {
	result := make([]labelKV, 0, len(labels))
	for k, v := range labels {
		result = append(result, labelKV{key: k, value: v})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].key < result[j].key
	})
	return result
}

// createSeriesFromMetric creates a new SeriesData from a metric.
func createSeriesFromMetric(key SeriesKey, metric *v1.OTelMetric, ringBufferSize int) *SeriesData {
	// Copy labels to avoid aliasing
	labels := make(map[string]string, len(metric.Labels))
	for k, v := range metric.Labels {
		labels[k] = v
	}

	now := time.Now()
	return &SeriesData{
		Key:           key,
		MetricName:    metric.Name,
		Description:   metric.Description,
		Unit:          metric.Unit,
		Type:          metric.Type,
		Namespace:     metric.Namespace,
		WorkloadName:  metric.WorkloadName,
		WorkloadKind:  metric.WorkloadKind,
		PodName:       metric.PodName,
		ContainerName: metric.ContainerName,
		NodeName:      metric.NodeName,
		Labels:        labels,
		ScopeName:     metric.ScopeName,
		ScopeVersion:  metric.ScopeVersion,
		Buffer:        NewRingBuffer(ringBufferSize),
		FirstSeen:     now,
		LastSeen:      now,
	}
}

// ToProto converts a SeriesData to a protobuf OTelMetric with the given data points.
func (s *SeriesData) ToProto(dataPoints []*v1.OTelMetricDataPoint) *v1.OTelMetric {
	// Copy labels to avoid aliasing
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		labels[k] = v
	}

	return &v1.OTelMetric{
		Name:          s.MetricName,
		Description:   s.Description,
		Unit:          s.Unit,
		Type:          s.Type,
		Namespace:     s.Namespace,
		WorkloadName:  s.WorkloadName,
		WorkloadKind:  s.WorkloadKind,
		PodName:       s.PodName,
		ContainerName: s.ContainerName,
		NodeName:      s.NodeName,
		Labels:        labels,
		DataPoints:    dataPoints,
		ScopeName:     s.ScopeName,
		ScopeVersion:  s.ScopeVersion,
	}
}
