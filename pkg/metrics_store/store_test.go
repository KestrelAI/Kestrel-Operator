package metrics_store

import (
	"testing"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMetricsStore_Insert(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Create a test metric
	metric := &v1.OTelMetric{
		Name:         "http_requests_total",
		Namespace:    "default",
		PodName:      "web-server-abc123",
		WorkloadName: "web-server",
		WorkloadKind: "Deployment",
		Labels: map[string]string{
			"status": "200",
		},
		DataPoints: []*v1.OTelMetricDataPoint{
			{
				Timestamp: timestamppb.New(time.Now()),
				Value:     100.0,
			},
		},
	}

	batch := &v1.OTelMetricBatch{
		Metrics: []*v1.OTelMetric{metric},
	}

	err := store.Insert(batch)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	stats := store.Stats()
	if stats.SeriesCount != 1 {
		t.Errorf("Expected 1 series, got %d", stats.SeriesCount)
	}
	if stats.TotalInserts != 1 {
		t.Errorf("Expected 1 insert, got %d", stats.TotalInserts)
	}
}

func TestMetricsStore_Query_ByNamespace(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Insert metrics in different namespaces
	namespaces := []string{"prod", "staging", "dev"}
	for _, ns := range namespaces {
		metric := &v1.OTelMetric{
			Name:         "cpu_usage",
			Namespace:    ns,
			PodName:      "app-" + ns,
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     50.0,
				},
			},
		}
		store.InsertMetric(metric)
	}

	// Query for prod namespace only
	req := &v1.MetricsQueryRequest{
		RequestId: "test-1",
		Namespace: "prod",
		StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:   timestamppb.New(time.Now().Add(time.Hour)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if resp.TotalSeriesMatched != 1 {
		t.Errorf("Expected 1 matched series, got %d", resp.TotalSeriesMatched)
	}
	if len(resp.Metrics) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(resp.Metrics))
	}
	if resp.Metrics[0].Namespace != "prod" {
		t.Errorf("Expected namespace 'prod', got '%s'", resp.Metrics[0].Namespace)
	}
}

func TestMetricsStore_Query_ByWorkload(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Insert metrics for different workloads
	workloads := []string{"frontend", "backend", "database"}
	for _, wl := range workloads {
		metric := &v1.OTelMetric{
			Name:         "memory_usage",
			Namespace:    "prod",
			PodName:      wl + "-pod",
			WorkloadName: wl,
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     1024.0,
				},
			},
		}
		store.InsertMetric(metric)
	}

	// Query for backend workload
	req := &v1.MetricsQueryRequest{
		RequestId:    "test-2",
		Namespace:    "prod",
		WorkloadName: "backend",
		WorkloadKind: "Deployment",
		StartTime:    timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:      timestamppb.New(time.Now().Add(time.Hour)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if resp.TotalSeriesMatched != 1 {
		t.Errorf("Expected 1 matched series, got %d", resp.TotalSeriesMatched)
	}
	if len(resp.Metrics) != 1 {
		t.Errorf("Expected 1 metric, got %d", len(resp.Metrics))
	}
	if resp.Metrics[0].WorkloadName != "backend" {
		t.Errorf("Expected workload 'backend', got '%s'", resp.Metrics[0].WorkloadName)
	}
}

func TestMetricsStore_Query_ByMetricName(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Insert different metric types
	metricNames := []string{"http_requests_total", "http_request_duration", "cpu_usage"}
	for _, name := range metricNames {
		metric := &v1.OTelMetric{
			Name:         name,
			Namespace:    "prod",
			PodName:      "app-pod",
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     100.0,
				},
			},
		}
		store.InsertMetric(metric)
	}

	// Query for specific metric names
	req := &v1.MetricsQueryRequest{
		RequestId:   "test-3",
		Namespace:   "prod",
		MetricNames: []string{"http_requests_total", "cpu_usage"},
		StartTime:   timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:     timestamppb.New(time.Now().Add(time.Hour)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if resp.TotalSeriesMatched != 2 {
		t.Errorf("Expected 2 matched series, got %d", resp.TotalSeriesMatched)
	}
}

func TestMetricsStore_Query_TimeRange(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	baseTime := time.Now()

	// Insert metrics at different times
	for i := 0; i < 5; i++ {
		metric := &v1.OTelMetric{
			Name:         "requests",
			Namespace:    "prod",
			PodName:      "app-pod",
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute)),
					Value:     float64(i),
				},
			},
		}
		store.InsertMetric(metric)
	}

	// Query for a time range that includes only some points
	req := &v1.MetricsQueryRequest{
		RequestId: "test-4",
		Namespace: "prod",
		StartTime: timestamppb.New(baseTime.Add(time.Minute)),
		EndTime:   timestamppb.New(baseTime.Add(3 * time.Minute)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Metrics) != 1 {
		t.Fatalf("Expected 1 metric series, got %d", len(resp.Metrics))
	}

	// Should have 3 data points (minutes 1, 2, 3)
	if len(resp.Metrics[0].DataPoints) != 3 {
		t.Errorf("Expected 3 data points, got %d", len(resp.Metrics[0].DataPoints))
	}
}

func TestMetricsStore_Query_MaxSeries(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Insert 10 different series
	for i := 0; i < 10; i++ {
		metric := &v1.OTelMetric{
			Name:         "metric",
			Namespace:    "prod",
			PodName:      "pod-" + string(rune('a'+i)),
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     float64(i),
				},
			},
		}
		store.InsertMetric(metric)
	}

	// Query with max series limit
	req := &v1.MetricsQueryRequest{
		RequestId: "test-5",
		Namespace: "prod",
		MaxSeries: 3,
		StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:   timestamppb.New(time.Now().Add(time.Hour)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if resp.TotalSeriesMatched != 10 {
		t.Errorf("Expected 10 total matched, got %d", resp.TotalSeriesMatched)
	}
	if resp.TotalSeriesReturned != 3 {
		t.Errorf("Expected 3 returned, got %d", resp.TotalSeriesReturned)
	}
	if !resp.Truncated {
		t.Error("Expected truncated to be true")
	}
}

func TestMetricsStore_MaxSeriesLimit(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver, WithMaxSeries(5))

	// Try to insert more than max series
	for i := 0; i < 10; i++ {
		metric := &v1.OTelMetric{
			Name:         "metric",
			Namespace:    "prod",
			PodName:      "pod-" + string(rune('a'+i)),
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     float64(i),
				},
			},
		}
		store.InsertMetric(metric)
	}

	stats := store.Stats()
	if stats.SeriesCount != 5 {
		t.Errorf("Expected 5 series (max), got %d", stats.SeriesCount)
	}
	if stats.DroppedSeries != 5 {
		t.Errorf("Expected 5 dropped series, got %d", stats.DroppedSeries)
	}
}

func TestMetricsStore_Clear(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)
	store := New(logger, resolver)

	// Insert some data
	for i := 0; i < 5; i++ {
		metric := &v1.OTelMetric{
			Name:         "metric",
			Namespace:    "prod",
			PodName:      "pod-" + string(rune('a'+i)),
			WorkloadName: "app",
			WorkloadKind: "Deployment",
			DataPoints: []*v1.OTelMetricDataPoint{
				{
					Timestamp: timestamppb.New(time.Now()),
					Value:     float64(i),
				},
			},
		}
		store.InsertMetric(metric)
	}

	if store.SeriesCount() != 5 {
		t.Errorf("Expected 5 series before clear, got %d", store.SeriesCount())
	}

	store.Clear()

	if store.SeriesCount() != 0 {
		t.Errorf("Expected 0 series after clear, got %d", store.SeriesCount())
	}
}

func TestMetricsStore_PodResolver(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod
	pod := &v1.Pod{
		Name:      "web-server-7f9b4c8d5-x2k9j",
		Namespace: "prod",
		Uid:       "pod-uid-123",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "ReplicaSet",
				Name:       "web-server-7f9b4c8d5",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	store := New(logger, resolver)

	// Insert a metric without workload info
	metric := &v1.OTelMetric{
		Name:      "cpu_usage",
		Namespace: "prod",
		PodName:   "web-server-7f9b4c8d5-x2k9j",
		// WorkloadName and WorkloadKind not set
		DataPoints: []*v1.OTelMetricDataPoint{
			{
				Timestamp: timestamppb.New(time.Now()),
				Value:     50.0,
			},
		},
	}
	store.InsertMetric(metric)

	// Query by workload - should find the metric because resolver filled in workload info
	req := &v1.MetricsQueryRequest{
		RequestId:    "test-resolver",
		Namespace:    "prod",
		WorkloadName: "web-server",
		WorkloadKind: "Deployment",
		StartTime:    timestamppb.New(time.Now().Add(-time.Hour)),
		EndTime:      timestamppb.New(time.Now().Add(time.Hour)),
	}

	resp, err := store.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if resp.TotalSeriesMatched != 1 {
		t.Errorf("Expected 1 matched series (resolved via pod resolver), got %d", resp.TotalSeriesMatched)
	}
}
