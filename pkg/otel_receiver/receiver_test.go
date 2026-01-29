package otel_receiver

import (
	"context"
	"testing"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/metrics_store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestOTelReceiverExport(t *testing.T) {
	logger := zap.NewNop()
	podResolver := metrics_store.NewPodWorkloadResolver(logger)
	store := metrics_store.New(logger, podResolver)

	receiver := NewOTelReceiverServer(logger, store, 0) // port 0 for testing

	ctx := context.Background()

	// Create test OTLP metrics request
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "k8s.namespace.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "production"}}},
						{Key: "k8s.deployment.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout-service"}}},
						{Key: "k8s.pod.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout-service-abc123"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "test-scope",
							Version: "1.0.0",
						},
						Metrics: []*metricspb.Metric{
							{
								Name:        "http_requests_total",
								Description: "Total HTTP requests",
								Unit:        "1",
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 100.0},
												Attributes: []*commonpb.KeyValue{
													{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
												},
											},
										},
									},
								},
							},
							{
								Name:        "cpu_usage",
								Description: "CPU usage percentage",
								Unit:        "%",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 45.5},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Call Export
	resp, err := receiver.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Verify metrics were stored
	stats := store.Stats()
	assert.Equal(t, 2, stats.SeriesCount, "Expected 2 series to be stored")

	// Query metrics to verify content
	// Use explicit time range that encompasses the test data points
	startTime := time.Now().Add(-1 * time.Minute)
	endTime := time.Now().Add(1 * time.Minute)
	queryResp, err := store.Query(&v1.MetricsQueryRequest{
		RequestId: "test-query",
		Namespace: "production",
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), queryResp.TotalSeriesMatched)

	// Verify K8s context was extracted
	foundCheckout := false
	for _, m := range queryResp.Metrics {
		if m.Name == "http_requests_total" {
			assert.Equal(t, "production", m.Namespace)
			assert.Equal(t, "checkout-service", m.WorkloadName)
			assert.Equal(t, "Deployment", m.WorkloadKind)
			assert.Equal(t, "checkout-service-abc123", m.PodName)
			assert.Equal(t, "test-scope", m.ScopeName)
			foundCheckout = true
		}
	}
	assert.True(t, foundCheckout, "Expected to find http_requests_total metric")
}

func TestExtractAttributes(t *testing.T) {
	tests := []struct {
		name     string
		input    []*commonpb.KeyValue
		expected map[string]string
	}{
		{
			name:     "empty",
			input:    nil,
			expected: map[string]string{},
		},
		{
			name: "string values",
			input: []*commonpb.KeyValue{
				{Key: "key1", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value1"}}},
				{Key: "key2", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value2"}}},
			},
			expected: map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			name: "int value",
			input: []*commonpb.KeyValue{
				{Key: "count", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
			},
			expected: map[string]string{"count": "42"},
		},
		{
			name: "bool value",
			input: []*commonpb.KeyValue{
				{Key: "enabled", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
			},
			expected: map[string]string{"enabled": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractAttributes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterK8sAttributes(t *testing.T) {
	input := map[string]string{
		"k8s.namespace.name":  "prod",
		"k8s.pod.name":        "my-pod",
		"k8s.deployment.name": "my-deployment",
		"http.method":         "GET",
		"http.status_code":    "200",
		"custom.label":        "value",
	}

	result := filterK8sAttributes(input)

	// K8s attributes should be filtered out
	assert.NotContains(t, result, "k8s.namespace.name")
	assert.NotContains(t, result, "k8s.pod.name")
	assert.NotContains(t, result, "k8s.deployment.name")

	// Other attributes should remain
	assert.Equal(t, "GET", result["http.method"])
	assert.Equal(t, "200", result["http.status_code"])
	assert.Equal(t, "value", result["custom.label"])
}

func TestHistogramConversion(t *testing.T) {
	logger := zap.NewNop()
	podResolver := metrics_store.NewPodWorkloadResolver(logger)
	store := metrics_store.New(logger, podResolver)

	receiver := NewOTelReceiverServer(logger, store, 0)

	ctx := context.Background()

	// Create histogram metrics request
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "k8s.namespace.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "default"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "http_request_duration_seconds",
								Data: &metricspb.Metric_Histogram{
									Histogram: &metricspb.Histogram{
										DataPoints: []*metricspb.HistogramDataPoint{
											{
												TimeUnixNano:   uint64(time.Now().UnixNano()),
												Count:          100,
												Sum:            func() *float64 { v := 45.5; return &v }(),
												ExplicitBounds: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
												BucketCounts:   []uint64{10, 20, 30, 15, 10, 8, 4, 2, 1, 0, 0, 0},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp, err := receiver.Export(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Verify histogram was stored
	queryResp, err := store.Query(&v1.MetricsQueryRequest{
		RequestId:   "test-histogram",
		MetricNames: []string{"http_request_duration_seconds"},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), queryResp.TotalSeriesMatched)

	if len(queryResp.Metrics) > 0 {
		metric := queryResp.Metrics[0]
		assert.Equal(t, v1.OTelMetricType_OTEL_METRIC_TYPE_HISTOGRAM, metric.Type)
		if len(metric.DataPoints) > 0 {
			dp := metric.DataPoints[0]
			assert.Equal(t, uint64(100), dp.Count)
			assert.Equal(t, 45.5, dp.Sum)
			assert.Len(t, dp.BucketBounds, 11)
			assert.Len(t, dp.BucketCounts, 12)
		}
	}
}

func TestReceiverStartStop(t *testing.T) {
	logger := zap.NewNop()
	podResolver := metrics_store.NewPodWorkloadResolver(logger)
	store := metrics_store.New(logger, podResolver)

	// Use port 0 to let OS assign a free port
	receiver := NewOTelReceiverServer(logger, store, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should succeed
	err := receiver.Start(ctx)
	require.NoError(t, err)

	stats := receiver.Stats()
	assert.True(t, stats.Running)

	// Stop should succeed
	receiver.Stop()

	stats = receiver.Stats()
	assert.False(t, stats.Running)
}
