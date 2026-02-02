// Package otel_receiver implements an OTLP gRPC receiver for metrics.
// It accepts metrics from customer OpenTelemetry Collectors and feeds them
// into the local MetricsStore for querying by the Kestrel agents service.
package otel_receiver

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/metrics_store"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// OTelReceiverServer implements the OTLP MetricsService for receiving metrics
// from customer OpenTelemetry Collectors.
type OTelReceiverServer struct {
	colmetricspb.UnimplementedMetricsServiceServer

	logger       *zap.Logger
	metricsStore *metrics_store.MetricsStore
	port         int

	server   *grpc.Server
	listener net.Listener

	mu      sync.Mutex
	running bool
	doneCh  chan struct{} // Signals shutdown to the monitoring goroutine

	// Stats
	totalReceived   int64
	totalDataPoints int64
	lastReceiveTime time.Time
}

// NewOTelReceiverServer creates a new OTLP metrics receiver.
func NewOTelReceiverServer(
	logger *zap.Logger,
	metricsStore *metrics_store.MetricsStore,
	port int,
) *OTelReceiverServer {
	return &OTelReceiverServer{
		logger:       logger,
		metricsStore: metricsStore,
		port:         port,
	}
}

// Start starts the OTLP gRPC server.
func (s *OTelReceiverServer) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("OTEL receiver already running")
	}

	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.server = grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(s.server, s)
	s.doneCh = make(chan struct{})
	s.running = true
	s.mu.Unlock()

	s.logger.Info("Starting OTEL metrics receiver",
		zap.Int("port", s.port),
		zap.String("address", addr))

	// Start serving in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := s.server.Serve(listener); err != nil {
			// Ignore the normal shutdown sentinel error
			if err == grpc.ErrServerStopped {
				return
			}
			errChan <- err
		}
	}()

	// Wait for context cancellation, server error, or explicit shutdown
	go func() {
		select {
		case <-ctx.Done():
			s.logger.Info("Shutting down OTEL receiver due to context cancellation")
			s.Stop()
		case err := <-errChan:
			// Only log actual errors, not shutdown signals
			if err != nil && err != grpc.ErrServerStopped {
				s.logger.Error("OTEL receiver server error", zap.Error(err))
			}
		case <-s.doneCh:
			// Explicit shutdown via Stop() - goroutine exits cleanly
		}
	}()

	return nil
}

// Stop gracefully stops the OTLP gRPC server.
// It copies server and listener references under the lock, then releases the lock
// before calling blocking operations to avoid deadlock with in-flight Export() RPCs.
func (s *OTelReceiverServer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}

	// Copy references and update state under lock
	server := s.server
	listener := s.listener
	doneCh := s.doneCh
	s.running = false
	s.mu.Unlock()

	// Perform blocking operations without holding the lock
	// This prevents deadlock with Export() RPCs that need the lock to update stats
	if server != nil {
		server.GracefulStop()
	}
	if listener != nil {
		listener.Close()
	}
	// Signal the monitoring goroutine to exit
	if doneCh != nil {
		close(doneCh)
	}
	s.logger.Info("OTEL receiver stopped")
}

// Export implements the OTLP MetricsService Export RPC.
// This is called by customer OTEL Collectors to push metrics.
func (s *OTelReceiverServer) Export(
	ctx context.Context,
	req *colmetricspb.ExportMetricsServiceRequest,
) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if req == nil || len(req.ResourceMetrics) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	receivedAt := time.Now()
	batch := s.convertToInternalBatch(req, receivedAt)

	if len(batch.Metrics) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	// Insert into metrics store
	if err := s.metricsStore.Insert(batch); err != nil {
		s.logger.Error("Failed to insert metrics batch",
			zap.Error(err),
			zap.Int("metric_count", len(batch.Metrics)))
		return nil, status.Errorf(codes.Internal, "failed to store metrics: %v", err)
	}

	// Update stats
	s.mu.Lock()
	s.totalReceived += int64(len(batch.Metrics))
	s.totalDataPoints += int64(batch.MetricCount)
	s.lastReceiveTime = receivedAt
	s.mu.Unlock()

	s.logger.Debug("Received and stored OTEL metrics",
		zap.Int("metrics", len(batch.Metrics)),
		zap.Int32("data_points", batch.MetricCount))

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// convertToInternalBatch converts OTLP metrics to internal OTelMetricBatch format.
func (s *OTelReceiverServer) convertToInternalBatch(
	req *colmetricspb.ExportMetricsServiceRequest,
	receivedAt time.Time,
) *v1.OTelMetricBatch {
	var metrics []*v1.OTelMetric
	var totalDataPoints int32

	for _, rm := range req.ResourceMetrics {
		// Extract resource attributes (K8s context often comes from here)
		// Handle nil Resource safely
		var resourceAttrs map[string]string
		if rm.Resource != nil {
			resourceAttrs = extractAttributes(rm.Resource.GetAttributes())
		} else {
			resourceAttrs = make(map[string]string)
		}

		for _, sm := range rm.ScopeMetrics {
			scopeName := ""
			scopeVersion := ""
			if sm.Scope != nil {
				scopeName = sm.Scope.Name
				scopeVersion = sm.Scope.Version
			}

			for _, m := range sm.Metrics {
				converted := s.convertMetric(m, resourceAttrs, scopeName, scopeVersion)
				if converted != nil {
					metrics = append(metrics, converted...)
					for _, cm := range converted {
						totalDataPoints += int32(len(cm.DataPoints))
					}
				}
			}
		}
	}

	return &v1.OTelMetricBatch{
		Metrics:     metrics,
		ReceivedAt:  timestamppb.New(receivedAt),
		MetricCount: totalDataPoints,
	}
}

// convertMetric converts a single OTLP metric to internal format.
// Returns multiple OTelMetric instances because a single OTLP metric can have
// multiple data points with different label combinations (different series).
func (s *OTelReceiverServer) convertMetric(
	m *metricspb.Metric,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if m == nil {
		return nil
	}

	var results []*v1.OTelMetric

	switch data := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		results = s.convertGauge(m, data.Gauge, resourceAttrs, scopeName, scopeVersion)

	case *metricspb.Metric_Sum:
		results = s.convertSum(m, data.Sum, resourceAttrs, scopeName, scopeVersion)

	case *metricspb.Metric_Histogram:
		results = s.convertHistogram(m, data.Histogram, resourceAttrs, scopeName, scopeVersion)

	case *metricspb.Metric_ExponentialHistogram:
		results = s.convertExponentialHistogram(m, data.ExponentialHistogram, resourceAttrs, scopeName, scopeVersion)

	case *metricspb.Metric_Summary:
		results = s.convertSummary(m, data.Summary, resourceAttrs, scopeName, scopeVersion)
	}

	return results
}

// convertGauge converts OTLP Gauge metrics.
func (s *OTelReceiverServer) convertGauge(
	m *metricspb.Metric,
	gauge *metricspb.Gauge,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if gauge == nil {
		return nil
	}

	// Group data points by their attributes (each unique attribute set is a series)
	seriesMap := make(map[string]*v1.OTelMetric)

	for _, dp := range gauge.DataPoints {
		pointAttrs := extractAttributes(dp.Attributes)
		allAttrs := mergeAttributes(resourceAttrs, pointAttrs)

		seriesKey := computeSeriesKeyFromAttrs(m.Name, allAttrs)

		metric, exists := seriesMap[seriesKey]
		if !exists {
			metric = s.createMetricFromAttrs(m, v1.OTelMetricType_OTEL_METRIC_TYPE_GAUGE, allAttrs, scopeName, scopeVersion)
			seriesMap[seriesKey] = metric
		}

		dataPoint := &v1.OTelMetricDataPoint{
			Timestamp:      timestamppb.New(time.Unix(0, int64(dp.TimeUnixNano))),
			StartTimestamp: timestamppb.New(time.Unix(0, int64(dp.StartTimeUnixNano))),
		}

		switch v := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsDouble:
			dataPoint.Value = v.AsDouble
		case *metricspb.NumberDataPoint_AsInt:
			dataPoint.Value = float64(v.AsInt)
		}

		metric.DataPoints = append(metric.DataPoints, dataPoint)
	}

	return mapValues(seriesMap)
}

// convertSum converts OTLP Sum metrics (counters).
func (s *OTelReceiverServer) convertSum(
	m *metricspb.Metric,
	sum *metricspb.Sum,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if sum == nil {
		return nil
	}

	seriesMap := make(map[string]*v1.OTelMetric)

	for _, dp := range sum.DataPoints {
		pointAttrs := extractAttributes(dp.Attributes)
		allAttrs := mergeAttributes(resourceAttrs, pointAttrs)

		seriesKey := computeSeriesKeyFromAttrs(m.Name, allAttrs)

		metric, exists := seriesMap[seriesKey]
		if !exists {
			metric = s.createMetricFromAttrs(m, v1.OTelMetricType_OTEL_METRIC_TYPE_SUM, allAttrs, scopeName, scopeVersion)
			seriesMap[seriesKey] = metric
		}

		dataPoint := &v1.OTelMetricDataPoint{
			Timestamp:      timestamppb.New(time.Unix(0, int64(dp.TimeUnixNano))),
			StartTimestamp: timestamppb.New(time.Unix(0, int64(dp.StartTimeUnixNano))),
		}

		switch v := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsDouble:
			dataPoint.Value = v.AsDouble
		case *metricspb.NumberDataPoint_AsInt:
			dataPoint.Value = float64(v.AsInt)
		}

		metric.DataPoints = append(metric.DataPoints, dataPoint)
	}

	return mapValues(seriesMap)
}

// convertHistogram converts OTLP Histogram metrics.
func (s *OTelReceiverServer) convertHistogram(
	m *metricspb.Metric,
	histogram *metricspb.Histogram,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if histogram == nil {
		return nil
	}

	seriesMap := make(map[string]*v1.OTelMetric)

	for _, dp := range histogram.DataPoints {
		pointAttrs := extractAttributes(dp.Attributes)
		allAttrs := mergeAttributes(resourceAttrs, pointAttrs)

		seriesKey := computeSeriesKeyFromAttrs(m.Name, allAttrs)

		metric, exists := seriesMap[seriesKey]
		if !exists {
			metric = s.createMetricFromAttrs(m, v1.OTelMetricType_OTEL_METRIC_TYPE_HISTOGRAM, allAttrs, scopeName, scopeVersion)
			seriesMap[seriesKey] = metric
		}

		dataPoint := &v1.OTelMetricDataPoint{
			Timestamp:      timestamppb.New(time.Unix(0, int64(dp.TimeUnixNano))),
			StartTimestamp: timestamppb.New(time.Unix(0, int64(dp.StartTimeUnixNano))),
			BucketBounds:   dp.ExplicitBounds,
			BucketCounts:   dp.BucketCounts,
			Count:          dp.Count,
			Sum:            dp.GetSum(),
		}

		if dp.Min != nil {
			dataPoint.Min = *dp.Min
		}
		if dp.Max != nil {
			dataPoint.Max = *dp.Max
		}

		metric.DataPoints = append(metric.DataPoints, dataPoint)
	}

	return mapValues(seriesMap)
}

// convertExponentialHistogram converts OTLP ExponentialHistogram metrics.
func (s *OTelReceiverServer) convertExponentialHistogram(
	m *metricspb.Metric,
	histogram *metricspb.ExponentialHistogram,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if histogram == nil {
		return nil
	}

	seriesMap := make(map[string]*v1.OTelMetric)

	for _, dp := range histogram.DataPoints {
		pointAttrs := extractAttributes(dp.Attributes)
		allAttrs := mergeAttributes(resourceAttrs, pointAttrs)

		seriesKey := computeSeriesKeyFromAttrs(m.Name, allAttrs)

		metric, exists := seriesMap[seriesKey]
		if !exists {
			metric = s.createMetricFromAttrs(m, v1.OTelMetricType_OTEL_METRIC_TYPE_EXPONENTIAL_HISTOGRAM, allAttrs, scopeName, scopeVersion)
			seriesMap[seriesKey] = metric
		}

		dataPoint := &v1.OTelMetricDataPoint{
			Timestamp:      timestamppb.New(time.Unix(0, int64(dp.TimeUnixNano))),
			StartTimestamp: timestamppb.New(time.Unix(0, int64(dp.StartTimeUnixNano))),
			Count:          dp.Count,
			Sum:            dp.GetSum(),
		}

		if dp.Min != nil {
			dataPoint.Min = *dp.Min
		}
		if dp.Max != nil {
			dataPoint.Max = *dp.Max
		}

		// Populate exponential histogram specific data
		expHistData := &v1.ExponentialHistogramData{
			Scale:         dp.Scale,
			ZeroCount:     dp.ZeroCount,
			ZeroThreshold: dp.ZeroThreshold,
		}

		// Convert positive buckets
		if dp.Positive != nil {
			expHistData.Positive = &v1.ExponentialHistogramBuckets{
				Offset:       dp.Positive.Offset,
				BucketCounts: dp.Positive.BucketCounts,
			}
		}

		// Convert negative buckets
		if dp.Negative != nil {
			expHistData.Negative = &v1.ExponentialHistogramBuckets{
				Offset:       dp.Negative.Offset,
				BucketCounts: dp.Negative.BucketCounts,
			}
		}

		dataPoint.ExponentialHistogram = expHistData
		metric.DataPoints = append(metric.DataPoints, dataPoint)
	}

	return mapValues(seriesMap)
}

// convertSummary converts OTLP Summary metrics.
func (s *OTelReceiverServer) convertSummary(
	m *metricspb.Metric,
	summary *metricspb.Summary,
	resourceAttrs map[string]string,
	scopeName, scopeVersion string,
) []*v1.OTelMetric {
	if summary == nil {
		return nil
	}

	seriesMap := make(map[string]*v1.OTelMetric)

	for _, dp := range summary.DataPoints {
		pointAttrs := extractAttributes(dp.Attributes)
		allAttrs := mergeAttributes(resourceAttrs, pointAttrs)

		seriesKey := computeSeriesKeyFromAttrs(m.Name, allAttrs)

		metric, exists := seriesMap[seriesKey]
		if !exists {
			metric = s.createMetricFromAttrs(m, v1.OTelMetricType_OTEL_METRIC_TYPE_SUMMARY, allAttrs, scopeName, scopeVersion)
			seriesMap[seriesKey] = metric
		}

		dataPoint := &v1.OTelMetricDataPoint{
			Timestamp:      timestamppb.New(time.Unix(0, int64(dp.TimeUnixNano))),
			StartTimestamp: timestamppb.New(time.Unix(0, int64(dp.StartTimeUnixNano))),
			Count:          dp.Count,
			Sum:            dp.Sum,
		}

		// Populate summary specific data with quantile values
		if len(dp.QuantileValues) > 0 {
			summaryData := &v1.SummaryData{
				QuantileValues: make([]*v1.QuantileValue, 0, len(dp.QuantileValues)),
			}
			for _, qv := range dp.QuantileValues {
				summaryData.QuantileValues = append(summaryData.QuantileValues, &v1.QuantileValue{
					Quantile: qv.Quantile,
					Value:    qv.Value,
				})
			}
			dataPoint.Summary = summaryData
		}

		metric.DataPoints = append(metric.DataPoints, dataPoint)
	}

	return mapValues(seriesMap)
}

// createMetricFromAttrs creates an OTelMetric with K8s context extracted from attributes.
func (s *OTelReceiverServer) createMetricFromAttrs(
	m *metricspb.Metric,
	metricType v1.OTelMetricType,
	attrs map[string]string,
	scopeName, scopeVersion string,
) *v1.OTelMetric {
	// Extract K8s semantic convention attributes
	namespace := extractK8sAttribute(attrs, "k8s.namespace.name", "namespace")
	podName := extractK8sAttribute(attrs, "k8s.pod.name", "pod")
	containerName := extractK8sAttribute(attrs, "k8s.container.name", "container")
	nodeName := extractK8sAttribute(attrs, "k8s.node.name", "node")

	// Extract workload - check various semantic conventions
	workloadName := ""
	workloadKind := ""

	if name := extractK8sAttribute(attrs, "k8s.deployment.name", ""); name != "" {
		workloadName = name
		workloadKind = "Deployment"
	} else if name := extractK8sAttribute(attrs, "k8s.statefulset.name", ""); name != "" {
		workloadName = name
		workloadKind = "StatefulSet"
	} else if name := extractK8sAttribute(attrs, "k8s.daemonset.name", ""); name != "" {
		workloadName = name
		workloadKind = "DaemonSet"
	} else if name := extractK8sAttribute(attrs, "k8s.replicaset.name", ""); name != "" {
		workloadName = name
		workloadKind = "ReplicaSet"
	} else if name := extractK8sAttribute(attrs, "k8s.job.name", ""); name != "" {
		workloadName = name
		workloadKind = "Job"
	} else if name := extractK8sAttribute(attrs, "k8s.cronjob.name", ""); name != "" {
		workloadName = name
		workloadKind = "CronJob"
	}

	// Remove K8s context attributes from labels (they're stored separately)
	labels := filterK8sAttributes(attrs)

	return &v1.OTelMetric{
		Name:          m.Name,
		Description:   m.Description,
		Unit:          m.Unit,
		Type:          metricType,
		Namespace:     namespace,
		WorkloadName:  workloadName,
		WorkloadKind:  workloadKind,
		PodName:       podName,
		ContainerName: containerName,
		NodeName:      nodeName,
		Labels:        labels,
		ScopeName:     scopeName,
		ScopeVersion:  scopeVersion,
	}
}

// extractAttributes converts OTLP KeyValue slice to map.
// Uses type switch on the protobuf oneof to properly handle zero/false values.
func extractAttributes(kvs []*commonpb.KeyValue) map[string]string {
	attrs := make(map[string]string)
	for _, kv := range kvs {
		if kv == nil || kv.Value == nil {
			continue
		}
		// Use type switch on the oneof to preserve zero/false values
		switch v := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			attrs[kv.Key] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			attrs[kv.Key] = fmt.Sprintf("%d", v.IntValue)
		case *commonpb.AnyValue_BoolValue:
			if v.BoolValue {
				attrs[kv.Key] = "true"
			} else {
				attrs[kv.Key] = "false"
			}
		case *commonpb.AnyValue_DoubleValue:
			attrs[kv.Key] = fmt.Sprintf("%f", v.DoubleValue)
		}
	}
	return attrs
}

// mergeAttributes merges two attribute maps, with pointAttrs taking precedence.
func mergeAttributes(resourceAttrs, pointAttrs map[string]string) map[string]string {
	result := make(map[string]string, len(resourceAttrs)+len(pointAttrs))
	for k, v := range resourceAttrs {
		result[k] = v
	}
	for k, v := range pointAttrs {
		result[k] = v
	}
	return result
}

// extractK8sAttribute extracts a K8s attribute, checking both semantic convention and short names.
func extractK8sAttribute(attrs map[string]string, semConvKey, shortKey string) string {
	if v, ok := attrs[semConvKey]; ok {
		return v
	}
	if shortKey != "" {
		if v, ok := attrs[shortKey]; ok {
			return v
		}
	}
	return ""
}

// filterK8sAttributes removes K8s context attributes from the labels map.
func filterK8sAttributes(attrs map[string]string) map[string]string {
	k8sKeys := []string{
		"k8s.namespace.name", "namespace",
		"k8s.pod.name", "pod",
		"k8s.pod.uid",
		"k8s.container.name", "container",
		"k8s.node.name", "node",
		"k8s.deployment.name",
		"k8s.statefulset.name",
		"k8s.daemonset.name",
		"k8s.replicaset.name",
		"k8s.job.name",
		"k8s.cronjob.name",
	}

	result := make(map[string]string)
	for k, v := range attrs {
		isK8s := false
		for _, k8sKey := range k8sKeys {
			if k == k8sKey {
				isK8s = true
				break
			}
		}
		if !isK8s {
			result[k] = v
		}
	}
	return result
}

// computeSeriesKeyFromAttrs creates a unique key for a series based on metric name and attributes.
// Keys are sorted to ensure deterministic output regardless of map iteration order.
// Uses length-prefixed encoding to avoid collisions when metric names or attribute keys/values
// contain delimiter characters.
func computeSeriesKeyFromAttrs(metricName string, attrs map[string]string) string {
	if len(attrs) == 0 {
		return metricName
	}

	// Collect and sort keys for deterministic ordering
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build the series key using length-prefixed encoding to prevent collisions.
	// Format: <metricNameLen>:<metricName><keyLen>:<key><valueLen>:<value>...
	// This ensures distinct series cannot produce identical keys even if they
	// contain special characters like '|', '=', or ':'.
	var builder strings.Builder
	builder.Grow(len(metricName) + len(attrs)*20) // Pre-allocate reasonable capacity

	// Write metric name with length prefix
	fmt.Fprintf(&builder, "%d:%s", len(metricName), metricName)

	// Write each key-value pair with length prefixes
	for _, k := range keys {
		v := attrs[k]
		fmt.Fprintf(&builder, "%d:%s%d:%s", len(k), k, len(v), v)
	}

	return builder.String()
}

// mapValues extracts values from a map into a slice.
func mapValues(m map[string]*v1.OTelMetric) []*v1.OTelMetric {
	result := make([]*v1.OTelMetric, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}
	return result
}

// Stats returns receiver statistics.
func (s *OTelReceiverServer) Stats() ReceiverStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return ReceiverStats{
		TotalReceived:   s.totalReceived,
		TotalDataPoints: s.totalDataPoints,
		LastReceiveTime: s.lastReceiveTime,
		Running:         s.running,
	}
}

// ReceiverStats contains statistics about the OTEL receiver.
type ReceiverStats struct {
	TotalReceived   int64
	TotalDataPoints int64
	LastReceiveTime time.Time
	Running         bool
}
