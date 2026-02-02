package metrics_store

import (
	"fmt"
	"strings"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

// addToIndexes adds a series to all secondary indexes.
func (s *MetricsStore) addToIndexes(series *SeriesData) {
	key := series.Key

	// Index by namespace
	if series.Namespace != "" {
		if s.byNamespace[series.Namespace] == nil {
			s.byNamespace[series.Namespace] = make(map[SeriesKey]struct{})
		}
		s.byNamespace[series.Namespace][key] = struct{}{}
	}

	// Index by workload
	if series.WorkloadName != "" {
		workloadKey := s.workloadIndexKey(series.Namespace, series.WorkloadKind, series.WorkloadName)
		if s.byWorkload[workloadKey] == nil {
			s.byWorkload[workloadKey] = make(map[SeriesKey]struct{})
		}
		s.byWorkload[workloadKey][key] = struct{}{}
	}

	// Index by pod
	if series.PodName != "" {
		podKey := s.podIndexKey(series.Namespace, series.PodName)
		if s.byPod[podKey] == nil {
			s.byPod[podKey] = make(map[SeriesKey]struct{})
		}
		s.byPod[podKey][key] = struct{}{}
	}

	// Index by metric name
	if series.MetricName != "" {
		if s.byMetricName[series.MetricName] == nil {
			s.byMetricName[series.MetricName] = make(map[SeriesKey]struct{})
		}
		s.byMetricName[series.MetricName][key] = struct{}{}
	}
}

// removeFromIndexes removes a series from all secondary indexes.
func (s *MetricsStore) removeFromIndexes(series *SeriesData) {
	key := series.Key

	// Remove from namespace index
	if series.Namespace != "" {
		delete(s.byNamespace[series.Namespace], key)
		if len(s.byNamespace[series.Namespace]) == 0 {
			delete(s.byNamespace, series.Namespace)
		}
	}

	// Remove from workload index
	if series.WorkloadName != "" {
		workloadKey := s.workloadIndexKey(series.Namespace, series.WorkloadKind, series.WorkloadName)
		delete(s.byWorkload[workloadKey], key)
		if len(s.byWorkload[workloadKey]) == 0 {
			delete(s.byWorkload, workloadKey)
		}
	}

	// Remove from pod index
	if series.PodName != "" {
		podKey := s.podIndexKey(series.Namespace, series.PodName)
		delete(s.byPod[podKey], key)
		if len(s.byPod[podKey]) == 0 {
			delete(s.byPod, podKey)
		}
	}

	// Remove from metric name index
	if series.MetricName != "" {
		delete(s.byMetricName[series.MetricName], key)
		if len(s.byMetricName[series.MetricName]) == 0 {
			delete(s.byMetricName, series.MetricName)
		}
	}
}

// findCandidates uses indexes to find series matching the query filters.
func (s *MetricsStore) findCandidates(req *v1.MetricsQueryRequest) map[SeriesKey]struct{} {
	var candidates map[SeriesKey]struct{}

	// Start with most specific filter available
	if req.PodName != "" {
		if req.Namespace != "" {
			// Use index lookup when namespace is provided
			podKey := s.podIndexKey(req.Namespace, req.PodName)
			candidates = s.copyKeySet(s.byPod[podKey])
		} else {
			// Namespace is empty but PodName is set - perform full scan to find matching pods
			// across all namespaces since index keys are namespace-qualified.
			// NOTE: This may be slow with large series counts; consider requiring namespace.
			s.logger.Debug("Query performing full scan: namespace empty with pod filter",
				zap.String("pod_name", req.PodName),
				zap.Int("series_count", len(s.seriesMap)))
			candidates = make(map[SeriesKey]struct{})
			for key, series := range s.seriesMap {
				if series.PodName == req.PodName {
					candidates[key] = struct{}{}
				}
			}
		}
	} else if req.WorkloadName != "" {
		if req.Namespace != "" {
			// Use index lookup when namespace is provided
			workloadKey := s.workloadIndexKey(req.Namespace, req.WorkloadKind, req.WorkloadName)
			candidates = s.copyKeySet(s.byWorkload[workloadKey])
		} else {
			// Namespace is empty but WorkloadName is set - perform full scan to find matching
			// workloads across all namespaces since index keys are namespace-qualified.
			// NOTE: This may be slow with large series counts; consider requiring namespace.
			s.logger.Debug("Query performing full scan: namespace empty with workload filter",
				zap.String("workload_name", req.WorkloadName),
				zap.String("workload_kind", req.WorkloadKind),
				zap.Int("series_count", len(s.seriesMap)))
			candidates = make(map[SeriesKey]struct{})
			reqKind := req.WorkloadKind
			if reqKind == "" {
				reqKind = "Deployment" // Default assumption, matching workloadIndexKey behavior
			}
			for key, series := range s.seriesMap {
				seriesKind := series.WorkloadKind
				if seriesKind == "" {
					seriesKind = "Deployment"
				}
				if series.WorkloadName == req.WorkloadName && seriesKind == reqKind {
					candidates[key] = struct{}{}
				}
			}
		}
	} else if req.Namespace != "" {
		candidates = s.copyKeySet(s.byNamespace[req.Namespace])
	} else {
		// No filter - return all series keys
		candidates = make(map[SeriesKey]struct{}, len(s.seriesMap))
		for key := range s.seriesMap {
			candidates[key] = struct{}{}
		}
	}

	// Intersect with pod name prefix if specified
	if req.PodNamePrefix != "" && candidates != nil {
		s.filterByPodPrefix(candidates, req.Namespace, req.PodNamePrefix)
	}

	return candidates
}

// workloadIndexKey constructs a key for the workload index.
func (s *MetricsStore) workloadIndexKey(namespace, kind, name string) string {
	if kind == "" {
		kind = "Deployment" // Default assumption
	}
	return fmt.Sprintf("%s/%s/%s", namespace, kind, name)
}

// podIndexKey constructs a key for the pod index.
func (s *MetricsStore) podIndexKey(namespace, podName string) string {
	return fmt.Sprintf("%s/%s", namespace, podName)
}

// copyKeySet creates a copy of a key set.
func (s *MetricsStore) copyKeySet(src map[SeriesKey]struct{}) map[SeriesKey]struct{} {
	if src == nil {
		return nil
	}
	dst := make(map[SeriesKey]struct{}, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

// filterByPodPrefix filters candidates by pod name prefix.
func (s *MetricsStore) filterByPodPrefix(candidates map[SeriesKey]struct{}, namespace, prefix string) {
	for key := range candidates {
		series := s.seriesMap[key]
		if series == nil {
			delete(candidates, key)
			continue
		}
		// If namespace filter is provided, check it
		if namespace != "" && series.Namespace != namespace {
			delete(candidates, key)
			continue
		}
		// Check pod name prefix
		if !strings.HasPrefix(series.PodName, prefix) {
			delete(candidates, key)
		}
	}
}

// matchLabels checks if a series' labels match the given matchers.
func (s *MetricsStore) matchLabels(seriesLabels, matchers map[string]string) bool {
	if len(matchers) == 0 {
		return true
	}
	for k, v := range matchers {
		if seriesLabels[k] != v {
			return false
		}
	}
	return true
}

// containsString checks if a string slice contains a specific string.
func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
