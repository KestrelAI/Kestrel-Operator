package trivy_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// trivy-operator report CRDs (aquasecurity.github.io/v1alpha1). All are
// namespaced except ClusterComplianceReport.
var (
	vulnerabilityReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "vulnerabilityreports",
	}
	configAuditReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "configauditreports",
	}
	exposedSecretReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "exposedsecretreports",
	}
	rbacAssessmentReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "rbacassessmentreports",
	}
	infraAssessmentReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "infraassessmentreports",
	}
	clusterComplianceReportGVR = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "clustercompliancereports",
	}
)

// Well-known labels trivy-operator stamps on every report, resolving the
// scanned workload (owner) and container.
const (
	labelResourceKind = "trivy-operator.resource.kind"
	labelResourceName = "trivy-operator.resource.name"
	labelContainer    = "trivy-operator.container.name"
)

// TrivyExecutor executes Trivy operations against trivy-operator report CRs
// via the Kubernetes API using the dynamic client (trivy-operator has no
// standalone API server; reports ARE the API).
//
// RBAC requirements for the operator's ServiceAccount:
//   - aquasecurity.github.io vulnerabilityreports: get, list, watch, delete (rescan)
//   - aquasecurity.github.io configauditreports, exposedsecretreports,
//     rbacassessmentreports, infraassessmentreports, clustercompliancereports:
//     get, list, watch
type TrivyExecutor struct {
	Logger  *zap.Logger
	Dynamic dynamic.Interface

	mu        sync.RWMutex
	available bool
	probed    bool
}

func NewTrivyExecutor(logger *zap.Logger, dynamicClient dynamic.Interface) *TrivyExecutor {
	return &TrivyExecutor{
		Logger:  logger,
		Dynamic: dynamicClient,
	}
}

// trivyParams is the JSON payload carried in DatadogQueryRequest.json_body.
type trivyParams struct {
	// Workload is the scanned workload name ("kind/name" also accepted, e.g.
	// "deployment/nginx").
	Workload string `json:"workload,omitempty"`
	// WorkloadKind scopes workload matching (Deployment, StatefulSet, ...).
	WorkloadKind string `json:"workload_kind,omitempty"`
	// Namespace scopes the operation to one namespace.
	Namespace string `json:"namespace,omitempty"`
	// Severity filters findings (critical, high, medium, low). Comma-separated
	// combinations are accepted (e.g. "critical,high").
	Severity string `json:"severity,omitempty"`
	// FixedOnly keeps only vulnerabilities that have a fixed version available.
	FixedOnly bool `json:"fixed_only,omitempty"`
	// ResourceKind filters misconfigurations by the scanned resource kind.
	ResourceKind string `json:"resource_kind,omitempty"`
	// ReportKind filters list-misconfigurations: config-audit (default with
	// rbac/infra folded in via "all"), rbac-assessment, infra-assessment, all.
	ReportKind string `json:"report_kind,omitempty"`
	// ReportName is the ClusterComplianceReport name (cis, nsa, ...).
	ReportName string `json:"report_name,omitempty"`
	// MaxResults caps the number of findings returned (default 200).
	MaxResults int `json:"max_results,omitempty"`
}

// ExecuteQuery dispatches a Trivy query envelope to the matching operation.
func (e *TrivyExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Trivy query",
		zap.String("request_id", req.RequestId),
		zap.String("query_type", req.QueryType.String()),
		zap.String("filter", req.Filter))

	params := e.parseParams(req)

	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var result *v1.DatadogQueryResponse
	switch req.QueryType {
	case v1.DatadogQueryType_TRIVY_LIST_VULNERABILITIES:
		result = e.listVulnerabilities(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_TRIVY_GET_VULNERABILITY_REPORT:
		result = e.getVulnerabilityReport(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_TRIVY_LIST_EXPOSED_SECRETS:
		result = e.listExposedSecrets(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_TRIVY_LIST_MISCONFIGURATIONS:
		result = e.listMisconfigurations(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_TRIVY_GET_COMPLIANCE_REPORT:
		result = e.getComplianceReport(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_TRIVY_LIST_COMPLIANCE_REPORTS:
		result = e.listComplianceReports(ctxTimeout, req.RequestId)
	case v1.DatadogQueryType_TRIVY_RESCAN_WORKLOAD:
		result = e.rescanWorkload(ctxTimeout, req.RequestId, params)
	default:
		return errResponse(req.RequestId, http.StatusBadRequest,
			fmt.Sprintf("unsupported Trivy query type: %s", req.QueryType.String()))
	}

	if result.Success {
		e.Logger.Info("Trivy query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
	} else {
		e.Logger.Error("Trivy query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// parseParams extracts operation parameters. The filter field carries the
// workload (optionally "namespace/kind/name" or "kind/name"); json_body
// overrides.
func (e *TrivyExecutor) parseParams(req *v1.DatadogQueryRequest) trivyParams {
	var params trivyParams
	if req.JsonBody != "" {
		if err := json.Unmarshal([]byte(req.JsonBody), &params); err != nil {
			e.Logger.Warn("Failed to parse Trivy json_body, falling back to filter",
				zap.String("request_id", req.RequestId), zap.Error(err))
		}
	}
	if req.Filter != "" && params.Workload == "" {
		params.Workload = strings.TrimSpace(req.Filter)
	}
	// Accept "kind/name" in the workload field.
	if parts := strings.SplitN(params.Workload, "/", 2); len(parts) == 2 {
		if params.WorkloadKind == "" {
			params.WorkloadKind = parts[0]
		}
		params.Workload = parts[1]
	}
	return params
}

// ---------------------------------------------------------------------------
// Availability
// ---------------------------------------------------------------------------

// Probe checks whether the trivy-operator VulnerabilityReport CRD is served.
func (e *TrivyExecutor) Probe(ctx context.Context) bool {
	available := false
	if _, err := e.Dynamic.Resource(vulnerabilityReportGVR).List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
		available = true
	}
	e.mu.Lock()
	e.available = available
	e.probed = true
	e.mu.Unlock()
	if available {
		e.Logger.Info("[Trivy] Probe: trivy-operator CRDs available")
	} else {
		e.Logger.Info("[Trivy] Probe: trivy-operator not found in cluster (this is normal if trivy-operator is not installed)")
	}
	return available
}

func (e *TrivyExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.probed && e.available
}

// ---------------------------------------------------------------------------
// Dynamic client helpers
// ---------------------------------------------------------------------------

// resourceInterface returns a namespaced or cluster-scoped interface for a GVR.
func (e *TrivyExecutor) resourceInterface(gvr schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	if namespace != "" {
		return e.Dynamic.Resource(gvr).Namespace(namespace)
	}
	return e.Dynamic.Resource(gvr)
}

// listReports lists reports of one kind (namespace == "" lists across all
// namespaces), optionally constrained by a label selector.
func (e *TrivyExecutor) listReports(ctx context.Context, gvr schema.GroupVersionResource, namespace, labelSelector string) (*unstructured.UnstructuredList, error) {
	return e.resourceInterface(gvr, namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
}

// workloadSelector builds a label selector matching reports owned by one
// workload. trivy-operator lowercases the kind in its labels.
func workloadSelector(params trivyParams) string {
	parts := []string{}
	if params.Workload != "" {
		parts = append(parts, fmt.Sprintf("%s=%s", labelResourceName, params.Workload))
	}
	if params.WorkloadKind != "" {
		parts = append(parts, fmt.Sprintf("%s=%s", labelResourceKind, canonicalKind(params.WorkloadKind)))
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

func errResponse(requestID string, code int32, msg string) *v1.DatadogQueryResponse {
	return &v1.DatadogQueryResponse{
		RequestId:    requestID,
		Success:      false,
		ErrorMessage: msg,
		StatusCode:   code,
	}
}

func okResponse(requestID string, payload interface{}) *v1.DatadogQueryResponse {
	data, err := json.Marshal(payload)
	if err != nil {
		return errResponse(requestID, http.StatusInternalServerError,
			fmt.Sprintf("failed to marshal response: %v", err))
	}
	return &v1.DatadogQueryResponse{
		RequestId:    requestID,
		Success:      true,
		ResponseData: string(data),
		StatusCode:   http.StatusOK,
	}
}

func k8sErrResponse(requestID string, err error) *v1.DatadogQueryResponse {
	code := int32(http.StatusInternalServerError)
	if apierrors.IsNotFound(err) {
		code = http.StatusNotFound
	} else if apierrors.IsForbidden(err) {
		code = http.StatusForbidden
	}
	return errResponse(requestID, code, err.Error())
}
