package kyverno_executor

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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// Kyverno CRDs. ClusterPolicy is cluster-scoped and Policy is namespaced
// (kyverno.io); PolicyReport is namespaced and ClusterPolicyReport is
// cluster-scoped (wgpolicyk8s.io).
var (
	clusterPolicyGVR = schema.GroupVersionResource{
		Group: "kyverno.io", Version: "v1", Resource: "clusterpolicies",
	}
	policyGVR = schema.GroupVersionResource{
		Group: "kyverno.io", Version: "v1", Resource: "policies",
	}
	policyReportGVR = schema.GroupVersionResource{
		Group: "wgpolicyk8s.io", Version: "v1alpha2", Resource: "policyreports",
	}
	clusterPolicyReportGVR = schema.GroupVersionResource{
		Group: "wgpolicyk8s.io", Version: "v1alpha2", Resource: "clusterpolicyreports",
	}
)

// reportFallbackVersions handles clusters serving a newer policy-report API.
var reportFallbackVersions = []string{"v1beta1"}

// KyvernoExecutor executes Kyverno operations against Kyverno CRs via the
// Kubernetes API using the dynamic client, mirroring what an admin does with
// kubectl (Kyverno has no standalone API server).
//
// RBAC requirements for the operator's ServiceAccount:
//   - kyverno.io clusterpolicies, policies: get, list, patch, create, update, delete
//   - wgpolicyk8s.io policyreports, clusterpolicyreports: get, list, watch
type KyvernoExecutor struct {
	Logger  *zap.Logger
	Dynamic dynamic.Interface

	mu        sync.RWMutex
	available bool
	probed    bool
}

func NewKyvernoExecutor(logger *zap.Logger, dynamicClient dynamic.Interface) *KyvernoExecutor {
	return &KyvernoExecutor{
		Logger:  logger,
		Dynamic: dynamicClient,
	}
}

// kyvernoParams is the JSON payload carried in DatadogQueryRequest.json_body.
type kyvernoParams struct {
	// Name of the policy ("namespace/name" also accepted for namespaced Policies).
	Name string `json:"name,omitempty"`
	// Namespace scopes the operation to a namespaced Policy / PolicyReports.
	Namespace string `json:"namespace,omitempty"`
	// EnforcementAction for set-enforcement: Audit or Enforce.
	EnforcementAction string `json:"enforcement_action,omitempty"`
	// Policy filters list-violations to results produced by one policy.
	Policy string `json:"policy,omitempty"`
	// Severity filters list-violations (critical, high, medium, low, info).
	Severity string `json:"severity,omitempty"`
	// Result filters list-violations: fail (default), warn, error, or all.
	// Comma-separated combinations are accepted (e.g. "fail,error").
	Result string `json:"result,omitempty"`
	// MaxResults caps the number of violations returned (default 200).
	MaxResults int `json:"max_results,omitempty"`
	// PolicySpec is a full ClusterPolicy/Policy manifest (YAML or JSON) for
	// apply-policy.
	PolicySpec string `json:"policy_spec,omitempty"`
}

// ExecuteQuery dispatches a Kyverno query envelope to the matching operation.
func (e *KyvernoExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Kyverno query",
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
	case v1.DatadogQueryType_KYVERNO_LIST_POLICIES:
		result = e.listPolicies(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KYVERNO_GET_POLICY:
		result = e.getPolicy(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KYVERNO_LIST_VIOLATIONS:
		result = e.listViolations(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KYVERNO_SET_ENFORCEMENT:
		result = e.setEnforcement(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KYVERNO_APPLY_POLICY:
		result = e.applyPolicy(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KYVERNO_DELETE_POLICY:
		result = e.deletePolicy(ctxTimeout, req.RequestId, params)
	default:
		return errResponse(req.RequestId, http.StatusBadRequest,
			fmt.Sprintf("unsupported Kyverno query type: %s", req.QueryType.String()))
	}

	if result.Success {
		e.Logger.Info("Kyverno query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
	} else {
		e.Logger.Error("Kyverno query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// parseParams extracts operation parameters. The filter field carries the
// policy name (optionally "namespace/name"); json_body overrides.
func (e *KyvernoExecutor) parseParams(req *v1.DatadogQueryRequest) kyvernoParams {
	var params kyvernoParams
	if req.JsonBody != "" {
		if err := json.Unmarshal([]byte(req.JsonBody), &params); err != nil {
			e.Logger.Warn("Failed to parse Kyverno json_body, falling back to filter",
				zap.String("request_id", req.RequestId), zap.Error(err))
		}
	}
	if req.Filter != "" && params.Name == "" {
		params.Name = strings.TrimSpace(req.Filter)
	}
	// Accept "namespace/name" in the name field for namespaced Policies.
	if parts := strings.SplitN(params.Name, "/", 2); len(parts) == 2 {
		if params.Namespace == "" {
			params.Namespace = parts[0]
		}
		params.Name = parts[1]
	}
	return params
}

// ---------------------------------------------------------------------------
// Availability
// ---------------------------------------------------------------------------

// Probe checks whether the Kyverno ClusterPolicy CRD is served.
func (e *KyvernoExecutor) Probe(ctx context.Context) bool {
	available := false
	if _, err := e.Dynamic.Resource(clusterPolicyGVR).List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
		available = true
	}
	e.mu.Lock()
	e.available = available
	e.probed = true
	e.mu.Unlock()
	if available {
		e.Logger.Info("[Kyverno] Probe: Kyverno CRDs available")
	} else {
		e.Logger.Info("[Kyverno] Probe: Kyverno not found in cluster (this is normal if Kyverno is not installed)")
	}
	return available
}

func (e *KyvernoExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.probed && e.available
}

// ---------------------------------------------------------------------------
// Dynamic client helpers
// ---------------------------------------------------------------------------

// resourceInterface returns a namespaced or cluster-scoped interface for a GVR.
func (e *KyvernoExecutor) resourceInterface(gvr schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	if namespace != "" {
		return e.Dynamic.Resource(gvr).Namespace(namespace)
	}
	return e.Dynamic.Resource(gvr)
}

// reportGVRCandidates returns candidate policy-report GVRs, preferred version first.
func reportGVRCandidates(gvr schema.GroupVersionResource) []schema.GroupVersionResource {
	gvrs := []schema.GroupVersionResource{gvr}
	for _, ver := range reportFallbackVersions {
		gvrs = append(gvrs, schema.GroupVersionResource{Group: gvr.Group, Version: ver, Resource: gvr.Resource})
	}
	return gvrs
}

// listReports lists policy reports (namespace == "" lists across all
// namespaces for namespaced reports), trying newer API versions on failure.
func (e *KyvernoExecutor) listReports(ctx context.Context, gvr schema.GroupVersionResource, namespace string) (*unstructured.UnstructuredList, error) {
	var lastErr error
	for _, candidate := range reportGVRCandidates(gvr) {
		list, err := e.resourceInterface(candidate, namespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			return list, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// getPolicyResource fetches a policy: the namespaced Policy when a namespace
// is provided, otherwise the cluster-scoped ClusterPolicy.
func (e *KyvernoExecutor) getPolicyResource(ctx context.Context, name, namespace string) (*unstructured.Unstructured, schema.GroupVersionResource, error) {
	if name == "" {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("policy name is required")
	}
	if namespace != "" {
		obj, err := e.Dynamic.Resource(policyGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		return obj, policyGVR, err
	}
	obj, err := e.Dynamic.Resource(clusterPolicyGVR).Get(ctx, name, metav1.GetOptions{})
	return obj, clusterPolicyGVR, err
}

func (e *KyvernoExecutor) patchPolicy(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, patch []byte) (*unstructured.Unstructured, error) {
	return e.resourceInterface(gvr, namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
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
