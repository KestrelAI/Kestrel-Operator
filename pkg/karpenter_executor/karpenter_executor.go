package karpenter_executor

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

// Karpenter CRDs. NodePool and NodeClaim are cluster-scoped resources in
// karpenter.sh; EC2NodeClass lives in karpenter.k8s.aws (AWS provider).
var (
	nodePoolGVR = schema.GroupVersionResource{
		Group: "karpenter.sh", Version: "v1", Resource: "nodepools",
	}
	nodeClaimGVR = schema.GroupVersionResource{
		Group: "karpenter.sh", Version: "v1", Resource: "nodeclaims",
	}
)

// fallbackVersions handles clusters running pre-v1 Karpenter APIs.
var fallbackVersions = []string{"v1beta1"}

// KarpenterExecutor executes Karpenter operations against Karpenter CRs via
// the Kubernetes API using the dynamic client, mirroring what an admin does
// with kubectl (there is no Karpenter API server).
//
// RBAC requirements for the operator's ServiceAccount:
//   - karpenter.sh nodepools: get, list, patch, create, update
//   - karpenter.sh nodeclaims: get, list, delete
type KarpenterExecutor struct {
	Logger  *zap.Logger
	Dynamic dynamic.Interface

	mu        sync.RWMutex
	available bool
	probed    bool
}

func NewKarpenterExecutor(logger *zap.Logger, dynamicClient dynamic.Interface) *KarpenterExecutor {
	return &KarpenterExecutor{
		Logger:  logger,
		Dynamic: dynamicClient,
	}
}

// karpenterParams is the JSON payload carried in DatadogQueryRequest.json_body.
type karpenterParams struct {
	// Name of the NodePool (or NodeClaim for delete-nodeclaim).
	Name string `json:"name,omitempty"`
	// NodePool filters list-nodeclaims to claims owned by one pool.
	NodePool string `json:"nodepool,omitempty"`
	// CPULimit / MemoryLimit for scale-nodepool (e.g. "100", "1000Gi").
	// Empty string leaves the limit unchanged; "none" removes it.
	CPULimit    string `json:"cpu_limit,omitempty"`
	MemoryLimit string `json:"memory_limit,omitempty"`
	// ConsolidationPolicy for set-disruption: WhenEmpty or
	// WhenEmptyOrUnderutilized. ConsolidateAfter e.g. "30s", "5m", "Never".
	ConsolidationPolicy string `json:"consolidation_policy,omitempty"`
	ConsolidateAfter    string `json:"consolidate_after,omitempty"`
	// NodePoolSpec is a full NodePool manifest (YAML or JSON) for
	// apply-nodepool.
	NodePoolSpec string `json:"nodepool_spec,omitempty"`
}

// ExecuteQuery dispatches a Karpenter query envelope to the matching operation.
func (e *KarpenterExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Karpenter query",
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
	case v1.DatadogQueryType_KARPENTER_LIST_NODEPOOLS:
		result = e.listNodePools(ctxTimeout, req.RequestId)
	case v1.DatadogQueryType_KARPENTER_GET_NODEPOOL_STATUS:
		result = e.getNodePoolStatus(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KARPENTER_LIST_NODECLAIMS:
		result = e.listNodeClaims(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KARPENTER_SCALE_NODEPOOL:
		result = e.scaleNodePool(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KARPENTER_SET_DISRUPTION:
		result = e.setDisruption(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KARPENTER_APPLY_NODEPOOL:
		result = e.applyNodePool(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_KARPENTER_DELETE_NODECLAIM:
		result = e.deleteNodeClaim(ctxTimeout, req.RequestId, params)
	default:
		return errResponse(req.RequestId, http.StatusBadRequest,
			fmt.Sprintf("unsupported Karpenter query type: %s", req.QueryType.String()))
	}

	if result.Success {
		e.Logger.Info("Karpenter query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
	} else {
		e.Logger.Error("Karpenter query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// parseParams extracts operation parameters. The filter field carries the
// resource name; json_body overrides.
func (e *KarpenterExecutor) parseParams(req *v1.DatadogQueryRequest) karpenterParams {
	var params karpenterParams
	if req.JsonBody != "" {
		if err := json.Unmarshal([]byte(req.JsonBody), &params); err != nil {
			e.Logger.Warn("Failed to parse Karpenter json_body, falling back to filter",
				zap.String("request_id", req.RequestId), zap.Error(err))
		}
	}
	if req.Filter != "" && params.Name == "" {
		params.Name = strings.TrimSpace(req.Filter)
	}
	return params
}

// ---------------------------------------------------------------------------
// Availability
// ---------------------------------------------------------------------------

// Probe checks whether the Karpenter NodePool CRD is served.
func (e *KarpenterExecutor) Probe(ctx context.Context) bool {
	available := false
	if _, _, err := e.listResource(ctx, nodePoolGVR, 1); err == nil {
		available = true
	}
	e.mu.Lock()
	e.available = available
	e.probed = true
	e.mu.Unlock()
	if available {
		e.Logger.Info("[Karpenter] Probe: Karpenter CRDs available")
	} else {
		e.Logger.Info("[Karpenter] Probe: Karpenter not found in cluster (this is normal if Karpenter is not installed)")
	}
	return available
}

func (e *KarpenterExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.probed && e.available
}

// ---------------------------------------------------------------------------
// Dynamic client helpers (with API version fallback)
// ---------------------------------------------------------------------------

// gvrCandidates returns the candidate GVRs, preferred version first.
func gvrCandidates(gvr schema.GroupVersionResource) []schema.GroupVersionResource {
	gvrs := []schema.GroupVersionResource{gvr}
	for _, ver := range fallbackVersions {
		gvrs = append(gvrs, schema.GroupVersionResource{Group: gvr.Group, Version: ver, Resource: gvr.Resource})
	}
	return gvrs
}

// listResource lists cluster-scoped Karpenter resources, trying newer API
// versions first.
func (e *KarpenterExecutor) listResource(ctx context.Context, gvr schema.GroupVersionResource, limit int64) (*unstructured.UnstructuredList, schema.GroupVersionResource, error) {
	opts := metav1.ListOptions{}
	if limit > 0 {
		opts.Limit = limit
	}
	var lastErr error
	for _, candidate := range gvrCandidates(gvr) {
		list, err := e.Dynamic.Resource(candidate).List(ctx, opts)
		if err == nil {
			return list, candidate, nil
		}
		lastErr = err
	}
	return nil, schema.GroupVersionResource{}, lastErr
}

// getResource fetches a cluster-scoped Karpenter resource by name, trying
// newer API versions first.
func (e *KarpenterExecutor) getResource(ctx context.Context, gvr schema.GroupVersionResource, name string) (*unstructured.Unstructured, schema.GroupVersionResource, error) {
	if name == "" {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("resource name is required")
	}
	var lastErr error
	for _, candidate := range gvrCandidates(gvr) {
		obj, err := e.Dynamic.Resource(candidate).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return obj, candidate, nil
		}
		lastErr = err
	}
	return nil, schema.GroupVersionResource{}, lastErr
}

func (e *KarpenterExecutor) patchResource(ctx context.Context, gvr schema.GroupVersionResource, name string, patch []byte) (*unstructured.Unstructured, error) {
	return e.Dynamic.Resource(gvr).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
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
