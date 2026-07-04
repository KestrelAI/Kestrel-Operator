package rollouts_executor

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

// RolloutGVR is the GroupVersionResource for Argo Rollouts Rollout CRs.
var RolloutGVR = schema.GroupVersionResource{
	Group:    "argoproj.io",
	Version:  "v1alpha1",
	Resource: "rollouts",
}

// ReplicaSetGVR is used for revision lookups on rollouts-undo.
var ReplicaSetGVR = schema.GroupVersionResource{
	Group:    "apps",
	Version:  "v1",
	Resource: "replicasets",
}

// RolloutsExecutor executes Argo Rollouts operations against Rollout CRs
// (rollouts.argoproj.io) via the Kubernetes API using the dynamic client.
// Unlike the ArgoCD executor there is no API-server login: everything is a
// CR read/patch, mirroring what the kubectl-argo-rollouts plugin does.
//
// RBAC requirements for the operator's ServiceAccount:
//   - argoproj.io/rollouts: get, list, patch
//   - apps/replicasets: get, list (revision history for undo)
type RolloutsExecutor struct {
	Logger  *zap.Logger
	Dynamic dynamic.Interface

	mu        sync.RWMutex
	available bool
	probed    bool
}

func NewRolloutsExecutor(logger *zap.Logger, dynamicClient dynamic.Interface) *RolloutsExecutor {
	return &RolloutsExecutor{
		Logger:  logger,
		Dynamic: dynamicClient,
	}
}

// rolloutParams is the JSON payload carried in DatadogQueryRequest.json_body
// for Rollouts operations.
type rolloutParams struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	// Full indicates a full promotion (skip all remaining canary steps).
	Full bool `json:"full,omitempty"`
	// Revision is the target revision for undo (0 = previous revision).
	Revision int64 `json:"revision,omitempty"`
}

// ExecuteQuery dispatches a Rollouts query envelope to the matching operation.
func (e *RolloutsExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Argo Rollouts query",
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
	case v1.DatadogQueryType_ROLLOUTS_PROMOTE:
		result = e.promote(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_ABORT:
		result = e.abort(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_RETRY:
		result = e.retry(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_UNDO:
		result = e.undo(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_PAUSE:
		result = e.setPaused(ctxTimeout, req.RequestId, params, true)
	case v1.DatadogQueryType_ROLLOUTS_RESUME:
		result = e.setPaused(ctxTimeout, req.RequestId, params, false)
	case v1.DatadogQueryType_ROLLOUTS_RESTART:
		result = e.restart(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_GET_STATUS:
		result = e.getStatus(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_ROLLOUTS_LIST:
		result = e.list(ctxTimeout, req.RequestId, params)
	default:
		return errResponse(req.RequestId, http.StatusBadRequest,
			fmt.Sprintf("unsupported Argo Rollouts query type: %s", req.QueryType.String()))
	}

	if result.Success {
		e.Logger.Info("Argo Rollouts query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
	} else {
		e.Logger.Error("Argo Rollouts query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// parseParams extracts operation parameters. The filter field carries
// "namespace/name" (or just "name"); json_body carries the rest.
func (e *RolloutsExecutor) parseParams(req *v1.DatadogQueryRequest) rolloutParams {
	var params rolloutParams
	if req.JsonBody != "" {
		if err := json.Unmarshal([]byte(req.JsonBody), &params); err != nil {
			e.Logger.Warn("Failed to parse Rollouts json_body, falling back to filter",
				zap.String("request_id", req.RequestId), zap.Error(err))
		}
	}
	if req.Filter != "" && params.Name == "" {
		if idx := strings.Index(req.Filter, "/"); idx >= 0 {
			params.Namespace = req.Filter[:idx]
			params.Name = req.Filter[idx+1:]
		} else {
			params.Name = req.Filter
		}
	}
	return params
}

// ---------------------------------------------------------------------------
// Availability
// ---------------------------------------------------------------------------

// Probe checks whether the Rollout CRD is served by the cluster.
func (e *RolloutsExecutor) Probe(ctx context.Context) bool {
	_, err := e.Dynamic.Resource(RolloutGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{Limit: 1})
	available := err == nil
	if err != nil && !apierrors.IsNotFound(err) && !isNoKindMatch(err) {
		e.Logger.Info("[Rollouts] Probe failed (this is normal if Argo Rollouts is not installed)",
			zap.Error(err))
	}
	e.mu.Lock()
	e.available = available
	e.probed = true
	e.mu.Unlock()
	if available {
		e.Logger.Info("[Rollouts] Probe: Argo Rollouts CRD available")
	}
	return available
}

func (e *RolloutsExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.probed && e.available
}

func isNoKindMatch(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "could not find the requested resource") ||
		strings.Contains(err.Error(), "no matches for kind") ||
		strings.Contains(err.Error(), "the server could not find the requested resource"))
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

func (e *RolloutsExecutor) getRollout(ctx context.Context, params rolloutParams) (*unstructured.Unstructured, error) {
	if params.Name == "" {
		return nil, fmt.Errorf("rollout name is required")
	}
	if params.Namespace != "" {
		return e.Dynamic.Resource(RolloutGVR).Namespace(params.Namespace).Get(ctx, params.Name, metav1.GetOptions{})
	}
	// No namespace given — search all namespaces for a unique match.
	list, err := e.Dynamic.Resource(RolloutGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var found *unstructured.Unstructured
	for i := range list.Items {
		if list.Items[i].GetName() == params.Name {
			if found != nil {
				return nil, fmt.Errorf("rollout %q exists in multiple namespaces (%s, %s) — specify a namespace",
					params.Name, found.GetNamespace(), list.Items[i].GetNamespace())
			}
			found = &list.Items[i]
		}
	}
	if found == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: RolloutGVR.Group, Resource: RolloutGVR.Resource}, params.Name)
	}
	return found, nil
}

func (e *RolloutsExecutor) patchRollout(ctx context.Context, namespace, name string, patchType types.PatchType, patch []byte) (*unstructured.Unstructured, error) {
	return e.Dynamic.Resource(RolloutGVR).Namespace(namespace).Patch(ctx, name, patchType, patch, metav1.PatchOptions{})
}
