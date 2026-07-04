package flux_executor

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
	"k8s.io/client-go/kubernetes"
)

// reconcileAnnotation is the annotation Flux controllers watch to trigger a
// manual reconciliation (same mechanism `flux reconcile` uses).
const reconcileAnnotation = "reconcile.fluxcd.io/requestedAt"

// fluxKinds maps the workflow-facing resource kind to its GVR.
// Kustomization and HelmRelease are the "apply" resources; the source kinds
// let workflows force-refresh artifacts.
var fluxKinds = map[string]schema.GroupVersionResource{
	"Kustomization":  {Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations"},
	"HelmRelease":    {Group: "helm.toolkit.fluxcd.io", Version: "v2", Resource: "helmreleases"},
	"GitRepository":  {Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "gitrepositories"},
	"HelmRepository": {Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "helmrepositories"},
	"OCIRepository":  {Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "ocirepositories"},
	"HelmChart":      {Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "helmcharts"},
	"Bucket":         {Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "buckets"},
}

// fallbackVersions handles clusters running older Flux APIs.
var fallbackVersions = map[string][]string{
	"Kustomization":  {"v1beta2"},
	"HelmRelease":    {"v2beta2", "v2beta1"},
	"GitRepository":  {"v1beta2"},
	"HelmRepository": {"v1beta2"},
	"OCIRepository":  {"v1beta2"},
	"HelmChart":      {"v1beta2"},
	"Bucket":         {"v1beta2"},
}

// listKinds is the order in which kinds are listed/searched for list and
// find operations.
var listKinds = []string{"Kustomization", "HelmRelease", "GitRepository", "HelmRepository", "OCIRepository", "Bucket"}

// FluxExecutor executes Flux CD operations against Flux CRs via the
// Kubernetes API using the dynamic client. There is no Flux API server:
// everything is a CR read/patch/annotation, mirroring the flux CLI.
//
// RBAC requirements for the operator's ServiceAccount:
//   - kustomize.toolkit.fluxcd.io / helm.toolkit.fluxcd.io /
//     source.toolkit.fluxcd.io resources: get, list, patch
//   - events: list (flux-get-events)
type FluxExecutor struct {
	Logger    *zap.Logger
	Dynamic   dynamic.Interface
	ClientSet kubernetes.Interface

	mu        sync.RWMutex
	available bool
	probed    bool
}

func NewFluxExecutor(logger *zap.Logger, dynamicClient dynamic.Interface, clientSet kubernetes.Interface) *FluxExecutor {
	return &FluxExecutor{
		Logger:    logger,
		Dynamic:   dynamicClient,
		ClientSet: clientSet,
	}
}

// fluxParams is the JSON payload carried in DatadogQueryRequest.json_body.
type fluxParams struct {
	Kind      string `json:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	// WithSource reconciles the resource's source before the resource itself.
	WithSource bool `json:"with_source,omitempty"`
	// TimeoutSeconds bounds flux-wait-ready style polling (server-side loop
	// uses get-status; kept for future use).
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// ExecuteQuery dispatches a Flux query envelope to the matching operation.
func (e *FluxExecutor) ExecuteQuery(ctx context.Context, req *v1.DatadogQueryRequest) *v1.DatadogQueryResponse {
	e.Logger.Info("Executing Flux query",
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
	case v1.DatadogQueryType_FLUX_RECONCILE:
		result = e.reconcile(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_FLUX_SUSPEND:
		result = e.setSuspended(ctxTimeout, req.RequestId, params, true)
	case v1.DatadogQueryType_FLUX_RESUME:
		result = e.setSuspended(ctxTimeout, req.RequestId, params, false)
	case v1.DatadogQueryType_FLUX_GET_STATUS:
		result = e.getStatus(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_FLUX_LIST:
		result = e.list(ctxTimeout, req.RequestId, params)
	case v1.DatadogQueryType_FLUX_GET_EVENTS:
		result = e.getEvents(ctxTimeout, req.RequestId, params)
	default:
		return errResponse(req.RequestId, http.StatusBadRequest,
			fmt.Sprintf("unsupported Flux query type: %s", req.QueryType.String()))
	}

	if result.Success {
		e.Logger.Info("Flux query succeeded",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()))
	} else {
		e.Logger.Error("Flux query failed",
			zap.String("request_id", req.RequestId),
			zap.String("query_type", req.QueryType.String()),
			zap.String("error", result.ErrorMessage))
	}
	return result
}

// parseParams extracts operation parameters. The filter field carries
// "kind/namespace/name" (or "kind/name"); json_body overrides.
func (e *FluxExecutor) parseParams(req *v1.DatadogQueryRequest) fluxParams {
	var params fluxParams
	if req.JsonBody != "" {
		if err := json.Unmarshal([]byte(req.JsonBody), &params); err != nil {
			e.Logger.Warn("Failed to parse Flux json_body, falling back to filter",
				zap.String("request_id", req.RequestId), zap.Error(err))
		}
	}
	if req.Filter != "" && params.Name == "" {
		parts := strings.Split(req.Filter, "/")
		switch len(parts) {
		case 3:
			params.Kind, params.Namespace, params.Name = parts[0], parts[1], parts[2]
		case 2:
			params.Kind, params.Name = parts[0], parts[1]
		case 1:
			params.Name = parts[0]
		}
	}
	params.Kind = normalizeKind(params.Kind)
	return params
}

// normalizeKind maps case-insensitive / plural aliases to canonical kinds.
func normalizeKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "kustomization", "kustomizations", "ks":
		return "Kustomization"
	case "helmrelease", "helmreleases", "hr":
		return "HelmRelease"
	case "gitrepository", "gitrepositories":
		return "GitRepository"
	case "helmrepository", "helmrepositories":
		return "HelmRepository"
	case "ocirepository", "ocirepositories":
		return "OCIRepository"
	case "helmchart", "helmcharts":
		return "HelmChart"
	case "bucket", "buckets":
		return "Bucket"
	case "":
		return ""
	default:
		return kind
	}
}

// ---------------------------------------------------------------------------
// Availability
// ---------------------------------------------------------------------------

// Probe checks whether core Flux CRDs (Kustomization or HelmRelease) are served.
func (e *FluxExecutor) Probe(ctx context.Context) bool {
	available := false
	for _, kind := range []string{"Kustomization", "HelmRelease"} {
		if _, _, err := e.listKind(ctx, kind, metav1.NamespaceAll, 1); err == nil {
			available = true
			break
		}
	}
	e.mu.Lock()
	e.available = available
	e.probed = true
	e.mu.Unlock()
	if available {
		e.Logger.Info("[Flux] Probe: Flux CD CRDs available")
	} else {
		e.Logger.Info("[Flux] Probe: Flux CD not found in cluster (this is normal if Flux is not installed)")
	}
	return available
}

func (e *FluxExecutor) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.probed && e.available
}

// ---------------------------------------------------------------------------
// Dynamic client helpers (with API version fallback)
// ---------------------------------------------------------------------------

// gvrsForKind returns the candidate GVRs for a kind, preferred version first.
func gvrsForKind(kind string) ([]schema.GroupVersionResource, error) {
	gvr, ok := fluxKinds[kind]
	if !ok {
		return nil, fmt.Errorf("unsupported Flux resource kind: %q (supported: Kustomization, HelmRelease, GitRepository, HelmRepository, OCIRepository, HelmChart, Bucket)", kind)
	}
	gvrs := []schema.GroupVersionResource{gvr}
	for _, ver := range fallbackVersions[kind] {
		gvrs = append(gvrs, schema.GroupVersionResource{Group: gvr.Group, Version: ver, Resource: gvr.Resource})
	}
	return gvrs, nil
}

// getResource fetches a Flux CR, trying newer API versions first. When the
// namespace is empty it searches all namespaces for a unique name match.
func (e *FluxExecutor) getResource(ctx context.Context, kind, namespace, name string) (*unstructured.Unstructured, schema.GroupVersionResource, error) {
	if kind == "" {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("resource kind is required")
	}
	if name == "" {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("resource name is required")
	}
	gvrs, err := gvrsForKind(kind)
	if err != nil {
		return nil, schema.GroupVersionResource{}, err
	}

	var lastErr error
	for _, gvr := range gvrs {
		if namespace != "" {
			obj, err := e.Dynamic.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			if err == nil {
				return obj, gvr, nil
			}
			lastErr = err
			if apierrors.IsNotFound(err) && !isNoKindMatch(err) {
				continue
			}
			continue
		}
		list, err := e.Dynamic.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			lastErr = err
			continue
		}
		var found *unstructured.Unstructured
		for i := range list.Items {
			if list.Items[i].GetName() == name {
				if found != nil {
					return nil, gvr, fmt.Errorf("%s %q exists in multiple namespaces (%s, %s) — specify a namespace",
						kind, name, found.GetNamespace(), list.Items[i].GetNamespace())
				}
				found = &list.Items[i]
			}
		}
		if found != nil {
			return found, gvr, nil
		}
		lastErr = apierrors.NewNotFound(schema.GroupResource{Group: gvr.Group, Resource: gvr.Resource}, name)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%s %q not found", kind, name)
	}
	return nil, schema.GroupVersionResource{}, lastErr
}

// listKind lists resources of a kind, trying newer API versions first.
func (e *FluxExecutor) listKind(ctx context.Context, kind, namespace string, limit int64) (*unstructured.UnstructuredList, schema.GroupVersionResource, error) {
	gvrs, err := gvrsForKind(kind)
	if err != nil {
		return nil, schema.GroupVersionResource{}, err
	}
	opts := metav1.ListOptions{}
	if limit > 0 {
		opts.Limit = limit
	}
	var lastErr error
	for _, gvr := range gvrs {
		list, err := e.Dynamic.Resource(gvr).Namespace(namespace).List(ctx, opts)
		if err == nil {
			return list, gvr, nil
		}
		lastErr = err
	}
	return nil, schema.GroupVersionResource{}, lastErr
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

func (e *FluxExecutor) patchResource(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, patch []byte) (*unstructured.Unstructured, error) {
	return e.Dynamic.Resource(gvr).Namespace(namespace).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
}
