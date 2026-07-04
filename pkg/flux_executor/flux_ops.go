package flux_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// reconcile triggers a manual reconciliation by setting the
// reconcile.fluxcd.io/requestedAt annotation (same as `flux reconcile`).
// With WithSource=true, the resource's source (GitRepository/HelmChart/...)
// is reconciled first.
func (e *FluxExecutor) reconcile(ctx context.Context, requestID string, params fluxParams) *v1.DatadogQueryResponse {
	obj, gvr, err := e.getResource(ctx, params.Kind, params.Namespace, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := obj.GetNamespace(), obj.GetName()

	var sourceReconciled string
	if params.WithSource {
		if srcKind, srcNS, srcName, ok := sourceRefOf(obj); ok {
			if srcObj, srcGVR, err := e.getResource(ctx, srcKind, firstNonEmpty(srcNS, ns), srcName); err == nil {
				if err := e.annotate(ctx, srcGVR, srcObj.GetNamespace(), srcObj.GetName()); err == nil {
					sourceReconciled = fmt.Sprintf("%s/%s/%s", srcKind, srcObj.GetNamespace(), srcObj.GetName())
				} else {
					e.Logger.Warn("[Flux] Failed to reconcile source, continuing with resource",
						zap.String("source", srcName), zap.Error(err))
				}
			}
		}
	}

	if err := e.annotate(ctx, gvr, ns, name); err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Flux] Reconcile requested",
		zap.String("kind", params.Kind), zap.String("namespace", ns), zap.String("name", name),
		zap.Bool("with_source", params.WithSource))

	result := map[string]interface{}{
		"status": "reconcile_requested", "kind": params.Kind, "name": name, "namespace": ns,
	}
	if sourceReconciled != "" {
		result["source_reconciled"] = sourceReconciled
	}
	return okResponse(requestID, result)
}

func (e *FluxExecutor) annotate(ctx context.Context, gvr schema.GroupVersionResource, ns, name string) error {
	patch := jsonMustMarshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				reconcileAnnotation: time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	})
	_, err := e.patchResource(ctx, gvr, ns, name, patch)
	return err
}

// setSuspended patches spec.suspend. Resuming also triggers a reconcile so
// the resource catches up immediately (mirrors `flux resume`).
func (e *FluxExecutor) setSuspended(ctx context.Context, requestID string, params fluxParams, suspend bool) *v1.DatadogQueryResponse {
	obj, gvr, err := e.getResource(ctx, params.Kind, params.Namespace, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := obj.GetNamespace(), obj.GetName()

	patch := fmt.Sprintf(`{"spec":{"suspend":%v}}`, suspend)
	if _, err := e.patchResource(ctx, gvr, ns, name, []byte(patch)); err != nil {
		return k8sErrResponse(requestID, err)
	}

	action := "suspended"
	if !suspend {
		action = "resumed"
		if err := e.annotate(ctx, gvr, ns, name); err != nil {
			e.Logger.Warn("[Flux] Resume succeeded but reconcile annotation failed",
				zap.String("name", name), zap.Error(err))
		}
	}

	e.Logger.Info("[Flux] Set suspend state",
		zap.String("kind", params.Kind), zap.String("namespace", ns), zap.String("name", name),
		zap.Bool("suspend", suspend))
	return okResponse(requestID, map[string]interface{}{
		"status": action, "kind": params.Kind, "name": name, "namespace": ns,
	})
}

// getStatus returns a compact status summary for one Flux resource.
func (e *FluxExecutor) getStatus(ctx context.Context, requestID string, params fluxParams) *v1.DatadogQueryResponse {
	obj, _, err := e.getResource(ctx, params.Kind, params.Namespace, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	return okResponse(requestID, summarizeFluxResource(params.Kind, obj))
}

// list returns summaries of Flux resources. With a kind, only that kind;
// otherwise all supported kinds. Optionally namespace-scoped.
func (e *FluxExecutor) list(ctx context.Context, requestID string, params fluxParams) *v1.DatadogQueryResponse {
	ns := params.Namespace
	if ns == "" {
		ns = metav1.NamespaceAll
	}
	kinds := listKinds
	if params.Kind != "" {
		kinds = []string{params.Kind}
	}

	items := make([]map[string]interface{}, 0)
	var lastErr error
	for _, kind := range kinds {
		list, _, err := e.listKind(ctx, kind, ns, 0)
		if err != nil {
			lastErr = err
			continue
		}
		for i := range list.Items {
			items = append(items, summarizeFluxResource(kind, &list.Items[i]))
		}
	}
	if len(items) == 0 && lastErr != nil && params.Kind != "" {
		return k8sErrResponse(requestID, lastErr)
	}
	return okResponse(requestID, map[string]interface{}{"items": items})
}

// getEvents returns recent Kubernetes events for a Flux resource, useful for
// debugging failed reconciliations.
func (e *FluxExecutor) getEvents(ctx context.Context, requestID string, params fluxParams) *v1.DatadogQueryResponse {
	obj, _, err := e.getResource(ctx, params.Kind, params.Namespace, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := obj.GetNamespace(), obj.GetName()

	fieldSelector := fmt.Sprintf("involvedObject.kind=%s,involvedObject.name=%s", params.Kind, name)
	events, err := e.ClientSet.CoreV1().Events(ns).List(ctx, metav1.ListOptions{FieldSelector: fieldSelector})
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	type eventEntry struct {
		Type      string    `json:"type"`
		Reason    string    `json:"reason"`
		Message   string    `json:"message"`
		Count     int32     `json:"count,omitempty"`
		Timestamp time.Time `json:"timestamp"`
	}
	entries := make([]eventEntry, 0, len(events.Items))
	for _, ev := range events.Items {
		ts := ev.LastTimestamp.Time
		if ts.IsZero() {
			ts = ev.EventTime.Time
		}
		entries = append(entries, eventEntry{
			Type: ev.Type, Reason: ev.Reason, Message: ev.Message, Count: ev.Count, Timestamp: ts,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp.After(entries[j].Timestamp) })
	if len(entries) > 30 {
		entries = entries[:30]
	}

	return okResponse(requestID, map[string]interface{}{
		"kind": params.Kind, "name": name, "namespace": ns, "events": entries,
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// summarizeFluxResource extracts the fields workflows care about: readiness,
// suspension, revision, and the source reference.
func summarizeFluxResource(kind string, obj *unstructured.Unstructured) map[string]interface{} {
	summary := map[string]interface{}{
		"kind":      kind,
		"name":      obj.GetName(),
		"namespace": obj.GetNamespace(),
	}

	if suspended, found, _ := unstructured.NestedBool(obj.Object, "spec", "suspend"); found {
		summary["suspended"] = suspended
	}

	// Ready condition
	if conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions"); found {
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if condType, _ := cond["type"].(string); condType == "Ready" {
				status, _ := cond["status"].(string)
				summary["ready"] = status == "True"
				if msg, ok := cond["message"].(string); ok {
					summary["message"] = msg
				}
				if reason, ok := cond["reason"].(string); ok {
					summary["reason"] = reason
				}
				break
			}
		}
	}

	if rev, found, _ := unstructured.NestedString(obj.Object, "status", "lastAppliedRevision"); found && rev != "" {
		summary["revision"] = rev
	} else if artifactRev, found, _ := unstructured.NestedString(obj.Object, "status", "artifact", "revision"); found && artifactRev != "" {
		summary["revision"] = artifactRev
	}

	if srcKind, srcNS, srcName, ok := sourceRefOf(obj); ok {
		ref := map[string]interface{}{"kind": srcKind, "name": srcName}
		if srcNS != "" {
			ref["namespace"] = srcNS
		}
		summary["source_ref"] = ref
	}

	return summary
}

// sourceRefOf extracts the sourceRef of a Kustomization (spec.sourceRef) or
// HelmRelease (spec.chart.spec.sourceRef / spec.chartRef).
func sourceRefOf(obj *unstructured.Unstructured) (kind, namespace, name string, ok bool) {
	paths := [][]string{
		{"spec", "sourceRef"},
		{"spec", "chart", "spec", "sourceRef"},
		{"spec", "chartRef"},
	}
	for _, path := range paths {
		ref, found, _ := unstructured.NestedMap(obj.Object, path...)
		if !found || ref == nil {
			continue
		}
		kind, _ = ref["kind"].(string)
		name, _ = ref["name"].(string)
		namespace, _ = ref["namespace"].(string)
		if kind != "" && name != "" {
			return normalizeKind(kind), namespace, name, true
		}
	}
	return "", "", "", false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// jsonMustMarshal is a tiny helper for building patches.
func jsonMustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
