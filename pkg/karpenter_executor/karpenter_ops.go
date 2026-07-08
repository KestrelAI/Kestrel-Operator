package karpenter_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// listNodePools returns summaries of all NodePools in the cluster.
func (e *KarpenterExecutor) listNodePools(ctx context.Context, requestID string) *v1.DatadogQueryResponse {
	list, _, err := e.listResource(ctx, nodePoolGVR, 0)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	items := make([]map[string]interface{}, 0, len(list.Items))
	for i := range list.Items {
		items = append(items, summarizeNodePool(&list.Items[i]))
	}
	return okResponse(requestID, map[string]interface{}{"items": items})
}

// getNodePoolStatus returns one NodePool's summary plus its NodeClaim counts.
func (e *KarpenterExecutor) getNodePoolStatus(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	obj, _, err := e.getResource(ctx, nodePoolGVR, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	summary := summarizeNodePool(obj)

	// Enrich with live NodeClaim counts; non-fatal if claims can't be listed.
	if claims, _, err := e.listResource(ctx, nodeClaimGVR, 0); err == nil {
		total, ready := 0, 0
		for i := range claims.Items {
			if nodeClaimOwnerPool(&claims.Items[i]) != obj.GetName() {
				continue
			}
			total++
			if nodeClaimIsReady(&claims.Items[i]) {
				ready++
			}
		}
		summary["nodeclaim_count"] = total
		summary["ready_nodeclaim_count"] = ready
	}
	return okResponse(requestID, summary)
}

// listNodeClaims returns summaries of NodeClaims, optionally filtered to one
// NodePool.
func (e *KarpenterExecutor) listNodeClaims(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	list, _, err := e.listResource(ctx, nodeClaimGVR, 0)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	pool := params.NodePool
	if pool == "" {
		pool = params.Name
	}
	items := make([]map[string]interface{}, 0, len(list.Items))
	for i := range list.Items {
		if pool != "" && nodeClaimOwnerPool(&list.Items[i]) != pool {
			continue
		}
		items = append(items, summarizeNodeClaim(&list.Items[i]))
	}
	return okResponse(requestID, map[string]interface{}{"items": items})
}

// scaleNodePool patches spec.limits.cpu / spec.limits.memory. A value of
// "none" removes the limit (unbounded); empty leaves it unchanged.
func (e *KarpenterExecutor) scaleNodePool(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	if params.CPULimit == "" && params.MemoryLimit == "" {
		return errResponse(requestID, http.StatusBadRequest,
			"at least one of cpu_limit or memory_limit is required")
	}
	obj, gvr, err := e.getResource(ctx, nodePoolGVR, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	limits := map[string]interface{}{}
	if params.CPULimit != "" {
		if strings.EqualFold(params.CPULimit, "none") {
			limits["cpu"] = nil
		} else {
			limits["cpu"] = params.CPULimit
		}
	}
	if params.MemoryLimit != "" {
		if strings.EqualFold(params.MemoryLimit, "none") {
			limits["memory"] = nil
		} else {
			limits["memory"] = params.MemoryLimit
		}
	}
	patch, _ := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{"limits": limits},
	})
	updated, err := e.patchResource(ctx, gvr, obj.GetName(), patch)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Karpenter] Scaled NodePool limits",
		zap.String("nodepool", obj.GetName()),
		zap.String("cpu_limit", params.CPULimit),
		zap.String("memory_limit", params.MemoryLimit))
	return okResponse(requestID, map[string]interface{}{
		"status":   "scaled",
		"nodepool": obj.GetName(),
		"limits":   nodePoolLimits(updated),
	})
}

// setDisruption patches spec.disruption.consolidationPolicy and/or
// spec.disruption.consolidateAfter.
func (e *KarpenterExecutor) setDisruption(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	if params.ConsolidationPolicy == "" && params.ConsolidateAfter == "" {
		return errResponse(requestID, http.StatusBadRequest,
			"at least one of consolidation_policy or consolidate_after is required")
	}
	if params.ConsolidationPolicy != "" {
		switch params.ConsolidationPolicy {
		case "WhenEmpty", "WhenEmptyOrUnderutilized", "WhenUnderutilized":
		default:
			return errResponse(requestID, http.StatusBadRequest,
				fmt.Sprintf("invalid consolidation_policy %q (expected WhenEmpty, WhenEmptyOrUnderutilized, or WhenUnderutilized)", params.ConsolidationPolicy))
		}
	}
	obj, gvr, err := e.getResource(ctx, nodePoolGVR, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	disruption := map[string]interface{}{}
	if params.ConsolidationPolicy != "" {
		disruption["consolidationPolicy"] = params.ConsolidationPolicy
	}
	if params.ConsolidateAfter != "" {
		disruption["consolidateAfter"] = params.ConsolidateAfter
	}
	patch, _ := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{"disruption": disruption},
	})
	updated, err := e.patchResource(ctx, gvr, obj.GetName(), patch)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Karpenter] Updated NodePool disruption settings",
		zap.String("nodepool", obj.GetName()),
		zap.String("consolidation_policy", params.ConsolidationPolicy),
		zap.String("consolidate_after", params.ConsolidateAfter))

	result := map[string]interface{}{
		"status":   "disruption_updated",
		"nodepool": obj.GetName(),
	}
	if d, found, _ := unstructured.NestedMap(updated.Object, "spec", "disruption"); found {
		result["disruption"] = d
	}
	return okResponse(requestID, result)
}

// applyNodePool creates or updates a NodePool from a full manifest
// (YAML or JSON). This is the primitive behind "autoscale via Karpenter":
// declaring capacity via a NodePool spec.
func (e *KarpenterExecutor) applyNodePool(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	if strings.TrimSpace(params.NodePoolSpec) == "" {
		return errResponse(requestID, http.StatusBadRequest, "nodepool_spec is required")
	}

	jsonSpec, err := yaml.YAMLToJSON([]byte(params.NodePoolSpec))
	if err != nil {
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("nodepool_spec is not valid YAML/JSON: %v", err))
	}
	var manifest map[string]interface{}
	if err := json.Unmarshal(jsonSpec, &manifest); err != nil {
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("nodepool_spec is not a valid object: %v", err))
	}

	obj := &unstructured.Unstructured{Object: manifest}
	if kind := obj.GetKind(); kind != "" && kind != "NodePool" {
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("nodepool_spec kind must be NodePool, got %q", kind))
	}
	if obj.GetKind() == "" {
		obj.SetKind("NodePool")
	}
	if obj.GetAPIVersion() == "" {
		obj.SetAPIVersion(nodePoolGVR.Group + "/" + nodePoolGVR.Version)
	}
	if obj.GetName() == "" && params.Name != "" {
		obj.SetName(params.Name)
	}
	if obj.GetName() == "" {
		return errResponse(requestID, http.StatusBadRequest, "NodePool name is required (metadata.name or name param)")
	}

	// Resolve the GVR from the manifest's apiVersion so v1beta1 specs work.
	gvr := nodePoolGVR
	if parts := strings.SplitN(obj.GetAPIVersion(), "/", 2); len(parts) == 2 {
		gvr.Group, gvr.Version = parts[0], parts[1]
	}

	existing, err := e.Dynamic.Resource(gvr).Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return k8sErrResponse(requestID, err)
		}
		created, err := e.Dynamic.Resource(gvr).Create(ctx, obj, metav1.CreateOptions{})
		if err != nil {
			return k8sErrResponse(requestID, err)
		}
		e.Logger.Info("[Karpenter] Created NodePool", zap.String("nodepool", created.GetName()))
		return okResponse(requestID, map[string]interface{}{
			"status":   "created",
			"nodepool": created.GetName(),
			"limits":   nodePoolLimits(created),
		})
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	updated, err := e.Dynamic.Resource(gvr).Update(ctx, obj, metav1.UpdateOptions{})
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Karpenter] Updated NodePool", zap.String("nodepool", updated.GetName()))
	return okResponse(requestID, map[string]interface{}{
		"status":   "updated",
		"nodepool": updated.GetName(),
		"limits":   nodePoolLimits(updated),
	})
}

// deleteNodeClaim deletes a NodeClaim, causing Karpenter to gracefully drain
// and replace the backing node (targeted node recycle).
func (e *KarpenterExecutor) deleteNodeClaim(ctx context.Context, requestID string, params karpenterParams) *v1.DatadogQueryResponse {
	obj, gvr, err := e.getResource(ctx, nodeClaimGVR, params.Name)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	nodeName, _, _ := unstructured.NestedString(obj.Object, "status", "nodeName")

	if err := e.Dynamic.Resource(gvr).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Karpenter] Deleted NodeClaim",
		zap.String("nodeclaim", obj.GetName()),
		zap.String("node", nodeName))
	return okResponse(requestID, map[string]interface{}{
		"status":    "deleted",
		"nodeclaim": obj.GetName(),
		"node_name": nodeName,
		"nodepool":  nodeClaimOwnerPool(obj),
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// summarizeNodePool extracts the fields workflows care about: limits,
// current resource usage, disruption settings, weight, and readiness.
func summarizeNodePool(obj *unstructured.Unstructured) map[string]interface{} {
	summary := map[string]interface{}{
		"name": obj.GetName(),
	}
	if limits := nodePoolLimits(obj); limits != nil {
		summary["limits"] = limits
	}
	if resources, found, _ := unstructured.NestedMap(obj.Object, "status", "resources"); found {
		summary["resources"] = resources
	}
	if disruption, found, _ := unstructured.NestedMap(obj.Object, "spec", "disruption"); found {
		summary["disruption"] = disruption
	}
	if weight, found, _ := unstructured.NestedInt64(obj.Object, "spec", "weight"); found {
		summary["weight"] = weight
	}
	if nodeClassRef, found, _ := unstructured.NestedMap(obj.Object, "spec", "template", "spec", "nodeClassRef"); found {
		summary["node_class_ref"] = nodeClassRef
	}
	ready, msg := readyCondition(obj)
	summary["ready"] = ready
	if msg != "" {
		summary["message"] = msg
	}
	return summary
}

// summarizeNodeClaim extracts NodeClaim identity, owner pool, backing node,
// instance details, and readiness.
func summarizeNodeClaim(obj *unstructured.Unstructured) map[string]interface{} {
	summary := map[string]interface{}{
		"name":     obj.GetName(),
		"nodepool": nodeClaimOwnerPool(obj),
		"created":  obj.GetCreationTimestamp().UTC().Format("2006-01-02T15:04:05Z"),
	}
	if nodeName, found, _ := unstructured.NestedString(obj.Object, "status", "nodeName"); found && nodeName != "" {
		summary["node_name"] = nodeName
	}
	if capacity, found, _ := unstructured.NestedMap(obj.Object, "status", "capacity"); found {
		summary["capacity"] = capacity
	}
	labels := obj.GetLabels()
	if instanceType := labels["node.kubernetes.io/instance-type"]; instanceType != "" {
		summary["instance_type"] = instanceType
	}
	if capacityType := labels["karpenter.sh/capacity-type"]; capacityType != "" {
		summary["capacity_type"] = capacityType
	}
	if zone := labels["topology.kubernetes.io/zone"]; zone != "" {
		summary["zone"] = zone
	}
	ready, msg := readyCondition(obj)
	summary["ready"] = ready
	if msg != "" {
		summary["message"] = msg
	}
	return summary
}

func nodePoolLimits(obj *unstructured.Unstructured) map[string]interface{} {
	limits, found, _ := unstructured.NestedMap(obj.Object, "spec", "limits")
	if !found {
		return nil
	}
	return limits
}

// nodeClaimOwnerPool resolves the owning NodePool from the standard label,
// falling back to ownerReferences.
func nodeClaimOwnerPool(obj *unstructured.Unstructured) string {
	if pool := obj.GetLabels()["karpenter.sh/nodepool"]; pool != "" {
		return pool
	}
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "NodePool" {
			return ref.Name
		}
	}
	return ""
}

func nodeClaimIsReady(obj *unstructured.Unstructured) bool {
	ready, _ := readyCondition(obj)
	return ready
}

// readyCondition reads the Ready condition from status.conditions.
func readyCondition(obj *unstructured.Unstructured) (bool, string) {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return false, ""
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if condType, _ := cond["type"].(string); condType == "Ready" {
			status, _ := cond["status"].(string)
			msg, _ := cond["message"].(string)
			return status == "True", msg
		}
	}
	return false, ""
}
