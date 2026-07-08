package karpenter_executor

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newFakeExecutor builds an executor backed by a fake dynamic client seeded
// with the given objects.
func newFakeExecutor(t *testing.T, objects ...runtime.Object) *KarpenterExecutor {
	t.Helper()
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		nodePoolGVR:  "NodePoolList",
		nodeClaimGVR: "NodeClaimList",
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objects...)
	return NewKarpenterExecutor(zap.NewNop(), client)
}

func nodePool(name string, cpuLimit string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "karpenter.sh/v1",
			"kind":       "NodePool",
			"metadata":   map[string]interface{}{"name": name},
			"spec": map[string]interface{}{
				"disruption": map[string]interface{}{
					"consolidationPolicy": "WhenEmptyOrUnderutilized",
				},
			},
			"status": map[string]interface{}{
				"resources": map[string]interface{}{"cpu": "8", "nodes": "2"},
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "True"},
				},
			},
		},
	}
	if cpuLimit != "" {
		_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{"cpu": cpuLimit}, "spec", "limits")
	}
	return obj
}

func nodeClaim(name, pool, nodeName string, ready bool) *unstructured.Unstructured {
	status := "False"
	if ready {
		status = "True"
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "karpenter.sh/v1",
			"kind":       "NodeClaim",
			"metadata": map[string]interface{}{
				"name": name,
				"labels": map[string]interface{}{
					"karpenter.sh/nodepool":            pool,
					"node.kubernetes.io/instance-type": "m5.large",
					"karpenter.sh/capacity-type":       "spot",
				},
				"creationTimestamp": "2026-07-01T00:00:00Z",
			},
			"status": map[string]interface{}{
				"nodeName": nodeName,
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": status},
				},
			},
		},
	}
}

func execQuery(e *KarpenterExecutor, qt v1.DatadogQueryType, filter string, body map[string]interface{}) *v1.DatadogQueryResponse {
	jsonBody := ""
	if body != nil {
		b, _ := json.Marshal(body)
		jsonBody = string(b)
	}
	return e.ExecuteQuery(context.Background(), &v1.DatadogQueryRequest{
		RequestId: "req-1",
		QueryType: qt,
		Filter:    filter,
		JsonBody:  jsonBody,
	})
}

func decodeResponse(t *testing.T, resp *v1.DatadogQueryResponse) map[string]interface{} {
	t.Helper()
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.ErrorMessage)
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(resp.ResponseData), &data); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	return data
}

// ---------------------------------------------------------------------------
// Probe / availability
// ---------------------------------------------------------------------------

func TestProbe_AvailableWhenCRDServed(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	if !e.Probe(context.Background()) {
		t.Fatal("expected probe to succeed when NodePool CRD is served")
	}
	if !e.IsAvailable() {
		t.Fatal("expected IsAvailable after successful probe")
	}
}

func TestIsAvailable_FalseBeforeProbe(t *testing.T) {
	e := newFakeExecutor(t)
	if e.IsAvailable() {
		t.Fatal("expected IsAvailable to be false before probing")
	}
}

// ---------------------------------------------------------------------------
// List / get operations
// ---------------------------------------------------------------------------

func TestListNodePools(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"), nodePool("gpu-pool", "50"))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_LIST_NODEPOOLS, "", nil)
	data := decodeResponse(t, resp)
	items, ok := data["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 nodepools, got %v", data["items"])
	}
	first := items[0].(map[string]interface{})
	if first["ready"] != true {
		t.Errorf("expected ready=true from Ready condition, got %v", first["ready"])
	}
	if first["limits"] == nil {
		t.Error("expected limits in summary")
	}
}

func TestGetNodePoolStatus_IncludesNodeClaimCounts(t *testing.T) {
	e := newFakeExecutor(t,
		nodePool("default", "100"),
		nodeClaim("default-abc12", "default", "node-1", true),
		nodeClaim("default-def34", "default", "node-2", false),
		nodeClaim("gpu-xyz", "gpu-pool", "node-3", true),
	)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_GET_NODEPOOL_STATUS, "default", nil)
	data := decodeResponse(t, resp)
	if data["name"] != "default" {
		t.Errorf("name = %v", data["name"])
	}
	if data["nodeclaim_count"] != float64(2) {
		t.Errorf("nodeclaim_count = %v, want 2", data["nodeclaim_count"])
	}
	if data["ready_nodeclaim_count"] != float64(1) {
		t.Errorf("ready_nodeclaim_count = %v, want 1", data["ready_nodeclaim_count"])
	}
}

func TestGetNodePoolStatus_NotFound(t *testing.T) {
	e := newFakeExecutor(t)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_GET_NODEPOOL_STATUS, "missing", nil)
	if resp.Success {
		t.Fatal("expected failure for missing NodePool")
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestListNodeClaims_FilteredByNodePool(t *testing.T) {
	e := newFakeExecutor(t,
		nodeClaim("default-abc12", "default", "node-1", true),
		nodeClaim("gpu-xyz", "gpu-pool", "node-3", true),
	)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_LIST_NODECLAIMS, "", map[string]interface{}{"nodepool": "gpu-pool"})
	data := decodeResponse(t, resp)
	items := data["items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("expected 1 filtered nodeclaim, got %d", len(items))
	}
	claim := items[0].(map[string]interface{})
	if claim["name"] != "gpu-xyz" || claim["nodepool"] != "gpu-pool" {
		t.Errorf("unexpected claim: %v", claim)
	}
	if claim["instance_type"] != "m5.large" || claim["capacity_type"] != "spot" {
		t.Errorf("missing instance labels: %v", claim)
	}
}

func TestListNodeClaims_Unfiltered(t *testing.T) {
	e := newFakeExecutor(t,
		nodeClaim("default-abc12", "default", "node-1", true),
		nodeClaim("gpu-xyz", "gpu-pool", "node-3", true),
	)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_LIST_NODECLAIMS, "", nil)
	data := decodeResponse(t, resp)
	if items := data["items"].([]interface{}); len(items) != 2 {
		t.Fatalf("expected 2 nodeclaims, got %d", len(items))
	}
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

func TestScaleNodePool_SetsLimits(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_SCALE_NODEPOOL, "default",
		map[string]interface{}{"cpu_limit": "200", "memory_limit": "800Gi"})
	data := decodeResponse(t, resp)
	if data["status"] != "scaled" {
		t.Errorf("status = %v", data["status"])
	}
	limits := data["limits"].(map[string]interface{})
	if limits["cpu"] != "200" || limits["memory"] != "800Gi" {
		t.Errorf("limits = %v", limits)
	}
}

func TestScaleNodePool_RequiresAtLeastOneLimit(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_SCALE_NODEPOOL, "default", nil)
	if resp.Success || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing limits, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

func TestSetDisruption_UpdatesPolicy(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_SET_DISRUPTION, "default",
		map[string]interface{}{"consolidation_policy": "WhenEmpty", "consolidate_after": "5m"})
	data := decodeResponse(t, resp)
	if data["status"] != "disruption_updated" {
		t.Errorf("status = %v", data["status"])
	}
	disruption := data["disruption"].(map[string]interface{})
	if disruption["consolidationPolicy"] != "WhenEmpty" || disruption["consolidateAfter"] != "5m" {
		t.Errorf("disruption = %v", disruption)
	}
}

func TestSetDisruption_RejectsInvalidPolicy(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_SET_DISRUPTION, "default",
		map[string]interface{}{"consolidation_policy": "Whenever"})
	if resp.Success || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid policy, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

func TestApplyNodePool_CreatesWhenMissing(t *testing.T) {
	e := newFakeExecutor(t)
	spec := `apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: new-pool
spec:
  limits:
    cpu: "64"
`
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_APPLY_NODEPOOL, "",
		map[string]interface{}{"nodepool_spec": spec})
	data := decodeResponse(t, resp)
	if data["status"] != "created" || data["nodepool"] != "new-pool" {
		t.Errorf("unexpected apply result: %v", data)
	}
}

func TestApplyNodePool_UpdatesWhenExists(t *testing.T) {
	e := newFakeExecutor(t, nodePool("default", "100"))
	spec := `apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: default
spec:
  limits:
    cpu: "500"
`
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_APPLY_NODEPOOL, "",
		map[string]interface{}{"nodepool_spec": spec})
	data := decodeResponse(t, resp)
	if data["status"] != "updated" {
		t.Errorf("status = %v, want updated", data["status"])
	}
	limits := data["limits"].(map[string]interface{})
	if limits["cpu"] != "500" {
		t.Errorf("limits = %v", limits)
	}
}

func TestApplyNodePool_RejectsWrongKind(t *testing.T) {
	e := newFakeExecutor(t)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_APPLY_NODEPOOL, "",
		map[string]interface{}{"nodepool_spec": "kind: Deployment\nmetadata:\n  name: x"})
	if resp.Success || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for wrong kind, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

func TestApplyNodePool_RequiresSpec(t *testing.T) {
	e := newFakeExecutor(t)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_APPLY_NODEPOOL, "", nil)
	if resp.Success || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing spec, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

func TestDeleteNodeClaim(t *testing.T) {
	e := newFakeExecutor(t, nodeClaim("default-abc12", "default", "node-1", true))
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_DELETE_NODECLAIM, "default-abc12", nil)
	data := decodeResponse(t, resp)
	if data["status"] != "deleted" || data["nodeclaim"] != "default-abc12" {
		t.Errorf("unexpected delete result: %v", data)
	}
	if data["node_name"] != "node-1" || data["nodepool"] != "default" {
		t.Errorf("missing node/pool context: %v", data)
	}

	// The claim is actually gone.
	_, err := e.Dynamic.Resource(nodeClaimGVR).Get(context.Background(), "default-abc12", metav1.GetOptions{})
	if err == nil {
		t.Error("expected NodeClaim to be deleted from the fake cluster")
	}
}

func TestDeleteNodeClaim_NotFound(t *testing.T) {
	e := newFakeExecutor(t)
	resp := execQuery(e, v1.DatadogQueryType_KARPENTER_DELETE_NODECLAIM, "missing", nil)
	if resp.Success || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Dispatch / params
// ---------------------------------------------------------------------------

func TestExecuteQuery_UnsupportedType(t *testing.T) {
	e := newFakeExecutor(t)
	resp := execQuery(e, v1.DatadogQueryType_DATADOG_QUERY_METRICS, "", nil)
	if resp.Success || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported type, got success=%v status=%d", resp.Success, resp.StatusCode)
	}
}

func TestParseParams_JSONBodyOverridesFilter(t *testing.T) {
	e := newFakeExecutor(t)
	params := e.parseParams(&v1.DatadogQueryRequest{
		Filter:   "from-filter",
		JsonBody: `{"name":"from-body","cpu_limit":"10"}`,
	})
	if params.Name != "from-body" || params.CPULimit != "10" {
		t.Errorf("params = %+v", params)
	}

	// Filter is the fallback when json_body has no name.
	params = e.parseParams(&v1.DatadogQueryRequest{Filter: "  padded-name "})
	if params.Name != "padded-name" {
		t.Errorf("expected trimmed filter fallback, got %q", params.Name)
	}
}
