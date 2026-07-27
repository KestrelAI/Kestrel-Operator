package kyverno_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// listPolicies returns summaries of all ClusterPolicies plus namespaced
// Policies (across all namespaces, or one namespace when scoped).
func (e *KyvernoExecutor) listPolicies(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	items := make([]map[string]interface{}, 0)

	if params.Namespace == "" {
		clusterList, err := e.resourceInterface(clusterPolicyGVR, "").List(ctx, listOpts())
		if err != nil {
			return k8sErrResponse(requestID, err)
		}
		for i := range clusterList.Items {
			items = append(items, summarizePolicy(&clusterList.Items[i], "ClusterPolicy"))
		}
	}

	nsList, err := e.resourceInterface(policyGVR, params.Namespace).List(ctx, listOpts())
	if err == nil {
		for i := range nsList.Items {
			items = append(items, summarizePolicy(&nsList.Items[i], "Policy"))
		}
	} else if params.Namespace != "" {
		return k8sErrResponse(requestID, err)
	}

	return okResponse(requestID, map[string]interface{}{"items": items})
}

// getPolicy returns one policy's summary including its rules.
func (e *KyvernoExecutor) getPolicy(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	obj, gvr, err := e.getPolicyResource(ctx, params.Name, params.Namespace)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	kind := "ClusterPolicy"
	if gvr == policyGVR {
		kind = "Policy"
	}
	summary := summarizePolicy(obj, kind)
	summary["rules"] = summarizeRules(obj)
	if desc := obj.GetAnnotations()["policies.kyverno.io/description"]; desc != "" {
		summary["description"] = desc
	}
	return okResponse(requestID, summary)
}

// listViolations aggregates PolicyReport and ClusterPolicyReport results.
// Filters: namespace, policy, severity, and result (fail by default; a
// comma-separated list or "all" widens the selection).
func (e *KyvernoExecutor) listViolations(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	wantResults := parseResultFilter(params.Result)
	maxResults := params.MaxResults
	if maxResults <= 0 {
		maxResults = 200
	}

	violations := make([]map[string]interface{}, 0)
	counts := map[string]int{"pass": 0, "fail": 0, "warn": 0, "error": 0, "skip": 0}

	collect := func(list *unstructured.UnstructuredList) {
		for i := range list.Items {
			report := &list.Items[i]
			accumulateSummary(report, counts)
			results, found, _ := unstructured.NestedSlice(report.Object, "results")
			if !found {
				continue
			}
			for _, r := range results {
				res, ok := r.(map[string]interface{})
				if !ok {
					continue
				}
				entry := summarizeResult(report, res)
				if !matchesViolationFilters(entry, params, wantResults) {
					continue
				}
				if len(violations) < maxResults {
					violations = append(violations, entry)
				}
			}
		}
	}

	if params.Namespace == "" {
		if clusterReports, err := e.listReports(ctx, clusterPolicyReportGVR, ""); err == nil {
			collect(clusterReports)
		}
	}
	nsReports, err := e.listReports(ctx, policyReportGVR, params.Namespace)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	collect(nsReports)

	return okResponse(requestID, map[string]interface{}{
		"violations":      violations,
		"violation_count": len(violations),
		"counts":          counts,
	})
}

// setEnforcement flips a policy between Audit and Enforce. It patches
// spec.validationFailureAction when the policy uses the policy-level field,
// otherwise it updates each rule's validate.failureAction (Kyverno 1.13+
// rule-level style).
func (e *KyvernoExecutor) setEnforcement(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	action := normalizeEnforcementAction(params.EnforcementAction)
	if action == "" {
		return errResponse(requestID, http.StatusBadRequest,
			"enforcement_action must be Audit or Enforce")
	}
	obj, gvr, err := e.getPolicyResource(ctx, params.Name, params.Namespace)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	previous := policyEnforcementAction(obj)

	specAction, hasSpecAction, _ := unstructured.NestedString(obj.Object, "spec", "validationFailureAction")
	if hasSpecAction && specAction != "" {
		patch, _ := json.Marshal(map[string]interface{}{
			"spec": map[string]interface{}{"validationFailureAction": action},
		})
		if _, err := e.patchPolicy(ctx, gvr, obj.GetNamespace(), obj.GetName(), patch); err != nil {
			return k8sErrResponse(requestID, err)
		}
	} else {
		rules, found, _ := unstructured.NestedSlice(obj.Object, "spec", "rules")
		if !found {
			return errResponse(requestID, http.StatusBadRequest,
				"policy has no rules to set an enforcement action on")
		}
		updatedAny := false
		for _, r := range rules {
			rule, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			if validate, ok := rule["validate"].(map[string]interface{}); ok {
				validate["failureAction"] = action
				updatedAny = true
			}
		}
		if !updatedAny {
			return errResponse(requestID, http.StatusBadRequest,
				"policy has no validate rules; enforcement action only applies to validation policies")
		}
		if err := unstructured.SetNestedSlice(obj.Object, rules, "spec", "rules"); err != nil {
			return errResponse(requestID, http.StatusInternalServerError, err.Error())
		}
		if _, err := e.resourceInterface(gvr, obj.GetNamespace()).Update(ctx, obj, updateOpts()); err != nil {
			return k8sErrResponse(requestID, err)
		}
	}

	return okResponse(requestID, map[string]interface{}{
		"status":             "enforcement_updated",
		"policy":             obj.GetName(),
		"namespace":          obj.GetNamespace(),
		"enforcement_action": action,
		"previous_action":    previous,
	})
}

// applyPolicy creates or updates a ClusterPolicy/Policy from a full manifest
// (YAML or JSON). This is the primitive behind "deploy a guardrail":
// declaring admission rules as a Kyverno policy.
func (e *KyvernoExecutor) applyPolicy(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	if strings.TrimSpace(params.PolicySpec) == "" {
		return errResponse(requestID, http.StatusBadRequest, "policy_spec is required")
	}

	jsonSpec, err := yaml.YAMLToJSON([]byte(params.PolicySpec))
	if err != nil {
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("policy_spec is not valid YAML/JSON: %v", err))
	}
	var manifest map[string]interface{}
	if err := json.Unmarshal(jsonSpec, &manifest); err != nil {
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("policy_spec is not a valid object: %v", err))
	}

	obj := &unstructured.Unstructured{Object: manifest}
	switch obj.GetKind() {
	case "":
		obj.SetKind("ClusterPolicy")
	case "ClusterPolicy", "Policy":
	default:
		return errResponse(requestID, http.StatusBadRequest,
			fmt.Sprintf("policy_spec kind must be ClusterPolicy or Policy, got %q", obj.GetKind()))
	}
	if obj.GetAPIVersion() == "" {
		obj.SetAPIVersion(clusterPolicyGVR.Group + "/" + clusterPolicyGVR.Version)
	}
	if obj.GetName() == "" && params.Name != "" {
		obj.SetName(params.Name)
	}
	if obj.GetName() == "" {
		return errResponse(requestID, http.StatusBadRequest,
			"policy name is required (metadata.name or name param)")
	}

	gvr := clusterPolicyGVR
	namespace := ""
	if obj.GetKind() == "Policy" {
		gvr = policyGVR
		namespace = obj.GetNamespace()
		if namespace == "" {
			namespace = params.Namespace
		}
		if namespace == "" {
			return errResponse(requestID, http.StatusBadRequest,
				"namespace is required for a namespaced Policy")
		}
		obj.SetNamespace(namespace)
	}
	// Resolve the GVR version from the manifest's apiVersion so older specs work.
	if parts := strings.SplitN(obj.GetAPIVersion(), "/", 2); len(parts) == 2 {
		gvr.Group, gvr.Version = parts[0], parts[1]
	}

	existing, err := e.resourceInterface(gvr, namespace).Get(ctx, obj.GetName(), getOpts())
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return k8sErrResponse(requestID, err)
		}
		created, err := e.resourceInterface(gvr, namespace).Create(ctx, obj, createOpts())
		if err != nil {
			return k8sErrResponse(requestID, err)
		}
		e.Logger.Info("[Kyverno] Created policy",
			zap.String("policy", created.GetName()), zap.String("kind", created.GetKind()))
		return okResponse(requestID, map[string]interface{}{
			"status":             "created",
			"policy":             created.GetName(),
			"kind":               created.GetKind(),
			"namespace":          created.GetNamespace(),
			"enforcement_action": policyEnforcementAction(created),
		})
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	updated, err := e.resourceInterface(gvr, namespace).Update(ctx, obj, updateOpts())
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Kyverno] Updated policy",
		zap.String("policy", updated.GetName()), zap.String("kind", updated.GetKind()))
	return okResponse(requestID, map[string]interface{}{
		"status":             "updated",
		"policy":             updated.GetName(),
		"kind":               updated.GetKind(),
		"namespace":          updated.GetNamespace(),
		"enforcement_action": policyEnforcementAction(updated),
	})
}

// deletePolicy deletes a ClusterPolicy (or a namespaced Policy when a
// namespace is provided).
func (e *KyvernoExecutor) deletePolicy(ctx context.Context, requestID string, params kyvernoParams) *v1.DatadogQueryResponse {
	obj, gvr, err := e.getPolicyResource(ctx, params.Name, params.Namespace)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	kind := "ClusterPolicy"
	if gvr == policyGVR {
		kind = "Policy"
	}
	if err := e.resourceInterface(gvr, obj.GetNamespace()).Delete(ctx, obj.GetName(), deleteOpts()); err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Kyverno] Deleted policy",
		zap.String("policy", obj.GetName()), zap.String("kind", kind))
	return okResponse(requestID, map[string]interface{}{
		"status":    "deleted",
		"policy":    obj.GetName(),
		"kind":      kind,
		"namespace": obj.GetNamespace(),
	})
}

