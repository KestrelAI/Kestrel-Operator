package kyverno_executor

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Option helpers keep call sites terse.
func listOpts() metav1.ListOptions     { return metav1.ListOptions{} }
func getOpts() metav1.GetOptions       { return metav1.GetOptions{} }
func createOpts() metav1.CreateOptions { return metav1.CreateOptions{} }
func updateOpts() metav1.UpdateOptions { return metav1.UpdateOptions{} }
func deleteOpts() metav1.DeleteOptions { return metav1.DeleteOptions{} }

// summarizePolicy extracts the fields workflows care about: enforcement mode,
// background scanning, rule count, readiness, and well-known annotations.
func summarizePolicy(obj *unstructured.Unstructured, kind string) map[string]interface{} {
	summary := map[string]interface{}{
		"name": obj.GetName(),
		"kind": kind,
	}
	if ns := obj.GetNamespace(); ns != "" {
		summary["namespace"] = ns
	}
	summary["enforcement_action"] = policyEnforcementAction(obj)
	if background, found, _ := unstructured.NestedBool(obj.Object, "spec", "background"); found {
		summary["background"] = background
	} else {
		summary["background"] = true
	}
	if rules, found, _ := unstructured.NestedSlice(obj.Object, "spec", "rules"); found {
		summary["rule_count"] = len(rules)
	}
	annotations := obj.GetAnnotations()
	if category := annotations["policies.kyverno.io/category"]; category != "" {
		summary["category"] = category
	}
	if severity := annotations["policies.kyverno.io/severity"]; severity != "" {
		summary["severity"] = severity
	}
	ready, msg := readyCondition(obj)
	summary["ready"] = ready
	if msg != "" {
		summary["message"] = msg
	}
	return summary
}

// policyEnforcementAction resolves the effective enforcement mode: the
// policy-level spec.validationFailureAction, or the first rule-level
// validate.failureAction (Kyverno 1.13+ style). Defaults to Audit, which is
// Kyverno's own default.
func policyEnforcementAction(obj *unstructured.Unstructured) string {
	if action, found, _ := unstructured.NestedString(obj.Object, "spec", "validationFailureAction"); found && action != "" {
		return action
	}
	if rules, found, _ := unstructured.NestedSlice(obj.Object, "spec", "rules"); found {
		for _, r := range rules {
			rule, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			if validate, ok := rule["validate"].(map[string]interface{}); ok {
				if action, ok := validate["failureAction"].(string); ok && action != "" {
					return action
				}
			}
		}
	}
	return "Audit"
}

// summarizeRules extracts each rule's name, type, and matched resource kinds.
func summarizeRules(obj *unstructured.Unstructured) []map[string]interface{} {
	rules, found, _ := unstructured.NestedSlice(obj.Object, "spec", "rules")
	if !found {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(rules))
	for _, r := range rules {
		rule, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		entry := map[string]interface{}{}
		if name, ok := rule["name"].(string); ok {
			entry["name"] = name
		}
		entry["type"] = ruleType(rule)
		if kinds := ruleMatchKinds(rule); len(kinds) > 0 {
			entry["match_kinds"] = kinds
		}
		if validate, ok := rule["validate"].(map[string]interface{}); ok {
			if msg, ok := validate["message"].(string); ok && msg != "" {
				entry["message"] = msg
			}
		}
		out = append(out, entry)
	}
	return out
}

func ruleType(rule map[string]interface{}) string {
	for _, t := range []string{"validate", "mutate", "generate", "verifyImages"} {
		if _, ok := rule[t]; ok {
			return t
		}
	}
	return "unknown"
}

// ruleMatchKinds reads match.any[].resources.kinds plus the legacy
// match.resources.kinds form.
func ruleMatchKinds(rule map[string]interface{}) []string {
	kinds := []string{}
	appendKinds := func(resources map[string]interface{}) {
		rawKinds, ok := resources["kinds"].([]interface{})
		if !ok {
			return
		}
		for _, k := range rawKinds {
			if s, ok := k.(string); ok {
				kinds = append(kinds, s)
			}
		}
	}
	match, ok := rule["match"].(map[string]interface{})
	if !ok {
		return kinds
	}
	if resources, ok := match["resources"].(map[string]interface{}); ok {
		appendKinds(resources)
	}
	for _, listKey := range []string{"any", "all"} {
		entries, ok := match[listKey].([]interface{})
		if !ok {
			continue
		}
		for _, entry := range entries {
			m, ok := entry.(map[string]interface{})
			if !ok {
				continue
			}
			if resources, ok := m["resources"].(map[string]interface{}); ok {
				appendKinds(resources)
			}
		}
	}
	return kinds
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

// accumulateSummary adds a report's summary block into the aggregate counts.
func accumulateSummary(report *unstructured.Unstructured, counts map[string]int) {
	summary, found, _ := unstructured.NestedMap(report.Object, "summary")
	if !found {
		return
	}
	for key := range counts {
		if val, ok := summary[key].(int64); ok {
			counts[key] += int(val)
		} else if val, ok := summary[key].(float64); ok {
			counts[key] += int(val)
		}
	}
}

// summarizeResult flattens one PolicyReport result entry into the violation
// shape returned to the server.
func summarizeResult(report *unstructured.Unstructured, res map[string]interface{}) map[string]interface{} {
	entry := map[string]interface{}{}
	for _, key := range []string{"policy", "rule", "result", "severity", "message", "category", "source"} {
		if val, ok := res[key].(string); ok && val != "" {
			entry[key] = val
		}
	}
	// Resolve the violated resource from the result, falling back to the
	// report's scope (Kyverno emits one report per resource).
	if resources, ok := res["resources"].([]interface{}); ok && len(resources) > 0 {
		if resource, ok := resources[0].(map[string]interface{}); ok {
			setResourceFields(entry, resource)
		}
	}
	if _, ok := entry["resource_name"]; !ok {
		if scope, found, _ := unstructured.NestedMap(report.Object, "scope"); found {
			setResourceFields(entry, scope)
		}
	}
	if _, ok := entry["resource_namespace"]; !ok {
		if ns := report.GetNamespace(); ns != "" {
			entry["resource_namespace"] = ns
		}
	}
	return entry
}

func setResourceFields(entry map[string]interface{}, resource map[string]interface{}) {
	if kind, ok := resource["kind"].(string); ok && kind != "" {
		entry["resource_kind"] = kind
	}
	if name, ok := resource["name"].(string); ok && name != "" {
		entry["resource_name"] = name
	}
	if ns, ok := resource["namespace"].(string); ok && ns != "" {
		entry["resource_namespace"] = ns
	}
}

// parseResultFilter turns the result param into a lookup set. Empty means
// fail only; "all" (or "*") disables filtering.
func parseResultFilter(result string) map[string]bool {
	result = strings.TrimSpace(strings.ToLower(result))
	if result == "" {
		return map[string]bool{"fail": true}
	}
	if result == "all" || result == "*" {
		return nil
	}
	want := map[string]bool{}
	for _, part := range strings.Split(result, ",") {
		if part = strings.TrimSpace(part); part != "" {
			want[part] = true
		}
	}
	if len(want) == 0 {
		return map[string]bool{"fail": true}
	}
	return want
}

func matchesViolationFilters(entry map[string]interface{}, params kyvernoParams, wantResults map[string]bool) bool {
	if wantResults != nil {
		result, _ := entry["result"].(string)
		if !wantResults[strings.ToLower(result)] {
			return false
		}
	}
	if params.Policy != "" {
		policy, _ := entry["policy"].(string)
		if !strings.EqualFold(policy, params.Policy) {
			return false
		}
	}
	if params.Severity != "" {
		severity, _ := entry["severity"].(string)
		if !strings.EqualFold(severity, params.Severity) {
			return false
		}
	}
	return true
}

// normalizeEnforcementAction canonicalizes audit/enforce input.
func normalizeEnforcementAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "audit":
		return "Audit"
	case "enforce":
		return "Enforce"
	default:
		return ""
	}
}
