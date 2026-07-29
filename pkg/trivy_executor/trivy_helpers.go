package trivy_executor

import (
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Option helpers keep call sites terse.
func getOpts() metav1.GetOptions       { return metav1.GetOptions{} }
func deleteOpts() metav1.DeleteOptions { return metav1.DeleteOptions{} }

// canonicalKind maps kind aliases to the values trivy-operator stamps in its
// resource-kind label (e.g. "deployment" -> "Deployment").
func canonicalKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "deployment", "deploy":
		return "Deployment"
	case "statefulset", "sts":
		return "StatefulSet"
	case "daemonset", "ds":
		return "DaemonSet"
	case "replicaset", "rs":
		return "ReplicaSet"
	case "cronjob":
		return "CronJob"
	case "job":
		return "Job"
	case "pod":
		return "Pod"
	case "role":
		return "Role"
	case "clusterrole":
		return "ClusterRole"
	default:
		return strings.TrimSpace(kind)
	}
}

// reportWorkload resolves the scanned workload from the report's
// trivy-operator labels.
func reportWorkload(report *unstructured.Unstructured) (kind, name string) {
	labels := report.GetLabels()
	return labels[labelResourceKind], labels[labelResourceName]
}

// imageRef reconstructs the scanned image reference from report.artifact.
func imageRef(report *unstructured.Unstructured) string {
	repo, _, _ := unstructured.NestedString(report.Object, "report", "artifact", "repository")
	if repo == "" {
		return ""
	}
	if registry, _, _ := unstructured.NestedString(report.Object, "report", "registry", "server"); registry != "" && registry != "index.docker.io" {
		repo = registry + "/" + repo
	}
	if tag, _, _ := unstructured.NestedString(report.Object, "report", "artifact", "tag"); tag != "" {
		return repo + ":" + tag
	}
	if digest, _, _ := unstructured.NestedString(report.Object, "report", "artifact", "digest"); digest != "" {
		return repo + "@" + digest
	}
	return repo
}

// accumulateVulnSummary adds a VulnerabilityReport's summary block into the
// aggregate per-severity counts.
func accumulateVulnSummary(report *unstructured.Unstructured, counts map[string]int) {
	summary, found, _ := unstructured.NestedMap(report.Object, "report", "summary")
	if !found {
		return
	}
	for key, field := range map[string]string{
		"critical": "criticalCount",
		"high":     "highCount",
		"medium":   "mediumCount",
		"low":      "lowCount",
		"unknown":  "unknownCount",
	} {
		if val, ok := summary[field].(int64); ok {
			counts[key] += int(val)
		} else if val, ok := summary[field].(float64); ok {
			counts[key] += int(val)
		}
	}
}

// summarizeVulnerability flattens one report vulnerability entry into the
// finding shape returned to the server.
func summarizeVulnerability(vuln map[string]interface{}) map[string]interface{} {
	entry := map[string]interface{}{}
	for key, out := range map[string]string{
		"vulnerabilityID":  "cve_id",
		"severity":         "severity",
		"title":            "title",
		"resource":         "package",
		"installedVersion": "installed_version",
		"fixedVersion":     "fixed_version",
		"primaryLink":      "link",
	} {
		if val, ok := vuln[key].(string); ok && val != "" {
			entry[out] = val
		}
	}
	if score, ok := vuln["score"].(float64); ok && score > 0 {
		entry["score"] = score
	}
	return entry
}

// summarizeCheck flattens one config-audit/rbac/infra check into the
// misconfiguration shape returned to the server.
func summarizeCheck(check map[string]interface{}) map[string]interface{} {
	entry := map[string]interface{}{}
	for key, out := range map[string]string{
		"checkID":     "check_id",
		"severity":    "severity",
		"title":       "title",
		"category":    "category",
		"description": "description",
		"remediation": "remediation",
	} {
		if val, ok := check[key].(string); ok && val != "" {
			entry[out] = val
		}
	}
	if messages, ok := check["messages"].([]interface{}); ok && len(messages) > 0 {
		if msg, ok := messages[0].(string); ok && msg != "" {
			entry["message"] = msg
		}
	}
	return entry
}

// complianceSummary reads status.summary pass/fail counts from a
// ClusterComplianceReport.
func complianceSummary(report *unstructured.Unstructured) (passCount, failCount int) {
	summary, found, _ := unstructured.NestedMap(report.Object, "status", "summary")
	if !found {
		return 0, 0
	}
	readInt := func(key string) int {
		if val, ok := summary[key].(int64); ok {
			return int(val)
		}
		if val, ok := summary[key].(float64); ok {
			return int(val)
		}
		return 0
	}
	return readInt("passCount"), readInt("failCount")
}

// complianceFailedControls extracts controls with failures from
// status.summaryReport.controlCheck (summary mode, the trivy-operator
// default).
func complianceFailedControls(report *unstructured.Unstructured) []map[string]interface{} {
	controls, found, _ := unstructured.NestedSlice(report.Object, "status", "summaryReport", "controlCheck")
	if !found {
		return []map[string]interface{}{}
	}
	failed := make([]map[string]interface{}, 0)
	for _, raw := range controls {
		control, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		totalFail := 0
		if val, ok := control["totalFail"].(int64); ok {
			totalFail = int(val)
		} else if val, ok := control["totalFail"].(float64); ok {
			totalFail = int(val)
		}
		if totalFail <= 0 {
			continue
		}
		entry := map[string]interface{}{"total_fail": totalFail}
		for key, out := range map[string]string{
			"id":       "control_id",
			"name":     "name",
			"severity": "severity",
		} {
			if val, ok := control[key].(string); ok && val != "" {
				entry[out] = val
			}
		}
		failed = append(failed, entry)
	}
	return failed
}

// reportKindsFor maps the report_kind param to the GVRs to aggregate.
func reportKindsFor(reportKind string) map[string]schema.GroupVersionResource {
	switch strings.ToLower(strings.TrimSpace(reportKind)) {
	case "", "all":
		return map[string]schema.GroupVersionResource{
			"config-audit":     configAuditReportGVR,
			"rbac-assessment":  rbacAssessmentReportGVR,
			"infra-assessment": infraAssessmentReportGVR,
		}
	case "config-audit", "configaudit":
		return map[string]schema.GroupVersionResource{"config-audit": configAuditReportGVR}
	case "rbac-assessment", "rbacassessment", "rbac":
		return map[string]schema.GroupVersionResource{"rbac-assessment": rbacAssessmentReportGVR}
	case "infra-assessment", "infraassessment", "infra":
		return map[string]schema.GroupVersionResource{"infra-assessment": infraAssessmentReportGVR}
	default:
		return nil
	}
}

// parseSeverityFilter turns the severity param into a lookup set. Empty means
// no filtering; "all" (or "*") also disables filtering.
func parseSeverityFilter(severity string) map[string]bool {
	severity = strings.TrimSpace(strings.ToLower(severity))
	if severity == "" || severity == "all" || severity == "*" {
		return nil
	}
	want := map[string]bool{}
	for _, part := range strings.Split(severity, ",") {
		if part = strings.TrimSpace(part); part != "" {
			want[part] = true
		}
	}
	if len(want) == 0 {
		return nil
	}
	return want
}

// severityRank orders findings for stable, highest-first output.
func severityRank(severity string) int {
	switch strings.ToLower(severity) {
	case "critical":
		return 0
	case "high":
		return 1
	case "medium":
		return 2
	case "low":
		return 3
	default:
		return 4
	}
}

// sortBySeverity sorts findings critical-first (stable within a severity).
func sortBySeverity(entries []map[string]interface{}) {
	sort.SliceStable(entries, func(i, j int) bool {
		sevA, _ := entries[i]["severity"].(string)
		sevB, _ := entries[j]["severity"].(string)
		return severityRank(sevA) < severityRank(sevB)
	})
}

// jsonKey converts trivy-operator camelCase field names used in exposed
// secret findings to snake_case output keys.
func jsonKey(key string) string {
	switch key {
	case "ruleID":
		return "rule_id"
	default:
		return key
	}
}
