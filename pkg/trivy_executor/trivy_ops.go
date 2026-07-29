package trivy_executor

import (
	"context"
	"net/http"
	"sort"
	"strings"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// listVulnerabilities aggregates VulnerabilityReports across the cluster (or
// one namespace / one workload), filtered by severity and fixability, with
// per-severity totals.
func (e *TrivyExecutor) listVulnerabilities(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	wantSeverities := parseSeverityFilter(params.Severity)
	maxResults := params.MaxResults
	if maxResults <= 0 {
		maxResults = 200
	}

	list, err := e.listReports(ctx, vulnerabilityReportGVR, params.Namespace, workloadSelector(params))
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	vulnerabilities := make([]map[string]interface{}, 0)
	counts := map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0, "unknown": 0}
	reportCount := 0

	for i := range list.Items {
		report := &list.Items[i]
		reportCount++
		accumulateVulnSummary(report, counts)
		image := imageRef(report)
		workloadKind, workloadName := reportWorkload(report)
		vulns, found, _ := unstructured.NestedSlice(report.Object, "report", "vulnerabilities")
		if !found {
			continue
		}
		for _, raw := range vulns {
			vuln, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			severity, _ := vuln["severity"].(string)
			if wantSeverities != nil && !wantSeverities[strings.ToLower(severity)] {
				continue
			}
			fixed, _ := vuln["fixedVersion"].(string)
			if params.FixedOnly && fixed == "" {
				continue
			}
			if len(vulnerabilities) >= maxResults {
				continue
			}
			entry := summarizeVulnerability(vuln)
			entry["image"] = image
			entry["workload_kind"] = workloadKind
			entry["workload_name"] = workloadName
			if ns := report.GetNamespace(); ns != "" {
				entry["namespace"] = ns
			}
			vulnerabilities = append(vulnerabilities, entry)
		}
	}

	sortBySeverity(vulnerabilities)
	return okResponse(requestID, map[string]interface{}{
		"vulnerabilities":     vulnerabilities,
		"vulnerability_count": len(vulnerabilities),
		"counts":              counts,
		"report_count":        reportCount,
	})
}

// getVulnerabilityReport returns the merged vulnerability picture for one
// workload: reports are per-container, so multiple reports may match.
func (e *TrivyExecutor) getVulnerabilityReport(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	if params.Workload == "" {
		return errResponse(requestID, http.StatusBadRequest, "workload is required")
	}
	if params.Namespace == "" {
		return errResponse(requestID, http.StatusBadRequest, "namespace is required")
	}

	list, err := e.listReports(ctx, vulnerabilityReportGVR, params.Namespace, workloadSelector(params))
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	if len(list.Items) == 0 {
		return errResponse(requestID, http.StatusNotFound,
			"no VulnerabilityReport found for workload "+params.Workload+" in namespace "+params.Namespace+
				" (trivy-operator may not have scanned it yet)")
	}

	counts := map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0, "unknown": 0}
	cveIDs := make([]string, 0)
	seen := map[string]bool{}
	containers := make([]map[string]interface{}, 0, len(list.Items))
	workloadKind, workloadName := reportWorkload(&list.Items[0])

	for i := range list.Items {
		report := &list.Items[i]
		accumulateVulnSummary(report, counts)
		container := map[string]interface{}{
			"container": report.GetLabels()[labelContainer],
			"image":     imageRef(report),
		}
		vulns, found, _ := unstructured.NestedSlice(report.Object, "report", "vulnerabilities")
		if found {
			top := make([]map[string]interface{}, 0)
			for _, raw := range vulns {
				vuln, ok := raw.(map[string]interface{})
				if !ok {
					continue
				}
				if id, ok := vuln["vulnerabilityID"].(string); ok && id != "" && !seen[id] {
					seen[id] = true
					cveIDs = append(cveIDs, id)
				}
				if len(top) < 50 {
					top = append(top, summarizeVulnerability(vuln))
				}
			}
			sortBySeverity(top)
			container["vulnerabilities"] = top
		}
		containers = append(containers, container)
	}

	return okResponse(requestID, map[string]interface{}{
		"workload_kind": workloadKind,
		"workload_name": workloadName,
		"namespace":     params.Namespace,
		"counts":        counts,
		"cve_ids":       cveIDs,
		"cve_count":     len(cveIDs),
		"containers":    containers,
	})
}

// listExposedSecrets aggregates ExposedSecretReports (secrets baked into
// container images).
func (e *TrivyExecutor) listExposedSecrets(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	wantSeverities := parseSeverityFilter(params.Severity)
	maxResults := params.MaxResults
	if maxResults <= 0 {
		maxResults = 200
	}

	list, err := e.listReports(ctx, exposedSecretReportGVR, params.Namespace, workloadSelector(params))
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	secrets := make([]map[string]interface{}, 0)
	counts := map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0}

	for i := range list.Items {
		report := &list.Items[i]
		image := imageRef(report)
		workloadKind, workloadName := reportWorkload(report)
		findings, found, _ := unstructured.NestedSlice(report.Object, "report", "secrets")
		if !found {
			continue
		}
		for _, raw := range findings {
			secret, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			severity, _ := secret["severity"].(string)
			if sev := strings.ToLower(severity); sev != "" {
				if _, tracked := counts[sev]; tracked {
					counts[sev]++
				}
			}
			if wantSeverities != nil && !wantSeverities[strings.ToLower(severity)] {
				continue
			}
			if len(secrets) >= maxResults {
				continue
			}
			entry := map[string]interface{}{}
			for _, key := range []string{"ruleID", "category", "severity", "title", "target", "match"} {
				if val, ok := secret[key].(string); ok && val != "" {
					entry[jsonKey(key)] = val
				}
			}
			entry["image"] = image
			entry["workload_kind"] = workloadKind
			entry["workload_name"] = workloadName
			if ns := report.GetNamespace(); ns != "" {
				entry["namespace"] = ns
			}
			secrets = append(secrets, entry)
		}
	}

	return okResponse(requestID, map[string]interface{}{
		"secrets":      secrets,
		"secret_count": len(secrets),
		"counts":       counts,
	})
}

// listMisconfigurations aggregates failed checks from ConfigAuditReports,
// RbacAssessmentReports, and InfraAssessmentReports.
func (e *TrivyExecutor) listMisconfigurations(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	wantSeverities := parseSeverityFilter(params.Severity)
	maxResults := params.MaxResults
	if maxResults <= 0 {
		maxResults = 200
	}

	kinds := reportKindsFor(params.ReportKind)
	if kinds == nil {
		return errResponse(requestID, http.StatusBadRequest,
			"report_kind must be config-audit, rbac-assessment, infra-assessment, or all")
	}

	misconfigs := make([]map[string]interface{}, 0)
	counts := map[string]int{"critical": 0, "high": 0, "medium": 0, "low": 0}
	var lastErr error
	listedAny := false

	for reportKind, gvr := range kinds {
		list, err := e.listReports(ctx, gvr, params.Namespace, workloadSelector(params))
		if err != nil {
			// Tolerate individual report kinds being absent (e.g. infra
			// assessment disabled) as long as one kind lists successfully.
			lastErr = err
			continue
		}
		listedAny = true
		for i := range list.Items {
			report := &list.Items[i]
			resourceKind, resourceName := reportWorkload(report)
			if params.ResourceKind != "" && !strings.EqualFold(resourceKind, params.ResourceKind) {
				continue
			}
			checks, found, _ := unstructured.NestedSlice(report.Object, "report", "checks")
			if !found {
				continue
			}
			for _, raw := range checks {
				check, ok := raw.(map[string]interface{})
				if !ok {
					continue
				}
				if success, ok := check["success"].(bool); ok && success {
					continue
				}
				severity, _ := check["severity"].(string)
				if sev := strings.ToLower(severity); sev != "" {
					if _, tracked := counts[sev]; tracked {
						counts[sev]++
					}
				}
				if wantSeverities != nil && !wantSeverities[strings.ToLower(severity)] {
					continue
				}
				if len(misconfigs) >= maxResults {
					continue
				}
				entry := summarizeCheck(check)
				entry["report_kind"] = reportKind
				entry["resource_kind"] = resourceKind
				entry["resource_name"] = resourceName
				if ns := report.GetNamespace(); ns != "" {
					entry["namespace"] = ns
				}
				misconfigs = append(misconfigs, entry)
			}
		}
	}

	if !listedAny && lastErr != nil {
		return k8sErrResponse(requestID, lastErr)
	}

	sortBySeverity(misconfigs)
	return okResponse(requestID, map[string]interface{}{
		"misconfigurations": misconfigs,
		"misconfig_count":   len(misconfigs),
		"counts":            counts,
	})
}

// getComplianceReport returns one ClusterComplianceReport's pass/fail summary
// and its failing controls.
func (e *TrivyExecutor) getComplianceReport(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	name := params.ReportName
	if name == "" {
		name = params.Workload // filter fallback
	}
	if name == "" {
		return errResponse(requestID, http.StatusBadRequest, "report_name is required")
	}

	obj, err := e.resourceInterface(clusterComplianceReportGVR, "").Get(ctx, name, getOpts())
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	passCount, failCount := complianceSummary(obj)
	title, _, _ := unstructured.NestedString(obj.Object, "spec", "compliance", "title")

	return okResponse(requestID, map[string]interface{}{
		"report_name":     obj.GetName(),
		"title":           title,
		"pass_count":      passCount,
		"fail_count":      failCount,
		"failed_controls": complianceFailedControls(obj),
	})
}

// listComplianceReports returns the names of all ClusterComplianceReports
// (dropdown support).
func (e *TrivyExecutor) listComplianceReports(ctx context.Context, requestID string) *v1.DatadogQueryResponse {
	list, err := e.listReports(ctx, clusterComplianceReportGVR, "", "")
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	items := make([]map[string]interface{}, 0, len(list.Items))
	for i := range list.Items {
		report := &list.Items[i]
		title, _, _ := unstructured.NestedString(report.Object, "spec", "compliance", "title")
		passCount, failCount := complianceSummary(report)
		items = append(items, map[string]interface{}{
			"name":       report.GetName(),
			"title":      title,
			"pass_count": passCount,
			"fail_count": failCount,
		})
	}
	return okResponse(requestID, map[string]interface{}{"items": items})
}

// rescanWorkload deletes a workload's VulnerabilityReports; trivy-operator
// treats a missing report as "needs scanning" and regenerates it.
func (e *TrivyExecutor) rescanWorkload(ctx context.Context, requestID string, params trivyParams) *v1.DatadogQueryResponse {
	if params.Workload == "" {
		return errResponse(requestID, http.StatusBadRequest, "workload is required")
	}
	if params.Namespace == "" {
		return errResponse(requestID, http.StatusBadRequest, "namespace is required")
	}

	list, err := e.listReports(ctx, vulnerabilityReportGVR, params.Namespace, workloadSelector(params))
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	if len(list.Items) == 0 {
		return errResponse(requestID, http.StatusNotFound,
			"no VulnerabilityReport found for workload "+params.Workload+" in namespace "+params.Namespace)
	}

	deleted := make([]string, 0, len(list.Items))
	for i := range list.Items {
		report := &list.Items[i]
		if err := e.resourceInterface(vulnerabilityReportGVR, report.GetNamespace()).Delete(ctx, report.GetName(), deleteOpts()); err != nil {
			return k8sErrResponse(requestID, err)
		}
		deleted = append(deleted, report.GetName())
	}
	sort.Strings(deleted)
	e.Logger.Info("[Trivy] Deleted vulnerability reports to trigger rescan",
		zap.String("workload", params.Workload),
		zap.String("namespace", params.Namespace),
		zap.Int("count", len(deleted)))

	return okResponse(requestID, map[string]interface{}{
		"status":          "rescan_triggered",
		"workload":        params.Workload,
		"namespace":       params.Namespace,
		"deleted_reports": deleted,
		"deleted_count":   len(deleted),
	})
}
