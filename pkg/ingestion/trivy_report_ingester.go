package ingestion

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// trivy-operator report resources watched for workflow signals
// (aquasecurity.github.io/v1alpha1).
var (
	TrivyVulnerabilityReportResource = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "vulnerabilityreports",
	}
	TrivyConfigAuditReportResource = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "configauditreports",
	}
	TrivyExposedSecretReportResource = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "exposedsecretreports",
	}
	TrivyRbacAssessmentReportResource = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "rbacassessmentreports",
	}
	TrivyClusterComplianceReportResource = schema.GroupVersionResource{
		Group: "aquasecurity.github.io", Version: "v1alpha1", Resource: "clustercompliancereports",
	}
)

// trivyReportComponent is the synthetic event source component the server's
// workflow matcher keys on for Trivy finding signals.
const trivyReportComponent = "trivy-report"

// trivyReportKind classifies which CRD a report came from.
type trivyReportKind string

const (
	trivyKindVulnerability  trivyReportKind = "vulnerability"
	trivyKindConfigAudit    trivyReportKind = "config-audit"
	trivyKindExposedSecret  trivyReportKind = "exposed-secret"
	trivyKindRbacAssessment trivyReportKind = "rbac-assessment"
	trivyKindCompliance     trivyReportKind = "compliance"
)

// TrivyReportIngester watches trivy-operator reports and streams NEW findings
// to the server as synthetic Kubernetes Warning events (source component
// "trivy-report"), riding the existing incident-event pipeline into the
// workflow matcher.
//
// Dedup design: trivy-operator deletes and recreates reports on its report
// TTL (default 24h), so keying on report UID would re-fire the same findings
// daily. Instead each report is fingerprinted by its stable subject identity
// (namespace + workload + container + report kind) mapped to a hash of its
// finding IDs; events fire only when the fingerprint changes to a non-clean
// value. Deletes do NOT clear fingerprints, so TTL recreation of an unchanged
// report stays silent. Reports present at informer sync (or after an operator
// restart) are baselined silently.
type TrivyReportIngester struct {
	logger          *zap.Logger
	eventChan       chan *v1.KubernetesEvent
	dynamicClient   dynamic.Interface
	informerFactory dynamicinformer.DynamicSharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
	trivyEnabled    bool
	dropCounter     *DropCounter

	// fingerprints maps a report subject key to the hash of its findings.
	fingerprintMu sync.Mutex
	fingerprints  map[string]uint64

	// synced reports whether the initial informer cache sync has completed.
	syncedMu sync.RWMutex
	synced   bool
}

// NewTrivyReportIngester creates a Trivy report ingester. trivy-operator
// presence is detected via discovery of the aquasecurity.github.io group;
// when absent the ingester is a no-op.
func NewTrivyReportIngester(logger *zap.Logger, eventChan chan *v1.KubernetesEvent) (*TrivyReportIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	restConfig, err := k8s_helper.NewRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	trivyEnabled := checkTrivyReportsEnabled(clientset, logger)

	return &TrivyReportIngester{
		logger:          logger,
		eventChan:       eventChan,
		dynamicClient:   dynamicClient,
		informerFactory: dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 5*time.Minute),
		stopCh:          make(chan struct{}),
		trivyEnabled:    trivyEnabled,
		dropCounter:     NewDropCounter("trivy_report", logger, 30*time.Second),
		fingerprints:    map[string]uint64{},
	}, nil
}

// checkTrivyReportsEnabled checks whether the trivy-operator CRDs are served.
func checkTrivyReportsEnabled(clientset kubernetes.Interface, logger *zap.Logger) bool {
	_, err := clientset.Discovery().ServerResourcesForGroupVersion("aquasecurity.github.io/v1alpha1")
	if err != nil {
		logger.Info("trivy-operator CRDs not found, Trivy report ingestion disabled (this is normal if trivy-operator is not installed)")
		return false
	}
	logger.Info("trivy-operator CRDs found, enabling Trivy report ingestion")
	return true
}

// StartSync starts the Trivy report ingester and signals when setup is done.
func (tri *TrivyReportIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	if !tri.trivyEnabled {
		if syncDone != nil {
			syncDone <- nil
		}
		return nil
	}

	tri.logger.Info("Starting Trivy report ingester")
	tri.setupInformers()

	if syncDone != nil {
		syncDone <- nil
	}

	tri.informerFactory.Start(tri.stopCh)

	syncFns := make([]cache.InformerSynced, 0, len(trivyWatchedResources()))
	for _, gvr := range trivyWatchedResources() {
		syncFns = append(syncFns, tri.informerFactory.ForResource(gvr).Informer().HasSynced)
	}
	if !cache.WaitForCacheSync(tri.stopCh, syncFns...) {
		tri.logger.Warn("Trivy report informer cache failed to sync; continuing without initial baseline")
	}
	tri.markSynced()
	tri.logger.Info("Trivy report informer cache synced; streaming new findings")

	<-ctx.Done()
	tri.safeClose()
	tri.logger.Info("Stopped Trivy report ingester")
	return nil
}

// Stop stops the Trivy report ingester.
func (tri *TrivyReportIngester) Stop() {
	tri.safeClose()
}

func (tri *TrivyReportIngester) safeClose() {
	tri.mu.Lock()
	defer tri.mu.Unlock()
	if !tri.stopped {
		close(tri.stopCh)
		tri.stopped = true
	}
}

func (tri *TrivyReportIngester) markSynced() {
	tri.syncedMu.Lock()
	tri.synced = true
	tri.syncedMu.Unlock()
}

func (tri *TrivyReportIngester) isSynced() bool {
	tri.syncedMu.RLock()
	defer tri.syncedMu.RUnlock()
	return tri.synced
}

func trivyWatchedResources() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{
		TrivyVulnerabilityReportResource,
		TrivyConfigAuditReportResource,
		TrivyExposedSecretReportResource,
		TrivyRbacAssessmentReportResource,
		TrivyClusterComplianceReportResource,
	}
}

func trivyKindForResource(resource string) trivyReportKind {
	switch resource {
	case TrivyVulnerabilityReportResource.Resource:
		return trivyKindVulnerability
	case TrivyConfigAuditReportResource.Resource:
		return trivyKindConfigAudit
	case TrivyExposedSecretReportResource.Resource:
		return trivyKindExposedSecret
	case TrivyRbacAssessmentReportResource.Resource:
		return trivyKindRbacAssessment
	case TrivyClusterComplianceReportResource.Resource:
		return trivyKindCompliance
	}
	return ""
}

func (tri *TrivyReportIngester) setupInformers() {
	for _, gvr := range trivyWatchedResources() {
		reportKind := trivyKindForResource(gvr.Resource)
		handler := cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				tri.handleReport(obj, reportKind)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				// Skip resync no-ops (unchanged ResourceVersion).
				oldReport, okOld := oldObj.(*unstructured.Unstructured)
				newReport, okNew := newObj.(*unstructured.Unstructured)
				if okOld && okNew && oldReport.GetResourceVersion() == newReport.GetResourceVersion() {
					return
				}
				tri.handleReport(newObj, reportKind)
			},
			// Deletes intentionally keep fingerprints: trivy-operator's
			// report TTL deletes and recreates reports with identical
			// findings, which must not re-fire signals.
		}
		if _, err := tri.informerFactory.ForResource(gvr).Informer().AddEventHandler(handler); err != nil {
			tri.logger.Error("Failed to add Trivy report event handler",
				zap.String("resource", gvr.Resource), zap.Error(err))
		}
	}
}

// trivyFinding is the flattened summary of one report used to build the
// synthetic event.
type trivyFinding struct {
	ReportKind    trivyReportKind
	Namespace     string
	WorkloadKind  string
	WorkloadName  string
	Container     string
	Image         string
	CriticalCount int
	HighCount     int
	MediumCount   int
	LowCount      int
	// TopIDs are the highest-severity finding IDs (CVE IDs, check IDs, secret
	// rule IDs, or compliance control IDs), capped at 5.
	TopIDs []string
	// FindingIDs is the full stable ID set used for fingerprinting.
	FindingIDs []string
	// ReportName is the report object name (compliance reports).
	ReportName string
}

// hasFindings reports whether the report contains anything signal-worthy.
func (f *trivyFinding) hasFindings() bool {
	return len(f.FindingIDs) > 0
}

func (f *trivyFinding) severity() string {
	switch {
	case f.CriticalCount > 0:
		return "critical"
	case f.HighCount > 0:
		return "high"
	case f.MediumCount > 0:
		return "medium"
	case f.LowCount > 0:
		return "low"
	default:
		return "unknown"
	}
}

// subjectKey identifies a report by its stable subject, surviving the
// delete/recreate cycle of trivy-operator's report TTL.
func (f *trivyFinding) subjectKey() string {
	return strings.Join([]string{
		string(f.ReportKind), f.Namespace, f.WorkloadKind, f.WorkloadName, f.Container, f.ReportName,
	}, "|")
}

// fingerprint hashes the sorted finding IDs (and image, so a new digest of
// the same workload re-fires) into a change-detection value.
func (f *trivyFinding) fingerprint() uint64 {
	ids := append([]string{}, f.FindingIDs...)
	sort.Strings(ids)
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%s", f.Image, strings.Join(ids, ","))
	return h.Sum64()
}

// handleReport fingerprints a report and emits an event when its findings
// changed to a non-clean state.
func (tri *TrivyReportIngester) handleReport(obj interface{}, reportKind trivyReportKind) {
	report, ok := extractReport(obj)
	if !ok {
		return
	}

	finding := extractTrivyFinding(report, reportKind)
	fp := finding.fingerprint()
	key := finding.subjectKey()

	tri.fingerprintMu.Lock()
	previous, seen := tri.fingerprints[key]
	tri.fingerprints[key] = fp
	tri.fingerprintMu.Unlock()

	if !tri.isSynced() {
		return
	}
	if seen && previous == fp {
		return
	}
	if !finding.hasFindings() {
		return
	}
	tri.sendFindingEvent(report, finding)
}

// extractTrivyFinding flattens one report into the finding summary.
func extractTrivyFinding(report *unstructured.Unstructured, reportKind trivyReportKind) *trivyFinding {
	labels := report.GetLabels()
	finding := &trivyFinding{
		ReportKind:   reportKind,
		Namespace:    report.GetNamespace(),
		WorkloadKind: labels["trivy-operator.resource.kind"],
		WorkloadName: labels["trivy-operator.resource.name"],
		Container:    labels["trivy-operator.container.name"],
	}
	// Names that are not valid label values (e.g. Role
	// "cert-manager-webhook:dynamic-serving") are stored by trivy-operator in
	// an annotation instead of the label.
	if finding.WorkloadName == "" {
		finding.WorkloadName = report.GetAnnotations()["trivy-operator.resource.name"]
	}
	if ns := labels["trivy-operator.resource.namespace"]; ns != "" && finding.Namespace == "" {
		finding.Namespace = ns
	}

	switch reportKind {
	case trivyKindVulnerability:
		finding.Image = trivyImageRef(report)
		extractTrivySummaryCounts(report, finding)
		finding.FindingIDs, finding.TopIDs = trivyFindingIDs(report, "vulnerabilities", "vulnerabilityID")
	case trivyKindExposedSecret:
		finding.Image = trivyImageRef(report)
		extractTrivySummaryCounts(report, finding)
		finding.FindingIDs, finding.TopIDs = trivyFindingIDs(report, "secrets", "ruleID")
	case trivyKindConfigAudit, trivyKindRbacAssessment:
		extractTrivySummaryCounts(report, finding)
		finding.FindingIDs, finding.TopIDs = trivyFailedCheckIDs(report)
	case trivyKindCompliance:
		finding.ReportName = report.GetName()
		finding.FindingIDs, finding.TopIDs = trivyFailedControlIDs(report)
		// Compliance summaries expose pass/fail, not severities; treat every
		// failing control as high for severity classification purposes.
		finding.HighCount = len(finding.FindingIDs)
	}
	return finding
}

// extractTrivySummaryCounts reads report.summary severity counts.
func extractTrivySummaryCounts(report *unstructured.Unstructured, finding *trivyFinding) {
	summary, found, _ := unstructured.NestedMap(report.Object, "report", "summary")
	if !found {
		return
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
	finding.CriticalCount = readInt("criticalCount")
	finding.HighCount = readInt("highCount")
	finding.MediumCount = readInt("mediumCount")
	finding.LowCount = readInt("lowCount")
}

// trivyImageRef reconstructs the scanned image reference from report.artifact.
func trivyImageRef(report *unstructured.Unstructured) string {
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

// trivySeverityRank orders severities critical-first for top-ID selection.
func trivySeverityRank(severity string) int {
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

// trivyFindingIDs extracts finding IDs from a report list field (e.g.
// vulnerabilities/vulnerabilityID, secrets/ruleID). Returns the full ID set
// (fingerprinting) and the top 5 highest-severity IDs (event message).
func trivyFindingIDs(report *unstructured.Unstructured, listField, idField string) (all []string, top []string) {
	items, found, _ := unstructured.NestedSlice(report.Object, "report", listField)
	if !found {
		return nil, nil
	}
	type rankedID struct {
		id   string
		rank int
	}
	ranked := make([]rankedID, 0, len(items))
	for _, raw := range items {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		id, _ := item[idField].(string)
		if id == "" {
			continue
		}
		severity, _ := item["severity"].(string)
		all = append(all, id)
		ranked = append(ranked, rankedID{id: id, rank: trivySeverityRank(severity)})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].rank < ranked[j].rank })
	seen := map[string]bool{}
	for _, r := range ranked {
		if seen[r.id] {
			continue
		}
		seen[r.id] = true
		top = append(top, r.id)
		if len(top) == 5 {
			break
		}
	}
	return all, top
}

// trivyFailedCheckIDs extracts failed check IDs from config-audit/RBAC
// reports (report.checks with success=false).
func trivyFailedCheckIDs(report *unstructured.Unstructured) (all []string, top []string) {
	checks, found, _ := unstructured.NestedSlice(report.Object, "report", "checks")
	if !found {
		return nil, nil
	}
	type rankedID struct {
		id   string
		rank int
	}
	ranked := make([]rankedID, 0)
	for _, raw := range checks {
		check, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if success, ok := check["success"].(bool); ok && success {
			continue
		}
		id, _ := check["checkID"].(string)
		if id == "" {
			continue
		}
		severity, _ := check["severity"].(string)
		all = append(all, id)
		ranked = append(ranked, rankedID{id: id, rank: trivySeverityRank(severity)})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].rank < ranked[j].rank })
	seen := map[string]bool{}
	for _, r := range ranked {
		if seen[r.id] {
			continue
		}
		seen[r.id] = true
		top = append(top, r.id)
		if len(top) == 5 {
			break
		}
	}
	return all, top
}

// trivyFailedControlIDs extracts failing control IDs from a
// ClusterComplianceReport's status.summaryReport.controlCheck.
func trivyFailedControlIDs(report *unstructured.Unstructured) (all []string, top []string) {
	controls, found, _ := unstructured.NestedSlice(report.Object, "status", "summaryReport", "controlCheck")
	if !found {
		return nil, nil
	}
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
		id, _ := control["id"].(string)
		if id == "" {
			continue
		}
		all = append(all, id)
		if len(top) < 5 {
			top = append(top, id)
		}
	}
	return all, top
}

// sendFindingEvent converts one report finding into a synthetic Kubernetes
// Warning event on the incident-event pipeline.
func (tri *TrivyReportIngester) sendFindingEvent(report *unstructured.Unstructured, finding *trivyFinding) {
	var reason string
	messageParts := []string{}
	switch finding.ReportKind {
	case trivyKindVulnerability:
		reason = "VulnerabilityDetected"
		if finding.Image != "" {
			messageParts = append(messageParts, fmt.Sprintf("image=%s", finding.Image))
		}
		messageParts = append(messageParts,
			fmt.Sprintf("workload=%s/%s", finding.WorkloadKind, finding.WorkloadName),
			fmt.Sprintf("critical=%d", finding.CriticalCount),
			fmt.Sprintf("high=%d", finding.HighCount))
		if len(finding.TopIDs) > 0 {
			messageParts = append(messageParts, fmt.Sprintf("top_cves=%s", strings.Join(finding.TopIDs, ",")))
		}
	case trivyKindExposedSecret:
		reason = "ExposedSecretDetected"
		if finding.Image != "" {
			messageParts = append(messageParts, fmt.Sprintf("image=%s", finding.Image))
		}
		messageParts = append(messageParts,
			fmt.Sprintf("workload=%s/%s", finding.WorkloadKind, finding.WorkloadName),
			fmt.Sprintf("secrets=%d", len(finding.FindingIDs)))
		if len(finding.TopIDs) > 0 {
			messageParts = append(messageParts, fmt.Sprintf("rules=%s", strings.Join(finding.TopIDs, ",")))
		}
	case trivyKindConfigAudit, trivyKindRbacAssessment:
		reason = "ConfigAuditFailed"
		messageParts = append(messageParts,
			fmt.Sprintf("report_kind=%s", finding.ReportKind),
			fmt.Sprintf("resource=%s/%s", finding.WorkloadKind, finding.WorkloadName),
			fmt.Sprintf("critical=%d", finding.CriticalCount),
			fmt.Sprintf("high=%d", finding.HighCount),
			fmt.Sprintf("failed_checks=%d", len(finding.FindingIDs)))
		if len(finding.TopIDs) > 0 {
			messageParts = append(messageParts, fmt.Sprintf("checks=%s", strings.Join(finding.TopIDs, ",")))
		}
	case trivyKindCompliance:
		reason = "ComplianceCheckFailed"
		messageParts = append(messageParts,
			fmt.Sprintf("report=%s", finding.ReportName),
			fmt.Sprintf("failed_controls=%d", len(finding.FindingIDs)))
		if len(finding.TopIDs) > 0 {
			messageParts = append(messageParts, fmt.Sprintf("controls=%s", strings.Join(finding.TopIDs, ",")))
		}
	default:
		return
	}
	messageParts = append(messageParts, fmt.Sprintf("severity=%s", finding.severity()))
	message := strings.Join(messageParts, " ")

	involvedKind := finding.WorkloadKind
	involvedName := finding.WorkloadName
	if finding.ReportKind == trivyKindCompliance {
		involvedKind = "ClusterComplianceReport"
		involvedName = finding.ReportName
	}

	now := timestamppb.New(time.Now())
	protoEvent := &v1.KubernetesEvent{
		Name:      fmt.Sprintf("trivy-finding-%x", finding.fingerprint()),
		Namespace: finding.Namespace,
		Uid:       string(report.GetUID()),
		EventType: "Warning",
		Reason:    reason,
		Message:   message,
		InvolvedObject: &v1.ObjectReference{
			Kind:      involvedKind,
			Namespace: finding.Namespace,
			Name:      involvedName,
		},
		Source:         &v1.EventSource{Component: trivyReportComponent},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Action:         stringToAction("CREATE"),
	}

	select {
	case tri.eventChan <- protoEvent:
		tri.logger.Debug("Sent Trivy finding event",
			zap.String("report_kind", string(finding.ReportKind)),
			zap.String("reason", reason),
			zap.String("workload", fmt.Sprintf("%s/%s/%s", finding.WorkloadKind, finding.Namespace, finding.WorkloadName)))
	default:
		tri.dropCounter.RecordDrop()
	}
}
