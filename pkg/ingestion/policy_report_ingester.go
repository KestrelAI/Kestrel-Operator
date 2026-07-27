package ingestion

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Kyverno policy report resources (wgpolicyk8s.io).
var (
	PolicyReportResource = schema.GroupVersionResource{
		Group: "wgpolicyk8s.io", Version: "v1alpha2", Resource: "policyreports",
	}
	ClusterPolicyReportResource = schema.GroupVersionResource{
		Group: "wgpolicyk8s.io", Version: "v1alpha2", Resource: "clusterpolicyreports",
	}
)

// kyvernoReportComponent is the synthetic event source component the server's
// workflow matcher keys on for Kyverno violation signals.
const kyvernoReportComponent = "kyverno-policyreport"

// PolicyReportIngester watches Kyverno PolicyReports/ClusterPolicyReports and
// streams NEW fail/warn/error results to the server as synthetic Kubernetes
// Warning events (source component "kyverno-policyreport"), riding the
// existing incident-event pipeline into the workflow matcher.
//
// Violations present when the informer first syncs (or after an operator
// restart) are recorded silently so historical results don't re-fire
// workflow triggers.
type PolicyReportIngester struct {
	logger          *zap.Logger
	eventChan       chan *v1.KubernetesEvent
	dynamicClient   dynamic.Interface
	informerFactory dynamicinformer.DynamicSharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
	kyvernoEnabled  bool
	dropCounter     *DropCounter

	// seen tracks result keys per report UID so only new results emit events.
	seenMu sync.Mutex
	seen   map[types.UID]map[string]struct{}

	// synced reports whether the initial informer cache sync has completed.
	syncedMu sync.RWMutex
	synced   bool
}

// NewPolicyReportIngester creates a PolicyReport ingester. Kyverno presence is
// detected via discovery of the wgpolicyk8s.io group; when absent the ingester
// is a no-op.
func NewPolicyReportIngester(logger *zap.Logger, eventChan chan *v1.KubernetesEvent) (*PolicyReportIngester, error) {
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

	kyvernoEnabled := checkPolicyReportsEnabled(clientset, logger)

	return &PolicyReportIngester{
		logger:          logger,
		eventChan:       eventChan,
		dynamicClient:   dynamicClient,
		informerFactory: dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 5*time.Minute),
		stopCh:          make(chan struct{}),
		kyvernoEnabled:  kyvernoEnabled,
		dropCounter:     NewDropCounter("policy_report", logger, 30*time.Second),
		seen:            map[types.UID]map[string]struct{}{},
	}, nil
}

// checkPolicyReportsEnabled checks whether the PolicyReport CRDs are served.
func checkPolicyReportsEnabled(clientset kubernetes.Interface, logger *zap.Logger) bool {
	_, err := clientset.Discovery().ServerResourcesForGroupVersion("wgpolicyk8s.io/v1alpha2")
	if err != nil {
		logger.Info("Kyverno PolicyReport CRDs not found, policy report ingestion disabled (this is normal if Kyverno is not installed)")
		return false
	}
	logger.Info("Kyverno PolicyReport CRDs found, enabling policy report ingestion")
	return true
}

// StartSync starts the policy report ingester and signals when setup is done.
func (pri *PolicyReportIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	if !pri.kyvernoEnabled {
		if syncDone != nil {
			syncDone <- nil
		}
		return nil
	}

	pri.logger.Info("Starting Kyverno policy report ingester")
	pri.setupInformers()

	if syncDone != nil {
		syncDone <- nil
	}

	pri.informerFactory.Start(pri.stopCh)

	if !cache.WaitForCacheSync(pri.stopCh,
		pri.informerFactory.ForResource(PolicyReportResource).Informer().HasSynced,
		pri.informerFactory.ForResource(ClusterPolicyReportResource).Informer().HasSynced,
	) {
		pri.logger.Warn("Policy report informer cache failed to sync; continuing without initial baseline")
	}
	pri.markSynced()
	pri.logger.Info("Policy report informer cache synced; streaming new violations")

	<-ctx.Done()
	pri.safeClose()
	pri.logger.Info("Stopped policy report ingester")
	return nil
}

// Stop stops the policy report ingester.
func (pri *PolicyReportIngester) Stop() {
	pri.safeClose()
}

func (pri *PolicyReportIngester) safeClose() {
	pri.mu.Lock()
	defer pri.mu.Unlock()
	if !pri.stopped {
		close(pri.stopCh)
		pri.stopped = true
	}
}

func (pri *PolicyReportIngester) markSynced() {
	pri.syncedMu.Lock()
	pri.synced = true
	pri.syncedMu.Unlock()
}

func (pri *PolicyReportIngester) isSynced() bool {
	pri.syncedMu.RLock()
	defer pri.syncedMu.RUnlock()
	return pri.synced
}

func (pri *PolicyReportIngester) setupInformers() {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pri.handleReport(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// Skip resync no-ops (unchanged ResourceVersion).
			oldReport, okOld := oldObj.(*unstructured.Unstructured)
			newReport, okNew := newObj.(*unstructured.Unstructured)
			if okOld && okNew && oldReport.GetResourceVersion() == newReport.GetResourceVersion() {
				return
			}
			pri.handleReport(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			report, ok := extractReport(obj)
			if !ok {
				return
			}
			pri.seenMu.Lock()
			delete(pri.seen, report.GetUID())
			pri.seenMu.Unlock()
		},
	}

	for _, gvr := range []schema.GroupVersionResource{PolicyReportResource, ClusterPolicyReportResource} {
		if _, err := pri.informerFactory.ForResource(gvr).Informer().AddEventHandler(handler); err != nil {
			pri.logger.Error("Failed to add policy report event handler",
				zap.String("resource", gvr.Resource), zap.Error(err))
		}
	}
}

// handleReport diffs a report's results against the previously seen set and
// emits events for new fail/warn/error results.
func (pri *PolicyReportIngester) handleReport(obj interface{}) {
	report, ok := extractReport(obj)
	if !ok {
		return
	}

	entries := extractReportViolations(report)
	current := make(map[string]struct{}, len(entries))
	for key := range entries {
		current[key] = struct{}{}
	}

	pri.seenMu.Lock()
	previous := pri.seen[report.GetUID()]
	pri.seen[report.GetUID()] = current
	pri.seenMu.Unlock()

	if !pri.isSynced() {
		return
	}
	for key, entry := range entries {
		if previous != nil {
			if _, alreadySeen := previous[key]; alreadySeen {
				continue
			}
		}
		pri.sendViolationEvent(report, entry)
	}
}

func extractReport(obj interface{}) (*unstructured.Unstructured, bool) {
	if report, ok := obj.(*unstructured.Unstructured); ok {
		return report, true
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		report, ok := tombstone.Obj.(*unstructured.Unstructured)
		return report, ok
	}
	return nil, false
}

// reportViolation is one flattened fail/warn/error result from a report.
type reportViolation struct {
	Policy            string
	Rule              string
	Result            string
	Severity          string
	Message           string
	ResourceKind      string
	ResourceName      string
	ResourceNamespace string
}

// extractReportViolations flattens a report's fail/warn/error results, keyed
// by a stable dedup key.
func extractReportViolations(report *unstructured.Unstructured) map[string]reportViolation {
	out := map[string]reportViolation{}
	results, found, _ := unstructured.NestedSlice(report.Object, "results")
	if !found {
		return out
	}
	for _, r := range results {
		res, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		result, _ := res["result"].(string)
		switch strings.ToLower(result) {
		case "fail", "warn", "error":
		default:
			continue
		}
		violation := reportViolation{Result: strings.ToLower(result)}
		violation.Policy, _ = res["policy"].(string)
		violation.Rule, _ = res["rule"].(string)
		violation.Severity, _ = res["severity"].(string)
		violation.Message, _ = res["message"].(string)

		if resources, ok := res["resources"].([]interface{}); ok && len(resources) > 0 {
			if resource, ok := resources[0].(map[string]interface{}); ok {
				violation.ResourceKind, _ = resource["kind"].(string)
				violation.ResourceName, _ = resource["name"].(string)
				violation.ResourceNamespace, _ = resource["namespace"].(string)
			}
		}
		if violation.ResourceName == "" {
			if scope, found, _ := unstructured.NestedMap(report.Object, "scope"); found {
				violation.ResourceKind, _ = scope["kind"].(string)
				violation.ResourceName, _ = scope["name"].(string)
				violation.ResourceNamespace, _ = scope["namespace"].(string)
			}
		}
		if violation.ResourceNamespace == "" {
			violation.ResourceNamespace = report.GetNamespace()
		}

		out[violationKey(violation)] = violation
	}
	return out
}

func violationKey(violation reportViolation) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%s|%s|%s|%s/%s/%s",
		violation.Policy, violation.Rule, violation.Result, violation.Message,
		violation.ResourceKind, violation.ResourceNamespace, violation.ResourceName)
	return fmt.Sprintf("%x", h.Sum64())
}

// sendViolationEvent converts one violation into a synthetic Kubernetes
// Warning event on the incident-event pipeline.
func (pri *PolicyReportIngester) sendViolationEvent(report *unstructured.Unstructured, violation reportViolation) {
	reason := "PolicyViolation"
	switch violation.Result {
	case "error":
		reason = "PolicyError"
	case "warn":
		reason = "PolicyWarning"
	}

	messageParts := []string{fmt.Sprintf("policy=%s", violation.Policy)}
	if violation.Rule != "" {
		messageParts = append(messageParts, fmt.Sprintf("rule=%s", violation.Rule))
	}
	if violation.Severity != "" {
		messageParts = append(messageParts, fmt.Sprintf("severity=%s", violation.Severity))
	}
	message := strings.Join(messageParts, " ")
	if violation.Message != "" {
		message += ": " + violation.Message
	}

	now := timestamppb.New(time.Now())
	protoEvent := &v1.KubernetesEvent{
		Name:      fmt.Sprintf("kyverno-violation-%s", violationKey(violation)),
		Namespace: violation.ResourceNamespace,
		Uid:       string(report.GetUID()),
		EventType: "Warning",
		Reason:    reason,
		Message:   message,
		InvolvedObject: &v1.ObjectReference{
			Kind:      violation.ResourceKind,
			Namespace: violation.ResourceNamespace,
			Name:      violation.ResourceName,
		},
		Source:         &v1.EventSource{Component: kyvernoReportComponent},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Action:         stringToAction("CREATE"),
	}

	select {
	case pri.eventChan <- protoEvent:
		pri.logger.Debug("Sent Kyverno policy violation event",
			zap.String("policy", violation.Policy),
			zap.String("rule", violation.Rule),
			zap.String("result", violation.Result),
			zap.String("resource", fmt.Sprintf("%s/%s/%s", violation.ResourceKind, violation.ResourceNamespace, violation.ResourceName)))
	default:
		pri.dropCounter.RecordDrop()
	}
}
