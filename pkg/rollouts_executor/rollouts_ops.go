package rollouts_executor

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"encoding/json"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

const revisionAnnotation = "rollout.argoproj.io/revision"

// promote unblocks a paused rollout step. With Full=true it performs a full
// promotion, skipping all remaining canary steps (status.promoteFull).
func (e *RolloutsExecutor) promote(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()

	// Clear spec.paused if set (blueGreen autoPromotionEnabled=false / paused rollouts)
	if paused, found, _ := unstructured.NestedBool(ro.Object, "spec", "paused"); found && paused {
		if _, err := e.patchRollout(ctx, ns, name, types.MergePatchType, []byte(`{"spec":{"paused":false}}`)); err != nil {
			return k8sErrResponse(requestID, err)
		}
	}

	// Clear pause conditions (and optionally request full promotion) on status.
	statusPatch := `{"status":{"pauseConditions":null,"controllerPause":false}}`
	if params.Full {
		statusPatch = `{"status":{"pauseConditions":null,"controllerPause":false,"promoteFull":true}}`
	}
	if err := e.patchStatus(ctx, ns, name, []byte(statusPatch)); err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Rollouts] Promoted rollout",
		zap.String("namespace", ns), zap.String("name", name), zap.Bool("full", params.Full))
	return okResponse(requestID, map[string]interface{}{
		"status":    "promoted",
		"name":      name,
		"namespace": ns,
		"full":      params.Full,
	})
}

// abort aborts an in-progress update, scaling back to the stable version.
func (e *RolloutsExecutor) abort(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()
	if err := e.patchStatus(ctx, ns, name, []byte(`{"status":{"abort":true,"pauseConditions":null,"controllerPause":false}}`)); err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Rollouts] Aborted rollout", zap.String("namespace", ns), zap.String("name", name))
	return okResponse(requestID, map[string]interface{}{
		"status": "aborted", "name": name, "namespace": ns,
	})
}

// retry retries an aborted rollout from the beginning of its steps.
func (e *RolloutsExecutor) retry(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()
	if err := e.patchStatus(ctx, ns, name, []byte(`{"status":{"abort":false}}`)); err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Rollouts] Retried rollout", zap.String("namespace", ns), zap.String("name", name))
	return okResponse(requestID, map[string]interface{}{
		"status": "retried", "name": name, "namespace": ns,
	})
}

// setPaused pauses (true) or resumes (false) a rollout via spec.paused.
func (e *RolloutsExecutor) setPaused(ctx context.Context, requestID string, params rolloutParams, paused bool) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()
	patch := fmt.Sprintf(`{"spec":{"paused":%v}}`, paused)
	if _, err := e.patchRollout(ctx, ns, name, types.MergePatchType, []byte(patch)); err != nil {
		return k8sErrResponse(requestID, err)
	}
	action := "paused"
	if !paused {
		action = "resumed"
		// Also clear any manual pause conditions so the rollout continues.
		_ = e.patchStatus(ctx, ns, name, []byte(`{"status":{"pauseConditions":null,"controllerPause":false}}`))
	}
	e.Logger.Info("[Rollouts] Set rollout paused state",
		zap.String("namespace", ns), zap.String("name", name), zap.Bool("paused", paused))
	return okResponse(requestID, map[string]interface{}{
		"status": action, "name": name, "namespace": ns,
	})
}

// restart triggers a pod restart by setting spec.restartAt to now.
func (e *RolloutsExecutor) restart(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()
	restartAt := time.Now().UTC().Format(time.RFC3339)
	patch := fmt.Sprintf(`{"spec":{"restartAt":"%s"}}`, restartAt)
	if _, err := e.patchRollout(ctx, ns, name, types.MergePatchType, []byte(patch)); err != nil {
		return k8sErrResponse(requestID, err)
	}
	e.Logger.Info("[Rollouts] Restarted rollout",
		zap.String("namespace", ns), zap.String("name", name), zap.String("restart_at", restartAt))
	return okResponse(requestID, map[string]interface{}{
		"status": "restarted", "name": name, "namespace": ns, "restart_at": restartAt,
	})
}

// undo rolls the rollout back to a previous revision by copying the pod
// template of the ReplicaSet with the target revision (mirrors
// `kubectl argo rollouts undo`). Revision 0 means "previous revision".
func (e *RolloutsExecutor) undo(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	ns, name := ro.GetNamespace(), ro.GetName()

	revisions, err := e.listRevisions(ctx, ro)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	if len(revisions) == 0 {
		return errResponse(requestID, http.StatusNotFound,
			fmt.Sprintf("no revision history found for rollout %s/%s", ns, name))
	}

	target := params.Revision
	if target == 0 {
		// Previous revision = second-highest.
		if len(revisions) < 2 {
			return errResponse(requestID, http.StatusNotFound,
				fmt.Sprintf("rollout %s/%s has no previous revision to roll back to", ns, name))
		}
		target = revisions[1].revision
	}

	var targetRS *unstructured.Unstructured
	for _, rev := range revisions {
		if rev.revision == target {
			targetRS = rev.rs
			break
		}
	}
	if targetRS == nil {
		return errResponse(requestID, http.StatusNotFound,
			fmt.Sprintf("revision %d not found for rollout %s/%s", target, ns, name))
	}

	template, found, err := unstructured.NestedMap(targetRS.Object, "spec", "template")
	if err != nil || !found {
		return errResponse(requestID, http.StatusInternalServerError,
			fmt.Sprintf("failed to read pod template from revision %d", target))
	}
	// Strip the rollouts pod-template-hash label — it is managed by the controller.
	if metadata, ok := template["metadata"].(map[string]interface{}); ok {
		if labels, ok := metadata["labels"].(map[string]interface{}); ok {
			delete(labels, "rollouts-pod-template-hash")
		}
	}

	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{"template": template},
	})
	if err != nil {
		return errResponse(requestID, http.StatusInternalServerError, err.Error())
	}
	if _, err := e.patchRollout(ctx, ns, name, types.MergePatchType, patch); err != nil {
		return k8sErrResponse(requestID, err)
	}

	e.Logger.Info("[Rollouts] Rolled back rollout",
		zap.String("namespace", ns), zap.String("name", name), zap.Int64("revision", target))
	return okResponse(requestID, map[string]interface{}{
		"status": "rolled_back", "name": name, "namespace": ns, "revision": target,
	})
}

// getStatus returns a compact status summary for one rollout, plus the
// revision history (used by the rollouts_revisions dropdown).
func (e *RolloutsExecutor) getStatus(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ro, err := e.getRollout(ctx, params)
	if err != nil {
		return k8sErrResponse(requestID, err)
	}

	summary := summarizeRollout(ro)

	// Include revision history for dropdowns / undo target selection.
	if revisions, err := e.listRevisions(ctx, ro); err == nil {
		history := make([]map[string]interface{}, 0, len(revisions))
		for _, rev := range revisions {
			entry := map[string]interface{}{"revision": rev.revision, "replicaset": rev.rs.GetName()}
			if images := containerImages(rev.rs); len(images) > 0 {
				entry["images"] = images
			}
			history = append(history, entry)
		}
		summary["history"] = history
	}

	return okResponse(requestID, summary)
}

// list returns summaries of all rollouts, optionally namespace-scoped.
func (e *RolloutsExecutor) list(ctx context.Context, requestID string, params rolloutParams) *v1.DatadogQueryResponse {
	ns := params.Namespace
	if ns == "" {
		ns = metav1.NamespaceAll
	}
	roList, err := e.Dynamic.Resource(RolloutGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return k8sErrResponse(requestID, err)
	}
	items := make([]map[string]interface{}, 0, len(roList.Items))
	for i := range roList.Items {
		items = append(items, summarizeRollout(&roList.Items[i]))
	}
	return okResponse(requestID, map[string]interface{}{"items": items})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// patchStatus patches the rollout's status subresource, falling back to a
// regular patch for clusters where the CRD has no status subresource.
func (e *RolloutsExecutor) patchStatus(ctx context.Context, namespace, name string, patch []byte) error {
	_, err := e.Dynamic.Resource(RolloutGVR).Namespace(namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err == nil {
		return nil
	}
	_, fallbackErr := e.Dynamic.Resource(RolloutGVR).Namespace(namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if fallbackErr != nil {
		return err
	}
	return nil
}

type rolloutRevision struct {
	revision int64
	rs       *unstructured.Unstructured
}

// listRevisions returns ReplicaSets owned by the rollout sorted by revision
// (newest first).
func (e *RolloutsExecutor) listRevisions(ctx context.Context, ro *unstructured.Unstructured) ([]rolloutRevision, error) {
	rsList, err := e.Dynamic.Resource(ReplicaSetGVR).Namespace(ro.GetNamespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list replicasets: %w", err)
	}
	var revisions []rolloutRevision
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !isOwnedBy(rs, ro.GetName(), ro.GetUID()) {
			continue
		}
		revStr := rs.GetAnnotations()[revisionAnnotation]
		if revStr == "" {
			continue
		}
		rev, err := strconv.ParseInt(revStr, 10, 64)
		if err != nil {
			continue
		}
		revisions = append(revisions, rolloutRevision{revision: rev, rs: rs})
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i].revision > revisions[j].revision })
	return revisions, nil
}

func isOwnedBy(obj *unstructured.Unstructured, ownerName string, ownerUID types.UID) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "Rollout" && ref.Name == ownerName && (ownerUID == "" || ref.UID == ownerUID) {
			return true
		}
	}
	return false
}

func summarizeRollout(ro *unstructured.Unstructured) map[string]interface{} {
	summary := map[string]interface{}{
		"name":      ro.GetName(),
		"namespace": ro.GetNamespace(),
	}

	if phase, found, _ := unstructured.NestedString(ro.Object, "status", "phase"); found {
		summary["phase"] = phase
	}
	if message, found, _ := unstructured.NestedString(ro.Object, "status", "message"); found {
		summary["message"] = message
	}
	if paused, found, _ := unstructured.NestedBool(ro.Object, "spec", "paused"); found {
		summary["paused"] = paused
	}
	if abort, found, _ := unstructured.NestedBool(ro.Object, "status", "abort"); found && abort {
		summary["aborted"] = true
	}

	// Strategy detection + step progress for canary rollouts.
	if _, found, _ := unstructured.NestedMap(ro.Object, "spec", "strategy", "canary"); found {
		summary["strategy"] = "canary"
		if steps, found, _ := unstructured.NestedSlice(ro.Object, "spec", "strategy", "canary", "steps"); found {
			summary["total_steps"] = len(steps)
		}
		if idx, found, _ := unstructured.NestedInt64(ro.Object, "status", "currentStepIndex"); found {
			summary["current_step"] = idx
		}
		if weight, found, _ := unstructured.NestedInt64(ro.Object, "status", "canary", "weights", "canary", "weight"); found {
			summary["canary_weight"] = weight
		}
	} else if _, found, _ := unstructured.NestedMap(ro.Object, "spec", "strategy", "blueGreen"); found {
		summary["strategy"] = "blueGreen"
	}

	if replicas, found, _ := unstructured.NestedInt64(ro.Object, "spec", "replicas"); found {
		summary["replicas"] = replicas
	}
	if ready, found, _ := unstructured.NestedInt64(ro.Object, "status", "readyReplicas"); found {
		summary["ready_replicas"] = ready
	}
	if rev, ok := ro.GetAnnotations()[revisionAnnotation]; ok {
		summary["revision"] = rev
	}
	if images := containerImages(ro); len(images) > 0 {
		summary["images"] = images
	}
	return summary
}

func containerImages(obj *unstructured.Unstructured) []string {
	containers, found, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
	if !found {
		return nil
	}
	var images []string
	for _, c := range containers {
		if m, ok := c.(map[string]interface{}); ok {
			if img, ok := m["image"].(string); ok && img != "" {
				images = append(images, img)
			}
		}
	}
	return images
}
