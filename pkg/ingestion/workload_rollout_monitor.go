package ingestion

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// WorkloadRolloutMonitor monitors deployment, statefulset, and daemonset rollout status for incident detection
type WorkloadRolloutMonitor struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	rolloutChan     chan *v1.WorkloadRolloutStatus
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex

	// Track previous rollout states to detect failures
	previousRolloutStates map[string]*rolloutStateSnapshot
	rolloutMu             sync.RWMutex
}

// rolloutStateSnapshot captures the rollout state for comparison
type rolloutStateSnapshot struct {
	generation         int64
	observedGeneration int64
	replicas           int32
	updatedReplicas    int32
	readyReplicas      int32
	availableReplicas  int32
	conditions         map[string]string // condition type -> status
}

// NewWorkloadRolloutMonitor creates a new workload rollout monitor for incident detection
func NewWorkloadRolloutMonitor(logger *zap.Logger, rolloutChan chan *v1.WorkloadRolloutStatus) (*WorkloadRolloutMonitor, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory with shorter resync for rollout monitoring
	informerFactory := informers.NewSharedInformerFactory(clientset, 15*time.Second)

	return &WorkloadRolloutMonitor{
		clientset:             clientset,
		logger:                logger,
		rolloutChan:           rolloutChan,
		informerFactory:       informerFactory,
		stopCh:                make(chan struct{}),
		previousRolloutStates: make(map[string]*rolloutStateSnapshot),
	}, nil
}

// StartSync starts the workload rollout monitor and signals when initial sync is complete
func (wrm *WorkloadRolloutMonitor) StartSync(ctx context.Context, syncDone chan<- error) error {
	wrm.logger.Info("Starting workload rollout monitor for incident detection")

	// Set up informers for Deployments, StatefulSets, and DaemonSets
	wrm.setupDeploymentInformer()
	wrm.setupStatefulSetInformer()
	wrm.setupDaemonSetInformer()

	// Send initial inventory (workloads with rollout issues)
	if err := wrm.sendInitialRolloutInventory(ctx); err != nil {
		wrm.logger.Error("Failed to send initial rollout inventory", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers
	wrm.informerFactory.Start(wrm.stopCh)

	// Wait for all caches to sync before processing events
	wrm.logger.Info("Waiting for workload rollout informer caches to sync...")
	if !cache.WaitForCacheSync(wrm.stopCh,
		wrm.informerFactory.Apps().V1().Deployments().Informer().HasSynced,
		wrm.informerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
		wrm.informerFactory.Apps().V1().DaemonSets().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for workload rollout informer caches to sync")
	}
	wrm.logger.Info("Workload rollout informer caches synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	wrm.safeClose()
	wrm.logger.Info("Stopped workload rollout monitor")
	return nil
}

// Stop stops the workload rollout monitor
func (wrm *WorkloadRolloutMonitor) Stop() {
	wrm.safeClose()
}

// safeClose safely closes the stop channel only once
func (wrm *WorkloadRolloutMonitor) safeClose() {
	wrm.mu.Lock()
	defer wrm.mu.Unlock()
	if !wrm.stopped {
		close(wrm.stopCh)
		wrm.stopped = true
	}
}

// setupDeploymentInformer sets up the deployment informer for rollout monitoring
func (wrm *WorkloadRolloutMonitor) setupDeploymentInformer() {
	deploymentInformer := wrm.informerFactory.Apps().V1().Deployments().Informer()

	_, err := deploymentInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				if wrm.isDeploymentProblematic(deployment) {
					wrm.sendDeploymentRolloutStatus(deployment, "CREATE")
				}
				wrm.updateDeploymentRolloutSnapshot(deployment)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if deployment, ok := newObj.(*appsv1.Deployment); ok {
				if wrm.hasSignificantRolloutChange(deployment, "Deployment") {
					wrm.sendDeploymentRolloutStatus(deployment, "UPDATE")
				}
				wrm.updateDeploymentRolloutSnapshot(deployment)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if deployment, ok := obj.(*appsv1.Deployment); ok {
				if wrm.isDeploymentProblematic(deployment) {
					wrm.sendDeploymentRolloutStatus(deployment, "DELETE")
				}
				wrm.removeRolloutSnapshot(deployment.Namespace, deployment.Name, "Deployment")
			}
		},
	})
	if err != nil {
		wrm.logger.Error("Failed to add deployment rollout event handler", zap.Error(err))
	}
}

// setupStatefulSetInformer sets up the statefulset informer for rollout monitoring
func (wrm *WorkloadRolloutMonitor) setupStatefulSetInformer() {
	statefulSetInformer := wrm.informerFactory.Apps().V1().StatefulSets().Informer()

	_, err := statefulSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				if wrm.isStatefulSetProblematic(sts) {
					wrm.sendStatefulSetRolloutStatus(sts, "CREATE")
				}
				wrm.updateStatefulSetRolloutSnapshot(sts)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if sts, ok := newObj.(*appsv1.StatefulSet); ok {
				if wrm.hasSignificantRolloutChange(sts, "StatefulSet") {
					wrm.sendStatefulSetRolloutStatus(sts, "UPDATE")
				}
				wrm.updateStatefulSetRolloutSnapshot(sts)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				if wrm.isStatefulSetProblematic(sts) {
					wrm.sendStatefulSetRolloutStatus(sts, "DELETE")
				}
				wrm.removeRolloutSnapshot(sts.Namespace, sts.Name, "StatefulSet")
			}
		},
	})
	if err != nil {
		wrm.logger.Error("Failed to add statefulset rollout event handler", zap.Error(err))
	}
}

// setupDaemonSetInformer sets up the daemonset informer for rollout monitoring
func (wrm *WorkloadRolloutMonitor) setupDaemonSetInformer() {
	daemonSetInformer := wrm.informerFactory.Apps().V1().DaemonSets().Informer()

	_, err := daemonSetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if ds, ok := obj.(*appsv1.DaemonSet); ok {
				if wrm.isDaemonSetProblematic(ds) {
					wrm.sendDaemonSetRolloutStatus(ds, "CREATE")
				}
				wrm.updateDaemonSetRolloutSnapshot(ds)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if ds, ok := newObj.(*appsv1.DaemonSet); ok {
				if wrm.hasSignificantRolloutChange(ds, "DaemonSet") {
					wrm.sendDaemonSetRolloutStatus(ds, "UPDATE")
				}
				wrm.updateDaemonSetRolloutSnapshot(ds)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if ds, ok := obj.(*appsv1.DaemonSet); ok {
				if wrm.isDaemonSetProblematic(ds) {
					wrm.sendDaemonSetRolloutStatus(ds, "DELETE")
				}
				wrm.removeRolloutSnapshot(ds.Namespace, ds.Name, "DaemonSet")
			}
		},
	})
	if err != nil {
		wrm.logger.Error("Failed to add daemonset rollout event handler", zap.Error(err))
	}
}

// isDeploymentProblematic checks if a deployment has rollout issues
func (wrm *WorkloadRolloutMonitor) isDeploymentProblematic(deployment *appsv1.Deployment) bool {
	// Check for ReplicaFailure condition
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == corev1.ConditionTrue {
			return true
		}
		// Check for stuck Progressing condition
		if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse {
			if condition.Reason == "ProgressDeadlineExceeded" {
				return true
			}
		}
	}

	// Check if deployment is not making progress
	if deployment.Spec.Replicas != nil {
		desiredReplicas := *deployment.Spec.Replicas
		if deployment.Status.ReadyReplicas < desiredReplicas {
			// If not all replicas are ready and deployment is older than 10 minutes, it's problematic
			if time.Since(deployment.CreationTimestamp.Time) > 10*time.Minute {
				return true
			}
		}
	}

	// Check for generation mismatch (stuck rollout)
	if deployment.Status.ObservedGeneration < deployment.Generation {
		// Rollout hasn't been observed yet
		if time.Since(deployment.CreationTimestamp.Time) > 5*time.Minute {
			return true
		}
	}

	return false
}

// isStatefulSetProblematic checks if a statefulset has rollout issues
func (wrm *WorkloadRolloutMonitor) isStatefulSetProblematic(sts *appsv1.StatefulSet) bool {
	// Check if not all replicas are ready
	if sts.Spec.Replicas != nil {
		desiredReplicas := *sts.Spec.Replicas
		if sts.Status.ReadyReplicas < desiredReplicas {
			// StatefulSets take longer to roll out, so wait 15 minutes before flagging
			if time.Since(sts.CreationTimestamp.Time) > 15*time.Minute {
				return true
			}
		}
	}

	// Check for generation mismatch
	if sts.Status.ObservedGeneration < sts.Generation {
		if time.Since(sts.CreationTimestamp.Time) > 10*time.Minute {
			return true
		}
	}

	// Check for update strategy issues
	if sts.Status.CurrentRevision != sts.Status.UpdateRevision && sts.Status.UpdatedReplicas == 0 {
		// Update hasn't started
		if time.Since(sts.CreationTimestamp.Time) > 10*time.Minute {
			return true
		}
	}

	return false
}

// isDaemonSetProblematic checks if a daemonset has rollout issues
func (wrm *WorkloadRolloutMonitor) isDaemonSetProblematic(ds *appsv1.DaemonSet) bool {
	// Check if not all desired nodes have the daemonset
	if ds.Status.DesiredNumberScheduled > 0 {
		if ds.Status.NumberReady < ds.Status.DesiredNumberScheduled {
			// Wait 10 minutes before flagging as problematic
			if time.Since(ds.CreationTimestamp.Time) > 10*time.Minute {
				return true
			}
		}
	}

	// Check for generation mismatch
	if ds.Status.ObservedGeneration < ds.Generation {
		if time.Since(ds.CreationTimestamp.Time) > 10*time.Minute {
			return true
		}
	}

	// Check for number unavailable
	if ds.Status.NumberUnavailable > 0 {
		if time.Since(ds.CreationTimestamp.Time) > 10*time.Minute {
			return true
		}
	}

	return false
}

// hasSignificantRolloutChange checks if rollout status changed significantly
func (wrm *WorkloadRolloutMonitor) hasSignificantRolloutChange(obj interface{}, kind string) bool {
	var key string
	var currentSnapshot *rolloutStateSnapshot

	switch kind {
	case "Deployment":
		deployment := obj.(*appsv1.Deployment)
		key = fmt.Sprintf("%s/%s/%s", deployment.Namespace, "Deployment", deployment.Name)
		currentSnapshot = wrm.buildDeploymentRolloutSnapshot(deployment)
	case "StatefulSet":
		sts := obj.(*appsv1.StatefulSet)
		key = fmt.Sprintf("%s/%s/%s", sts.Namespace, "StatefulSet", sts.Name)
		currentSnapshot = wrm.buildStatefulSetRolloutSnapshot(sts)
	case "DaemonSet":
		ds := obj.(*appsv1.DaemonSet)
		key = fmt.Sprintf("%s/%s/%s", ds.Namespace, "DaemonSet", ds.Name)
		currentSnapshot = wrm.buildDaemonSetRolloutSnapshot(ds)
	default:
		return false
	}

	wrm.rolloutMu.RLock()
	previousSnapshot, exists := wrm.previousRolloutStates[key]
	wrm.rolloutMu.RUnlock()

	if !exists {
		// First time seeing this workload - check if it's problematic
		switch kind {
		case "Deployment":
			return wrm.isDeploymentProblematic(obj.(*appsv1.Deployment))
		case "StatefulSet":
			return wrm.isStatefulSetProblematic(obj.(*appsv1.StatefulSet))
		case "DaemonSet":
			return wrm.isDaemonSetProblematic(obj.(*appsv1.DaemonSet))
		}
		return false
	}

	// Check for replica count changes
	if currentSnapshot.readyReplicas != previousSnapshot.readyReplicas {
		return true
	}
	if currentSnapshot.availableReplicas != previousSnapshot.availableReplicas {
		return true
	}

	// Check for generation updates (new rollout started)
	if currentSnapshot.generation > previousSnapshot.generation {
		return true
	}

	// Check for condition changes
	for condType, currentStatus := range currentSnapshot.conditions {
		if previousStatus, exists := previousSnapshot.conditions[condType]; exists {
			if currentStatus != previousStatus {
				return true
			}
		}
	}

	return false
}

// buildDeploymentRolloutSnapshot builds a snapshot of deployment rollout state
func (wrm *WorkloadRolloutMonitor) buildDeploymentRolloutSnapshot(deployment *appsv1.Deployment) *rolloutStateSnapshot {
	snapshot := &rolloutStateSnapshot{
		generation:         deployment.Generation,
		observedGeneration: deployment.Status.ObservedGeneration,
		conditions:         make(map[string]string),
	}

	if deployment.Spec.Replicas != nil {
		snapshot.replicas = *deployment.Spec.Replicas
	}
	snapshot.updatedReplicas = deployment.Status.UpdatedReplicas
	snapshot.readyReplicas = deployment.Status.ReadyReplicas
	snapshot.availableReplicas = deployment.Status.AvailableReplicas

	for _, condition := range deployment.Status.Conditions {
		snapshot.conditions[string(condition.Type)] = string(condition.Status)
	}

	return snapshot
}

// buildStatefulSetRolloutSnapshot builds a snapshot of statefulset rollout state
func (wrm *WorkloadRolloutMonitor) buildStatefulSetRolloutSnapshot(sts *appsv1.StatefulSet) *rolloutStateSnapshot {
	snapshot := &rolloutStateSnapshot{
		generation:         sts.Generation,
		observedGeneration: sts.Status.ObservedGeneration,
		conditions:         make(map[string]string),
	}

	if sts.Spec.Replicas != nil {
		snapshot.replicas = *sts.Spec.Replicas
	}
	snapshot.updatedReplicas = sts.Status.UpdatedReplicas
	snapshot.readyReplicas = sts.Status.ReadyReplicas
	snapshot.availableReplicas = sts.Status.AvailableReplicas

	// StatefulSets don't have standard conditions like Deployments in older K8s versions
	// But we can track the status fields

	return snapshot
}

// buildDaemonSetRolloutSnapshot builds a snapshot of daemonset rollout state
func (wrm *WorkloadRolloutMonitor) buildDaemonSetRolloutSnapshot(ds *appsv1.DaemonSet) *rolloutStateSnapshot {
	snapshot := &rolloutStateSnapshot{
		generation:         ds.Generation,
		observedGeneration: ds.Status.ObservedGeneration,
		replicas:           ds.Status.DesiredNumberScheduled,
		updatedReplicas:    ds.Status.UpdatedNumberScheduled,
		readyReplicas:      ds.Status.NumberReady,
		availableReplicas:  ds.Status.NumberAvailable,
		conditions:         make(map[string]string),
	}

	return snapshot
}

// updateDeploymentRolloutSnapshot updates the stored rollout snapshot for a deployment
func (wrm *WorkloadRolloutMonitor) updateDeploymentRolloutSnapshot(deployment *appsv1.Deployment) {
	key := fmt.Sprintf("%s/%s/%s", deployment.Namespace, "Deployment", deployment.Name)
	snapshot := wrm.buildDeploymentRolloutSnapshot(deployment)

	wrm.rolloutMu.Lock()
	wrm.previousRolloutStates[key] = snapshot
	wrm.rolloutMu.Unlock()
}

// updateStatefulSetRolloutSnapshot updates the stored rollout snapshot for a statefulset
func (wrm *WorkloadRolloutMonitor) updateStatefulSetRolloutSnapshot(sts *appsv1.StatefulSet) {
	key := fmt.Sprintf("%s/%s/%s", sts.Namespace, "StatefulSet", sts.Name)
	snapshot := wrm.buildStatefulSetRolloutSnapshot(sts)

	wrm.rolloutMu.Lock()
	wrm.previousRolloutStates[key] = snapshot
	wrm.rolloutMu.Unlock()
}

// updateDaemonSetRolloutSnapshot updates the stored rollout snapshot for a daemonset
func (wrm *WorkloadRolloutMonitor) updateDaemonSetRolloutSnapshot(ds *appsv1.DaemonSet) {
	key := fmt.Sprintf("%s/%s/%s", ds.Namespace, "DaemonSet", ds.Name)
	snapshot := wrm.buildDaemonSetRolloutSnapshot(ds)

	wrm.rolloutMu.Lock()
	wrm.previousRolloutStates[key] = snapshot
	wrm.rolloutMu.Unlock()
}

// removeRolloutSnapshot removes the rollout snapshot for a deleted workload
func (wrm *WorkloadRolloutMonitor) removeRolloutSnapshot(namespace, name, kind string) {
	key := fmt.Sprintf("%s/%s/%s", namespace, kind, name)

	wrm.rolloutMu.Lock()
	delete(wrm.previousRolloutStates, key)
	wrm.rolloutMu.Unlock()
}

// sendDeploymentRolloutStatus sends deployment rollout status to the stream
func (wrm *WorkloadRolloutMonitor) sendDeploymentRolloutStatus(deployment *appsv1.Deployment, action string) {
	var desiredReplicas int32
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	// Determine rollout status based on conditions
	rolloutStatus, rolloutReason, rolloutMessage := wrm.determineDeploymentRolloutStatus(deployment)

	// Convert conditions
	var protoConditions []*v1.WorkloadCondition
	for _, condition := range deployment.Status.Conditions {
		protoCondition := &v1.WorkloadCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  condition.Reason,
			Message: condition.Message,
		}
		if !condition.LastUpdateTime.IsZero() {
			protoCondition.LastUpdateTime = timestamppb.New(condition.LastUpdateTime.Time)
		}
		if !condition.LastTransitionTime.IsZero() {
			protoCondition.LastTransitionTime = timestamppb.New(condition.LastTransitionTime.Time)
		}
		protoConditions = append(protoConditions, protoCondition)
	}

	protoRolloutStatus := &v1.WorkloadRolloutStatus{
		Name:                deployment.Name,
		Namespace:           deployment.Namespace,
		Uid:                 string(deployment.UID),
		Kind:                "Deployment",
		RolloutStatus:       rolloutStatus,
		RolloutReason:       rolloutReason,
		RolloutMessage:      rolloutMessage,
		DesiredReplicas:     desiredReplicas,
		CurrentReplicas:     deployment.Status.Replicas,
		ReadyReplicas:       deployment.Status.ReadyReplicas,
		AvailableReplicas:   deployment.Status.AvailableReplicas,
		UpdatedReplicas:     deployment.Status.UpdatedReplicas,
		UnavailableReplicas: deployment.Status.UnavailableReplicas,
		Conditions:          protoConditions,
		ObservedGeneration:  deployment.Status.ObservedGeneration,
		MetadataGeneration:  deployment.Generation,
		CreatedAt:           timestamppb.New(deployment.CreationTimestamp.Time),
		StatusTimestamp:     timestamppb.Now(),
		Action:              stringToAction(action),
	}

	select {
	case wrm.rolloutChan <- protoRolloutStatus:
		wrm.logger.Info("Sent deployment rollout status",
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace),
			zap.String("status", protoRolloutStatus.RolloutStatus),
			zap.String("reason", protoRolloutStatus.RolloutReason),
			zap.Int32("ready", protoRolloutStatus.ReadyReplicas),
			zap.Int32("desired", protoRolloutStatus.DesiredReplicas))
	default:
		wrm.logger.Warn("Rollout status channel full, dropping event",
			zap.String("kind", "Deployment"),
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace))
	}
}

// sendStatefulSetRolloutStatus sends statefulset rollout status to the stream
func (wrm *WorkloadRolloutMonitor) sendStatefulSetRolloutStatus(sts *appsv1.StatefulSet, action string) {
	var desiredReplicas int32
	if sts.Spec.Replicas != nil {
		desiredReplicas = *sts.Spec.Replicas
	}

	// Determine rollout status
	rolloutStatus, rolloutReason, rolloutMessage := wrm.determineStatefulSetRolloutStatus(sts)

	protoRolloutStatus := &v1.WorkloadRolloutStatus{
		Name:                sts.Name,
		Namespace:           sts.Namespace,
		Uid:                 string(sts.UID),
		Kind:                "StatefulSet",
		RolloutStatus:       rolloutStatus,
		RolloutReason:       rolloutReason,
		RolloutMessage:      rolloutMessage,
		DesiredReplicas:     desiredReplicas,
		CurrentReplicas:     sts.Status.Replicas,
		ReadyReplicas:       sts.Status.ReadyReplicas,
		AvailableReplicas:   sts.Status.AvailableReplicas,
		UpdatedReplicas:     sts.Status.UpdatedReplicas,
		UnavailableReplicas: desiredReplicas - sts.Status.AvailableReplicas,
		Conditions:          []*v1.WorkloadCondition{}, // StatefulSets have limited condition support
		ObservedGeneration:  sts.Status.ObservedGeneration,
		MetadataGeneration:  sts.Generation,
		CreatedAt:           timestamppb.New(sts.CreationTimestamp.Time),
		StatusTimestamp:     timestamppb.Now(),
		Action:              stringToAction(action),
	}

	select {
	case wrm.rolloutChan <- protoRolloutStatus:
		wrm.logger.Info("Sent statefulset rollout status",
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace),
			zap.String("status", protoRolloutStatus.RolloutStatus),
			zap.Int32("ready", protoRolloutStatus.ReadyReplicas),
			zap.Int32("desired", protoRolloutStatus.DesiredReplicas))
	default:
		wrm.logger.Warn("Rollout status channel full, dropping event",
			zap.String("kind", "StatefulSet"),
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace))
	}
}

// sendDaemonSetRolloutStatus sends daemonset rollout status to the stream
func (wrm *WorkloadRolloutMonitor) sendDaemonSetRolloutStatus(ds *appsv1.DaemonSet, action string) {
	rolloutStatus, rolloutReason, rolloutMessage := wrm.determineDaemonSetRolloutStatus(ds)

	protoRolloutStatus := &v1.WorkloadRolloutStatus{
		Name:                ds.Name,
		Namespace:           ds.Namespace,
		Uid:                 string(ds.UID),
		Kind:                "DaemonSet",
		RolloutStatus:       rolloutStatus,
		RolloutReason:       rolloutReason,
		RolloutMessage:      rolloutMessage,
		DesiredReplicas:     ds.Status.DesiredNumberScheduled,
		CurrentReplicas:     ds.Status.CurrentNumberScheduled,
		ReadyReplicas:       ds.Status.NumberReady,
		AvailableReplicas:   ds.Status.NumberAvailable,
		UpdatedReplicas:     ds.Status.UpdatedNumberScheduled,
		UnavailableReplicas: ds.Status.NumberUnavailable,
		Conditions:          []*v1.WorkloadCondition{}, // DaemonSets have limited condition support
		ObservedGeneration:  ds.Status.ObservedGeneration,
		MetadataGeneration:  ds.Generation,
		CreatedAt:           timestamppb.New(ds.CreationTimestamp.Time),
		StatusTimestamp:     timestamppb.Now(),
		Action:              stringToAction(action),
	}

	select {
	case wrm.rolloutChan <- protoRolloutStatus:
		wrm.logger.Info("Sent daemonset rollout status",
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace),
			zap.String("status", protoRolloutStatus.RolloutStatus),
			zap.Int32("ready", protoRolloutStatus.ReadyReplicas),
			zap.Int32("desired", protoRolloutStatus.DesiredReplicas))
	default:
		wrm.logger.Warn("Rollout status channel full, dropping event",
			zap.String("kind", "DaemonSet"),
			zap.String("name", protoRolloutStatus.Name),
			zap.String("namespace", protoRolloutStatus.Namespace))
	}
}

// determineDeploymentRolloutStatus analyzes deployment conditions to determine rollout status
func (wrm *WorkloadRolloutMonitor) determineDeploymentRolloutStatus(deployment *appsv1.Deployment) (string, string, string) {
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == corev1.ConditionTrue {
			return "Failed", condition.Reason, condition.Message
		}
		if condition.Type == appsv1.DeploymentProgressing {
			if condition.Status == corev1.ConditionFalse && condition.Reason == "ProgressDeadlineExceeded" {
				return "TimedOut", "ProgressDeadlineExceeded", condition.Message
			}
			if condition.Status == corev1.ConditionTrue && condition.Reason == "NewReplicaSetAvailable" {
				return "Complete", "NewReplicaSetAvailable", condition.Message
			}
			if condition.Status == corev1.ConditionTrue {
				return "Progressing", condition.Reason, condition.Message
			}
		}
	}

	// No explicit condition - infer from replica status
	if deployment.Spec.Replicas != nil {
		desiredReplicas := *deployment.Spec.Replicas
		if deployment.Status.ReadyReplicas >= desiredReplicas && deployment.Status.UpdatedReplicas >= desiredReplicas {
			return "Complete", "AllReplicasReady", "All replicas are ready and updated"
		}
		if deployment.Status.UpdatedReplicas > 0 {
			return "Progressing", "RollingUpdate", "Deployment is rolling out new replicas"
		}
	}

	return "Unknown", "", ""
}

// determineStatefulSetRolloutStatus analyzes statefulset status to determine rollout status
func (wrm *WorkloadRolloutMonitor) determineStatefulSetRolloutStatus(sts *appsv1.StatefulSet) (string, string, string) {
	if sts.Spec.Replicas != nil {
		desiredReplicas := *sts.Spec.Replicas
		if sts.Status.ReadyReplicas >= desiredReplicas && sts.Status.CurrentReplicas >= desiredReplicas {
			if sts.Status.CurrentRevision == sts.Status.UpdateRevision {
				return "Complete", "AllReplicasReady", "All replicas are ready and at current revision"
			}
		}
		if sts.Status.UpdatedReplicas > 0 && sts.Status.UpdatedReplicas < desiredReplicas {
			return "Progressing", "RollingUpdate", "StatefulSet is rolling out updated replicas"
		}
		if sts.Status.ReadyReplicas < desiredReplicas {
			if time.Since(sts.CreationTimestamp.Time) > 15*time.Minute {
				return "Failed", "ReplicasNotReady", fmt.Sprintf("Only %d/%d replicas ready after 15 minutes", sts.Status.ReadyReplicas, desiredReplicas)
			}
			return "Progressing", "WaitingForReplicas", fmt.Sprintf("Waiting for %d/%d replicas to be ready", sts.Status.ReadyReplicas, desiredReplicas)
		}
	}

	return "Unknown", "", ""
}

// determineDaemonSetRolloutStatus analyzes daemonset status to determine rollout status
func (wrm *WorkloadRolloutMonitor) determineDaemonSetRolloutStatus(ds *appsv1.DaemonSet) (string, string, string) {
	if ds.Status.DesiredNumberScheduled > 0 {
		if ds.Status.NumberReady >= ds.Status.DesiredNumberScheduled && ds.Status.UpdatedNumberScheduled >= ds.Status.DesiredNumberScheduled {
			return "Complete", "AllNodesReady", "DaemonSet pods are running on all desired nodes"
		}
		if ds.Status.UpdatedNumberScheduled > 0 && ds.Status.UpdatedNumberScheduled < ds.Status.DesiredNumberScheduled {
			return "Progressing", "RollingUpdate", "DaemonSet is rolling out to nodes"
		}
		if ds.Status.NumberUnavailable > 0 {
			if time.Since(ds.CreationTimestamp.Time) > 10*time.Minute {
				return "Failed", "PodsUnavailable", fmt.Sprintf("%d pods are unavailable after 10 minutes", ds.Status.NumberUnavailable)
			}
			return "Progressing", "WaitingForPods", fmt.Sprintf("Waiting for %d unavailable pods", ds.Status.NumberUnavailable)
		}
	}

	return "Unknown", "", ""
}

// sendInitialRolloutInventory sends workloads with problematic rollout states
func (wrm *WorkloadRolloutMonitor) sendInitialRolloutInventory(ctx context.Context) error {
	wrm.logger.Info("Sending initial workload rollout inventory (problematic rollouts only)")

	problematicCount := 0

	// Check deployments
	deployments, err := wrm.clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %w", err)
	}
	for _, deployment := range deployments.Items {
		if wrm.isDeploymentProblematic(&deployment) {
			wrm.sendDeploymentRolloutStatus(&deployment, "CREATE")
			problematicCount++
		}
		wrm.updateDeploymentRolloutSnapshot(&deployment)
	}

	// Check statefulsets
	statefulSets, err := wrm.clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list statefulsets: %w", err)
	}
	for _, sts := range statefulSets.Items {
		if wrm.isStatefulSetProblematic(&sts) {
			wrm.sendStatefulSetRolloutStatus(&sts, "CREATE")
			problematicCount++
		}
		wrm.updateStatefulSetRolloutSnapshot(&sts)
	}

	// Check daemonsets
	daemonSets, err := wrm.clientset.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list daemonsets: %w", err)
	}
	for _, ds := range daemonSets.Items {
		if wrm.isDaemonSetProblematic(&ds) {
			wrm.sendDaemonSetRolloutStatus(&ds, "CREATE")
			problematicCount++
		}
		wrm.updateDaemonSetRolloutSnapshot(&ds)
	}

	wrm.logger.Info("Completed sending initial workload rollout inventory",
		zap.Int("total_deployments", len(deployments.Items)),
		zap.Int("total_statefulsets", len(statefulSets.Items)),
		zap.Int("total_daemonsets", len(daemonSets.Items)),
		zap.Int("problematic_workloads", problematicCount))
	return nil
}
