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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// PodStatusMonitor monitors pod and container status changes for incident detection
type PodStatusMonitor struct {
	clientset        *kubernetes.Clientset
	logger           *zap.Logger
	statusChangeChan chan *v1.PodStatusChange
	informerFactory  informers.SharedInformerFactory
	stopCh           chan struct{}
	stopped          bool
	mu               sync.Mutex

	// Track previous pod states to detect meaningful changes
	previousStates map[string]*podStateSnapshot
	statesMu       sync.RWMutex
}

// podStateSnapshot captures the relevant state of a pod for comparison
type podStateSnapshot struct {
	phase             string
	totalRestartCount int32
	containerStates   map[string]string // container name -> state reason
	conditions        map[string]string // condition type -> status
}

// NewPodStatusMonitor creates a new pod status monitor for incident detection
func NewPodStatusMonitor(logger *zap.Logger, statusChangeChan chan *v1.PodStatusChange) (*PodStatusMonitor, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory with shorter resync for incident detection
	informerFactory := informers.NewSharedInformerFactory(clientset, 10*time.Second)

	return &PodStatusMonitor{
		clientset:        clientset,
		logger:           logger,
		statusChangeChan: statusChangeChan,
		informerFactory:  informerFactory,
		stopCh:           make(chan struct{}),
		previousStates:   make(map[string]*podStateSnapshot),
	}, nil
}

// StartSync starts the pod status monitor and signals when initial sync is complete
func (psm *PodStatusMonitor) StartSync(ctx context.Context, syncDone chan<- error) error {
	psm.logger.Info("Starting pod status monitor for incident detection")

	// Set up pod informer - AddFunc will be called for all existing pods during cache sync
	psm.setupPodInformer()

	// Signal that setup is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers - AddFunc will be called for all existing problematic pods during sync
	psm.informerFactory.Start(psm.stopCh)

	// Wait for all caches to sync before processing events
	psm.logger.Info("Waiting for pod status informer cache to sync...")
	if !cache.WaitForCacheSync(psm.stopCh,
		psm.informerFactory.Core().V1().Pods().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for pod status informer cache to sync")
	}
	psm.logger.Info("Pod status informer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	psm.safeClose()
	psm.logger.Info("Stopped pod status monitor")
	return nil
}

// Stop stops the pod status monitor
func (psm *PodStatusMonitor) Stop() {
	psm.safeClose()
}

// safeClose safely closes the stop channel only once
func (psm *PodStatusMonitor) safeClose() {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	if !psm.stopped {
		close(psm.stopCh)
		psm.stopped = true
	}
}

// setupPodInformer sets up the pod informer to track status changes
func (psm *PodStatusMonitor) setupPodInformer() {
	podInformer := psm.informerFactory.Core().V1().Pods().Informer()

	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				// Check if this pod is in a problematic state
				if psm.isPodProblematic(pod) {
					psm.sendPodStatus(pod, "CREATE")
				}
				// Always store the current state for future comparisons
				psm.updatePodStateSnapshot(pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				// Check if the status changed in a meaningful way
				if psm.hasSignificantStatusChange(pod) {
					psm.sendPodStatus(pod, "UPDATE")
				}
				// Update the state snapshot
				psm.updatePodStateSnapshot(pod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				// Send deletion if pod was in problematic state
				if psm.isPodProblematic(pod) {
					psm.sendPodStatus(pod, "DELETE")
				}
				// Clean up state snapshot
				psm.removePodStateSnapshot(pod)
			}
		},
	})
	if err != nil {
		psm.logger.Error("Failed to add pod status event handler", zap.Error(err))
	}
}

// isPodProblematic checks if a pod is in a problematic state indicating an incident
func (psm *PodStatusMonitor) isPodProblematic(pod *corev1.Pod) bool {
	// Check pod phase
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodUnknown {
		return true
	}

	// Check container statuses for problematic states
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Waiting != nil {
			reason := containerStatus.State.Waiting.Reason
			// Common problematic container states
			if reason == "CrashLoopBackOff" || reason == "ImagePullBackOff" ||
				reason == "ErrImagePull" || reason == "CreateContainerError" ||
				reason == "InvalidImageName" || reason == "CreateContainerConfigError" {
				return true
			}
		}

		if containerStatus.State.Terminated != nil {
			reason := containerStatus.State.Terminated.Reason
			// OOMKilled is a critical incident signal
			if reason == "OOMKilled" || reason == "Error" {
				return true
			}
		}

		// High restart count indicates persistent issues
		if containerStatus.RestartCount >= 3 {
			return true
		}
	}

	// Check pod conditions
	for _, condition := range pod.Status.Conditions {
		// Pod not ready for extended period
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse {
			// If pod has been not ready for more than 2 minutes, it's problematic
			if time.Since(condition.LastTransitionTime.Time) > 2*time.Minute {
				return true
			}
		}

		// Container readiness issues
		if condition.Type == corev1.ContainersReady && condition.Status == corev1.ConditionFalse {
			return true
		}
	}

	return false
}

// hasSignificantStatusChange checks if a pod's status changed in a meaningful way
func (psm *PodStatusMonitor) hasSignificantStatusChange(pod *corev1.Pod) bool {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	psm.statesMu.RLock()
	previousState, exists := psm.previousStates[podKey]
	psm.statesMu.RUnlock()

	if !exists {
		// First time seeing this pod - check if it's problematic
		return psm.isPodProblematic(pod)
	}

	// Check for phase changes
	if pod.Status.Phase != corev1.PodPhase(previousState.phase) {
		return true
	}

	// Check for restart count increases
	currentRestartCount := psm.getTotalRestartCount(pod)
	if currentRestartCount > previousState.totalRestartCount {
		return true
	}

	// Check for container state changes
	for _, containerStatus := range pod.Status.ContainerStatuses {
		currentReason := psm.getContainerStateReason(containerStatus)
		previousReason, exists := previousState.containerStates[containerStatus.Name]

		if !exists || currentReason != previousReason {
			// Container state changed
			if currentReason != "" && currentReason != "Running" {
				// Only report non-running states
				return true
			}
		}
	}

	// Check for condition changes
	for _, condition := range pod.Status.Conditions {
		currentStatus := string(condition.Status)
		previousStatus, exists := previousState.conditions[string(condition.Type)]

		if !exists || currentStatus != previousStatus {
			// Condition changed
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse {
				return true
			}
			if condition.Type == corev1.ContainersReady && condition.Status == corev1.ConditionFalse {
				return true
			}
		}
	}

	return false
}

// updatePodStateSnapshot updates the stored state snapshot for a pod
func (psm *PodStatusMonitor) updatePodStateSnapshot(pod *corev1.Pod) {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	containerStates := make(map[string]string)
	for _, containerStatus := range pod.Status.ContainerStatuses {
		containerStates[containerStatus.Name] = psm.getContainerStateReason(containerStatus)
	}

	conditions := make(map[string]string)
	for _, condition := range pod.Status.Conditions {
		conditions[string(condition.Type)] = string(condition.Status)
	}

	snapshot := &podStateSnapshot{
		phase:             string(pod.Status.Phase),
		totalRestartCount: psm.getTotalRestartCount(pod),
		containerStates:   containerStates,
		conditions:        conditions,
	}

	psm.statesMu.Lock()
	psm.previousStates[podKey] = snapshot
	psm.statesMu.Unlock()
}

// removePodStateSnapshot removes the state snapshot for a deleted pod
func (psm *PodStatusMonitor) removePodStateSnapshot(pod *corev1.Pod) {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	psm.statesMu.Lock()
	delete(psm.previousStates, podKey)
	psm.statesMu.Unlock()
}

// getTotalRestartCount calculates the total restart count across all containers
func (psm *PodStatusMonitor) getTotalRestartCount(pod *corev1.Pod) int32 {
	var total int32
	for _, containerStatus := range pod.Status.ContainerStatuses {
		total += containerStatus.RestartCount
	}
	for _, initContainerStatus := range pod.Status.InitContainerStatuses {
		total += initContainerStatus.RestartCount
	}
	return total
}

// getContainerStateReason returns the reason for the current container state
func (psm *PodStatusMonitor) getContainerStateReason(containerStatus corev1.ContainerStatus) string {
	if containerStatus.State.Waiting != nil {
		return containerStatus.State.Waiting.Reason
	}
	if containerStatus.State.Running != nil {
		return "Running"
	}
	if containerStatus.State.Terminated != nil {
		return containerStatus.State.Terminated.Reason
	}
	return ""
}

// sendPodStatus converts a Pod to protobuf PodStatusChange and sends it to the stream
func (psm *PodStatusMonitor) sendPodStatus(pod *corev1.Pod, action string) {
	// Convert container statuses
	var protoContainerStatuses []*v1.ContainerStatus
	for _, cs := range pod.Status.ContainerStatuses {
		protoCS := psm.convertContainerStatus(cs)
		protoContainerStatuses = append(protoContainerStatuses, protoCS)
	}

	// Convert init container statuses
	var protoInitContainerStatuses []*v1.ContainerStatus
	for _, cs := range pod.Status.InitContainerStatuses {
		protoCS := psm.convertContainerStatus(cs)
		protoInitContainerStatuses = append(protoInitContainerStatuses, protoCS)
	}

	// Convert pod conditions
	var protoConditions []*v1.PodCondition
	for _, condition := range pod.Status.Conditions {
		protoCondition := &v1.PodCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  condition.Reason,
			Message: condition.Message,
		}
		if !condition.LastProbeTime.IsZero() {
			protoCondition.LastProbeTime = timestamppb.New(condition.LastProbeTime.Time)
		}
		if !condition.LastTransitionTime.IsZero() {
			protoCondition.LastTransitionTime = timestamppb.New(condition.LastTransitionTime.Time)
		}
		protoConditions = append(protoConditions, protoCondition)
	}

	// Convert owner references
	var protoOwnerRefs []*v1.OwnerReference
	for _, ref := range pod.OwnerReferences {
		protoOwnerRefs = append(protoOwnerRefs, &v1.OwnerReference{
			ApiVersion:         ref.APIVersion,
			Kind:               ref.Kind,
			Name:               ref.Name,
			Uid:                string(ref.UID),
			Controller:         ref.Controller != nil && *ref.Controller,
			BlockOwnerDeletion: ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion,
		})
	}

	// Calculate total restart count
	totalRestartCount := psm.getTotalRestartCount(pod)

	protoPodStatus := &v1.PodStatusChange{
		Name:                  pod.Name,
		Namespace:             pod.Namespace,
		Uid:                   string(pod.UID),
		PodIp:                 pod.Status.PodIP,
		NodeName:              pod.Spec.NodeName,
		Phase:                 string(pod.Status.Phase),
		ContainerStatuses:     protoContainerStatuses,
		InitContainerStatuses: protoInitContainerStatuses,
		Conditions:            protoConditions,
		QosClass:              string(pod.Status.QOSClass),
		TotalRestartCount:     totalRestartCount,
		ResourceUsage:         psm.extractResourceUsage(pod),
		OwnerReferences:       protoOwnerRefs,
		CreatedAt:             timestamppb.New(pod.CreationTimestamp.Time),
		StatusTimestamp:       timestamppb.Now(),
		Action:                stringToAction(action),
	}

	select {
	case psm.statusChangeChan <- protoPodStatus:
		psm.logger.Info("Sent pod status change",
			zap.String("name", protoPodStatus.Name),
			zap.String("namespace", protoPodStatus.Namespace),
			zap.String("phase", protoPodStatus.Phase),
			zap.Int32("totalRestarts", protoPodStatus.TotalRestartCount),
			zap.String("action", protoPodStatus.Action.String()))
	default:
		psm.logger.Warn("Pod status change channel full, dropping event",
			zap.String("name", protoPodStatus.Name),
			zap.String("namespace", protoPodStatus.Namespace),
			zap.String("action", protoPodStatus.Action.String()))
	}
}

// convertContainerStatus converts a Kubernetes ContainerStatus to protobuf
func (psm *PodStatusMonitor) convertContainerStatus(cs corev1.ContainerStatus) *v1.ContainerStatus {
	protoCS := &v1.ContainerStatus{
		Name:         cs.Name,
		Image:        cs.Image,
		ImageId:      cs.ImageID,
		ContainerId:  cs.ContainerID,
		Ready:        cs.Ready,
		RestartCount: cs.RestartCount,
		Started:      cs.Started != nil && *cs.Started,
	}

	// Convert current state
	protoCS.State = psm.convertContainerState(cs.State)

	// Convert last termination state if available
	if cs.LastTerminationState.Terminated != nil || cs.LastTerminationState.Waiting != nil || cs.LastTerminationState.Running != nil {
		protoCS.LastTerminationState = psm.convertContainerState(cs.LastTerminationState)
	}

	return protoCS
}

// convertContainerState converts a Kubernetes ContainerState to protobuf
func (psm *PodStatusMonitor) convertContainerState(state corev1.ContainerState) *v1.ContainerState {
	protoState := &v1.ContainerState{}

	if state.Waiting != nil {
		protoState.State = &v1.ContainerState_Waiting{
			Waiting: &v1.ContainerStateWaiting{
				Reason:  state.Waiting.Reason,
				Message: state.Waiting.Message,
			},
		}
	} else if state.Running != nil {
		protoRunning := &v1.ContainerStateRunning{}
		if !state.Running.StartedAt.IsZero() {
			protoRunning.StartedAt = timestamppb.New(state.Running.StartedAt.Time)
		}
		protoState.State = &v1.ContainerState_Running{
			Running: protoRunning,
		}
	} else if state.Terminated != nil {
		protoTerminated := &v1.ContainerStateTerminated{
			ExitCode:    state.Terminated.ExitCode,
			Signal:      state.Terminated.Signal,
			Reason:      state.Terminated.Reason,
			Message:     state.Terminated.Message,
			ContainerId: state.Terminated.ContainerID,
		}
		if !state.Terminated.StartedAt.IsZero() {
			protoTerminated.StartedAt = timestamppb.New(state.Terminated.StartedAt.Time)
		}
		if !state.Terminated.FinishedAt.IsZero() {
			protoTerminated.FinishedAt = timestamppb.New(state.Terminated.FinishedAt.Time)
		}
		protoState.State = &v1.ContainerState_Terminated{
			Terminated: protoTerminated,
		}
	}

	return protoState
}

// extractResourceUsage extracts resource usage information from pod
func (psm *PodStatusMonitor) extractResourceUsage(pod *corev1.Pod) *v1.ResourceUsage {
	// Note: Actual resource usage (CPU/memory consumed) requires metrics-server integration
	// For now, we extract resource requests and limits from the pod spec

	var cpuRequestNano, memoryRequestBytes, cpuLimitNano, memoryLimitBytes int64

	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			if cpuRequest := container.Resources.Requests.Cpu(); cpuRequest != nil {
				cpuRequestNano += cpuRequest.MilliValue() * 1000000 // Convert milli-cores to nano-cores
			}
			if memRequest := container.Resources.Requests.Memory(); memRequest != nil {
				memoryRequestBytes += memRequest.Value()
			}
		}
		if container.Resources.Limits != nil {
			if cpuLimit := container.Resources.Limits.Cpu(); cpuLimit != nil {
				cpuLimitNano += cpuLimit.MilliValue() * 1000000
			}
			if memLimit := container.Resources.Limits.Memory(); memLimit != nil {
				memoryLimitBytes += memLimit.Value()
			}
		}
	}

	return &v1.ResourceUsage{
		CpuRequestNanoCores: cpuRequestNano,
		MemoryRequestBytes:  memoryRequestBytes,
		CpuLimitNanoCores:   cpuLimitNano,
		MemoryLimitBytes:    memoryLimitBytes,
		// Actual usage would be populated by metrics-server integration
		CpuUsageNanoCores: 0,
		MemoryUsageBytes:  0,
	}
}

// sendInitialPodStatusInventory sends pods currently in problematic states
func (psm *PodStatusMonitor) sendInitialPodStatusInventory(ctx context.Context) error {
	psm.logger.Info("Sending initial pod status inventory (problematic pods only)")

	pods, err := psm.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	problematicCount := 0
	for _, pod := range pods.Items {
		if psm.isPodProblematic(&pod) {
			psm.sendPodStatus(&pod, "CREATE")
			problematicCount++
		}
		// Store initial state for all pods
		psm.updatePodStateSnapshot(&pod)
	}

	psm.logger.Info("Completed sending initial pod status inventory",
		zap.Int("total_pods", len(pods.Items)),
		zap.Int("problematic_pods", problematicCount))
	return nil
}
