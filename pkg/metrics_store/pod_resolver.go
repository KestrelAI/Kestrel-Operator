package metrics_store

import (
	"strings"
	"sync"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

// PodWorkloadResolver maps pods to their parent workloads.
type PodWorkloadResolver struct {
	mu sync.RWMutex

	// podUID → workload info
	podToWorkload map[string]WorkloadInfo

	// "namespace/podName" → podUID
	podNameToUID map[string]string

	logger *zap.Logger
}

// WorkloadInfo contains information about a pod's parent workload.
type WorkloadInfo struct {
	Namespace string
	Kind      string // Deployment, StatefulSet, DaemonSet, ReplicaSet, Job
	Name      string
}

// NewPodWorkloadResolver creates a new resolver.
func NewPodWorkloadResolver(logger *zap.Logger) *PodWorkloadResolver {
	return &PodWorkloadResolver{
		podToWorkload: make(map[string]WorkloadInfo),
		podNameToUID:  make(map[string]string),
		logger:        logger,
	}
}

// RegisterPod registers a pod and extracts its workload information.
func (r *PodWorkloadResolver) RegisterPod(pod *v1.Pod) {
	if pod == nil || pod.Uid == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Find the controller owner reference
	var controllerOwner *v1.OwnerReference
	for _, ref := range pod.OwnerReferences {
		if ref.Controller {
			controllerOwner = ref
			break
		}
	}

	if controllerOwner == nil {
		// No controller owner, pod might be standalone
		r.logger.Debug("Pod has no controller owner reference",
			zap.String("pod", pod.Name),
			zap.String("namespace", pod.Namespace))
		return
	}

	workload := r.resolveWorkloadFromOwner(pod.Namespace, controllerOwner)

	r.podToWorkload[pod.Uid] = workload
	r.podNameToUID[r.podKey(pod.Namespace, pod.Name)] = pod.Uid

	r.logger.Debug("Registered pod workload mapping",
		zap.String("pod", pod.Name),
		zap.String("namespace", pod.Namespace),
		zap.String("workload", workload.Name),
		zap.String("kind", workload.Kind))
}

// UnregisterPod removes a pod from the resolver.
func (r *PodWorkloadResolver) UnregisterPod(namespace, name, uid string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.podToWorkload, uid)
	delete(r.podNameToUID, r.podKey(namespace, name))

	r.logger.Debug("Unregistered pod workload mapping",
		zap.String("pod", name),
		zap.String("namespace", namespace))
}

// ResolveWorkload returns the workload info for a pod by name.
func (r *PodWorkloadResolver) ResolveWorkload(namespace, podName string) (WorkloadInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	uid, ok := r.podNameToUID[r.podKey(namespace, podName)]
	if !ok {
		return WorkloadInfo{}, false
	}

	info, ok := r.podToWorkload[uid]
	return info, ok
}

// ResolveWorkloadByUID returns the workload info for a pod by UID.
func (r *PodWorkloadResolver) ResolveWorkloadByUID(uid string) (WorkloadInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, ok := r.podToWorkload[uid]
	return info, ok
}

// resolveWorkloadFromOwner extracts the top-level workload from an owner reference.
func (r *PodWorkloadResolver) resolveWorkloadFromOwner(namespace string, owner *v1.OwnerReference) WorkloadInfo {
	switch owner.Kind {
	case "ReplicaSet":
		// ReplicaSet is owned by Deployment
		// ReplicaSet name format: <deployment-name>-<hash>
		deploymentName := r.parseDeploymentFromReplicaSet(owner.Name)
		return WorkloadInfo{
			Namespace: namespace,
			Kind:      "Deployment",
			Name:      deploymentName,
		}

	case "StatefulSet", "DaemonSet", "Job":
		// These directly own pods
		return WorkloadInfo{
			Namespace: namespace,
			Kind:      owner.Kind,
			Name:      owner.Name,
		}

	default:
		// Unknown owner type, use as-is
		return WorkloadInfo{
			Namespace: namespace,
			Kind:      owner.Kind,
			Name:      owner.Name,
		}
	}
}

// parseDeploymentFromReplicaSet extracts deployment name from ReplicaSet name.
// ReplicaSet name format: <deployment-name>-<pod-template-hash>
func (r *PodWorkloadResolver) parseDeploymentFromReplicaSet(rsName string) string {
	// Find the last hyphen followed by what looks like a hash
	lastDash := strings.LastIndex(rsName, "-")
	if lastDash == -1 {
		return rsName
	}

	suffix := rsName[lastDash+1:]

	// Pod template hash is typically 8-10 alphanumeric characters
	if len(suffix) >= 5 && len(suffix) <= 15 && isAlphanumeric(suffix) {
		return rsName[:lastDash]
	}

	return rsName
}

// podKey constructs a key for pod name lookup.
func (r *PodWorkloadResolver) podKey(namespace, name string) string {
	return namespace + "/" + name
}

// Stats returns resolver statistics.
func (r *PodWorkloadResolver) Stats() (podCount int, mappingCount int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.podNameToUID), len(r.podToWorkload)
}

// isAlphanumeric checks if a string contains only lowercase alphanumeric characters.
func isAlphanumeric(s string) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}
