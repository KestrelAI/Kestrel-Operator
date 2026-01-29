package metrics_store

import (
	"testing"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

func TestPodWorkloadResolver_ReplicaSet(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod owned by a ReplicaSet (which is owned by a Deployment)
	pod := &v1.Pod{
		Name:      "checkout-service-7f9b4c8d5-x2k9j",
		Namespace: "prod",
		Uid:       "pod-uid-1",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "ReplicaSet",
				Name:       "checkout-service-7f9b4c8d5",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	// Resolve workload
	info, ok := resolver.ResolveWorkload("prod", "checkout-service-7f9b4c8d5-x2k9j")
	if !ok {
		t.Fatal("Expected to resolve workload")
	}

	if info.Kind != "Deployment" {
		t.Errorf("Expected kind 'Deployment', got '%s'", info.Kind)
	}
	if info.Name != "checkout-service" {
		t.Errorf("Expected name 'checkout-service', got '%s'", info.Name)
	}
	if info.Namespace != "prod" {
		t.Errorf("Expected namespace 'prod', got '%s'", info.Namespace)
	}
}

func TestPodWorkloadResolver_StatefulSet(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod owned by a StatefulSet
	pod := &v1.Pod{
		Name:      "postgres-0",
		Namespace: "database",
		Uid:       "pod-uid-2",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "StatefulSet",
				Name:       "postgres",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	info, ok := resolver.ResolveWorkload("database", "postgres-0")
	if !ok {
		t.Fatal("Expected to resolve workload")
	}

	if info.Kind != "StatefulSet" {
		t.Errorf("Expected kind 'StatefulSet', got '%s'", info.Kind)
	}
	if info.Name != "postgres" {
		t.Errorf("Expected name 'postgres', got '%s'", info.Name)
	}
}

func TestPodWorkloadResolver_DaemonSet(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod owned by a DaemonSet
	pod := &v1.Pod{
		Name:      "fluentd-abc123",
		Namespace: "logging",
		Uid:       "pod-uid-3",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "DaemonSet",
				Name:       "fluentd",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	info, ok := resolver.ResolveWorkload("logging", "fluentd-abc123")
	if !ok {
		t.Fatal("Expected to resolve workload")
	}

	if info.Kind != "DaemonSet" {
		t.Errorf("Expected kind 'DaemonSet', got '%s'", info.Kind)
	}
	if info.Name != "fluentd" {
		t.Errorf("Expected name 'fluentd', got '%s'", info.Name)
	}
}

func TestPodWorkloadResolver_Job(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod owned by a Job
	pod := &v1.Pod{
		Name:      "backup-job-xyz123",
		Namespace: "jobs",
		Uid:       "pod-uid-4",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "Job",
				Name:       "backup-job",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	info, ok := resolver.ResolveWorkload("jobs", "backup-job-xyz123")
	if !ok {
		t.Fatal("Expected to resolve workload")
	}

	if info.Kind != "Job" {
		t.Errorf("Expected kind 'Job', got '%s'", info.Kind)
	}
	if info.Name != "backup-job" {
		t.Errorf("Expected name 'backup-job', got '%s'", info.Name)
	}
}

func TestPodWorkloadResolver_Unregister(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	pod := &v1.Pod{
		Name:      "test-pod",
		Namespace: "default",
		Uid:       "pod-uid-5",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "ReplicaSet",
				Name:       "test-rs-abc123",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	// Verify it's registered
	_, ok := resolver.ResolveWorkload("default", "test-pod")
	if !ok {
		t.Fatal("Expected pod to be registered")
	}

	// Unregister
	resolver.UnregisterPod("default", "test-pod", "pod-uid-5")

	// Verify it's gone
	_, ok = resolver.ResolveWorkload("default", "test-pod")
	if ok {
		t.Error("Expected pod to be unregistered")
	}
}

func TestPodWorkloadResolver_NoController(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	// Register a pod without a controller owner reference
	pod := &v1.Pod{
		Name:      "standalone-pod",
		Namespace: "default",
		Uid:       "pod-uid-6",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "ReplicaSet",
				Name:       "some-rs",
				Controller: false, // Not a controller
			},
		},
	}
	resolver.RegisterPod(pod)

	// Should not resolve because there's no controller
	_, ok := resolver.ResolveWorkload("default", "standalone-pod")
	if ok {
		t.Error("Expected pod without controller to not be resolvable")
	}
}

func TestPodWorkloadResolver_Stats(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	podCount, mappingCount := resolver.Stats()
	if podCount != 0 || mappingCount != 0 {
		t.Error("Expected empty stats initially")
	}

	// Register a pod
	pod := &v1.Pod{
		Name:      "test-pod",
		Namespace: "default",
		Uid:       "pod-uid-7",
		OwnerReferences: []*v1.OwnerReference{
			{
				Kind:       "ReplicaSet",
				Name:       "test-rs-abc123",
				Controller: true,
			},
		},
	}
	resolver.RegisterPod(pod)

	podCount, mappingCount = resolver.Stats()
	if podCount != 1 || mappingCount != 1 {
		t.Errorf("Expected (1, 1) stats, got (%d, %d)", podCount, mappingCount)
	}
}

func TestParseDeploymentFromReplicaSet(t *testing.T) {
	logger := zap.NewNop()
	resolver := NewPodWorkloadResolver(logger)

	testCases := []struct {
		rsName       string
		expectedName string
	}{
		{"web-server-7f9b4c8d5", "web-server"},
		{"checkout-service-abc123xyz", "checkout-service"},
		{"simple-name-12345", "simple-name"},
		{"name-with-dashes-in-it-abcd1234", "name-with-dashes-in-it"},
		{"no-hash", "no-hash"}, // No valid hash suffix
		{"short", "short"},     // Too short to have a hash
	}

	for _, tc := range testCases {
		result := resolver.parseDeploymentFromReplicaSet(tc.rsName)
		if result != tc.expectedName {
			t.Errorf("parseDeploymentFromReplicaSet(%q) = %q, want %q", tc.rsName, result, tc.expectedName)
		}
	}
}
