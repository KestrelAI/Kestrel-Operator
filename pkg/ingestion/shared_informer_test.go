package ingestion

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// TestSharedInformerMultipleHandlers_Pods verifies that when multiple ingesters
// register handlers on the same Pod informer (from a shared factory), all handlers
// receive events. This is the core behavioral guarantee of our shared factory refactor.
//
// In production, 4 ingesters watch Pods: WorkloadIngester, PodIngester,
// PodStatusMonitor, and PodLogStreamer.
func TestSharedInformerMultipleHandlers_Pods(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0) // 0 resync = no resync
	logger := zap.NewNop()

	// Create channels for each ingester
	workloadChan := make(chan *v1.Workload, 100)
	podChan := make(chan *v1.Pod, 100)
	podStatusChan := make(chan *v1.PodStatusChange, 100)

	// Create ingesters that share the same factory
	workloadIngester := NewWorkloadIngester(logger, workloadChan, clientset, factory)
	podIngester := NewPodIngester(logger, podChan, clientset, factory)
	podStatusMonitor := NewPodStatusMonitor(logger, podStatusChan, clientset, factory)

	// Register all handlers (this is what happens in StartSync before factory.Start)
	workloadIngester.setupPodInformer()
	podIngester.setupPodInformer()
	podStatusMonitor.setupPodInformer()

	// Start the shared factory
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a pod via the fake clientset — this should trigger all 3 handlers
	testPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       types.UID("test-pod-uid"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.1",
		},
	}

	_, err := clientset.CoreV1().Pods("default").Create(ctx, testPod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Wait for events to propagate (informer loop needs a moment)
	// PodIngester should always receive the event
	select {
	case pod := <-podChan:
		assert.Equal(t, "test-pod", pod.Name)
		assert.Equal(t, "default", pod.Namespace)
		assert.Equal(t, v1.Action_ACTION_CREATE, pod.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("PodIngester did not receive pod create event")
	}

	// WorkloadIngester only sends standalone pods (no owner refs) — this pod has none,
	// so it should also appear on workloadChan
	select {
	case workload := <-workloadChan:
		assert.Equal(t, "test-pod", workload.Name)
		assert.Equal(t, "Pod", workload.Kind)
		assert.Equal(t, v1.Action_ACTION_CREATE, workload.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("WorkloadIngester did not receive standalone pod create event")
	}

	// PodStatusMonitor only sends problematic pods — a Running pod is NOT problematic,
	// so podStatusChan should be empty
	select {
	case status := <-podStatusChan:
		t.Fatalf("PodStatusMonitor should NOT have sent event for healthy pod, got: %s/%s", status.Namespace, status.Name)
	case <-time.After(500 * time.Millisecond):
		// Expected — healthy pod should not trigger status event
	}
}

// TestSharedInformerMultipleHandlers_ProblematicPod verifies that PodStatusMonitor
// correctly detects problematic pods when sharing the same factory.
func TestSharedInformerMultipleHandlers_ProblematicPod(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	podChan := make(chan *v1.Pod, 100)
	podStatusChan := make(chan *v1.PodStatusChange, 100)

	podIngester := NewPodIngester(logger, podChan, clientset, factory)
	podStatusMonitor := NewPodStatusMonitor(logger, podStatusChan, clientset, factory)

	podIngester.setupPodInformer()
	podStatusMonitor.setupPodInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a pod in CrashLoopBackOff — should be caught by PodStatusMonitor
	crashingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "crashing-pod",
			Namespace: "default",
			UID:       types.UID("crashing-pod-uid"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.2",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "CrashLoopBackOff",
							Message: "Back-off 5m0s restarting failed container",
						},
					},
					RestartCount: 10,
				},
			},
		},
	}

	_, err := clientset.CoreV1().Pods("default").Create(ctx, crashingPod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Both handlers should fire
	select {
	case pod := <-podChan:
		assert.Equal(t, "crashing-pod", pod.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("PodIngester did not receive event for crashing pod")
	}

	select {
	case status := <-podStatusChan:
		assert.Equal(t, "crashing-pod", status.Name)
		assert.Equal(t, v1.Action_ACTION_CREATE, status.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("PodStatusMonitor did not receive event for crashing pod")
	}
}

// TestSharedInformerMultipleHandlers_Nodes verifies that NodeIngester and
// NodeConditionMonitor both receive events from a shared Node informer.
func TestSharedInformerMultipleHandlers_Nodes(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	nodeChan := make(chan *v1.Node, 100)
	nodeConditionChan := make(chan *v1.NodeConditionChange, 100)

	nodeIngester := NewNodeIngester(logger, nodeChan, clientset, factory)
	nodeConditionMonitor := NewNodeConditionMonitor(logger, nodeConditionChan, clientset, factory)

	nodeIngester.setupNodeInformer()
	nodeConditionMonitor.setupNodeInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a healthy node — NodeIngester should see it, NodeConditionMonitor should NOT
	// (only sends on problematic conditions)
	healthyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "healthy-node",
			UID:  types.UID("healthy-node-uid"),
			Labels: map[string]string{
				"node.kubernetes.io/instance-type": "m5.large",
				"topology.kubernetes.io/zone":      "us-east-1a",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.1.1"},
			},
		},
	}

	_, err := clientset.CoreV1().Nodes().Create(ctx, healthyNode, metav1.CreateOptions{})
	require.NoError(t, err)

	// NodeIngester sends ALL nodes
	select {
	case node := <-nodeChan:
		assert.Equal(t, "healthy-node", node.Name)
		assert.True(t, node.Ready)
		assert.Equal(t, "m5.large", node.InstanceType)
	case <-time.After(5 * time.Second):
		t.Fatal("NodeIngester did not receive healthy node event")
	}

	// NodeConditionMonitor should NOT send for a healthy node
	select {
	case nc := <-nodeConditionChan:
		t.Fatalf("NodeConditionMonitor should NOT have sent event for healthy node, got: %s", nc.Name)
	case <-time.After(500 * time.Millisecond):
		// Expected
	}
}

// TestSharedInformerMultipleHandlers_ProblematicNode verifies that
// NodeConditionMonitor detects problematic nodes via the shared informer.
func TestSharedInformerMultipleHandlers_ProblematicNode(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	nodeChan := make(chan *v1.Node, 100)
	nodeConditionChan := make(chan *v1.NodeConditionChange, 100)

	nodeIngester := NewNodeIngester(logger, nodeChan, clientset, factory)
	nodeConditionMonitor := NewNodeConditionMonitor(logger, nodeConditionChan, clientset, factory)

	nodeIngester.setupNodeInformer()
	nodeConditionMonitor.setupNodeInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a node with NotReady condition — both should fire
	notReadyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "not-ready-node",
			UID:  types.UID("not-ready-node-uid"),
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionFalse,
					Reason: "KubeletNotReady",
				},
			},
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.1.2"},
			},
		},
	}

	_, err := clientset.CoreV1().Nodes().Create(ctx, notReadyNode, metav1.CreateOptions{})
	require.NoError(t, err)

	select {
	case node := <-nodeChan:
		assert.Equal(t, "not-ready-node", node.Name)
		assert.False(t, node.Ready)
	case <-time.After(5 * time.Second):
		t.Fatal("NodeIngester did not receive not-ready node event")
	}

	select {
	case nc := <-nodeConditionChan:
		assert.Equal(t, "not-ready-node", nc.Name)
		assert.Equal(t, v1.Action_ACTION_CREATE, nc.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("NodeConditionMonitor did not receive not-ready node event")
	}
}

// TestSharedInformerMultipleHandlers_Deployments verifies that WorkloadIngester
// and WorkloadRolloutMonitor both receive Deployment events from the shared factory.
func TestSharedInformerMultipleHandlers_Deployments(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	workloadChan := make(chan *v1.Workload, 100)
	rolloutChan := make(chan *v1.WorkloadRolloutStatus, 100)

	workloadIngester := NewWorkloadIngester(logger, workloadChan, clientset, factory)
	rolloutMonitor := NewWorkloadRolloutMonitor(logger, rolloutChan, clientset, factory)

	workloadIngester.setupDeploymentInformer()
	rolloutMonitor.setupDeploymentInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	replicas := int32(3)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
			UID:       types.UID("test-deploy-uid"),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "test"},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "test-sa",
					Containers: []corev1.Container{
						{Name: "app", Image: "nginx:latest"},
					},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:     3,
			AvailableReplicas: 3,
			UpdatedReplicas:   3,
			Replicas:          3,
		},
	}

	_, err := clientset.AppsV1().Deployments("default").Create(ctx, deployment, metav1.CreateOptions{})
	require.NoError(t, err)

	// WorkloadIngester should receive the Deployment event
	select {
	case workload := <-workloadChan:
		assert.Equal(t, "test-deployment", workload.Name)
		assert.Equal(t, "Deployment", workload.Kind)
		assert.Equal(t, "test-sa", workload.ServiceAccount)
		assert.Equal(t, v1.Action_ACTION_CREATE, workload.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("WorkloadIngester did not receive deployment event")
	}

	// WorkloadRolloutMonitor should NOT send for a healthy deployment
	// (all replicas ready, no problematic conditions)
	select {
	case status := <-rolloutChan:
		t.Fatalf("WorkloadRolloutMonitor should NOT have sent for healthy deployment, got: %s", status.Name)
	case <-time.After(500 * time.Millisecond):
		// Expected
	}
}

// TestSharedInformerEventUpdate verifies that Update events are correctly
// delivered to multiple handlers on the shared informer.
func TestSharedInformerEventUpdate(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	podChan := make(chan *v1.Pod, 100)
	podStatusChan := make(chan *v1.PodStatusChange, 100)

	podIngester := NewPodIngester(logger, podChan, clientset, factory)
	podStatusMonitor := NewPodStatusMonitor(logger, podStatusChan, clientset, factory)

	podIngester.setupPodInformer()
	podStatusMonitor.setupPodInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a healthy pod first
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "update-test-pod",
			Namespace: "default",
			UID:       types.UID("update-test-uid"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.3",
		},
	}
	_, err := clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Drain the create event
	select {
	case <-podChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive initial create event")
	}

	// Update the pod to a CrashLoopBackOff state
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		PodIP: "10.0.0.3",
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name: "app",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "CrashLoopBackOff",
					},
				},
				RestartCount: 5,
			},
		},
	}
	_, err = clientset.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	// PodIngester should receive the UPDATE
	select {
	case p := <-podChan:
		assert.Equal(t, "update-test-pod", p.Name)
		assert.Equal(t, v1.Action_ACTION_UPDATE, p.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("PodIngester did not receive update event")
	}

	// PodStatusMonitor should now fire because the pod transitioned to problematic
	select {
	case status := <-podStatusChan:
		assert.Equal(t, "update-test-pod", status.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("PodStatusMonitor did not receive update event for newly problematic pod")
	}
}

// TestSharedInformerEventDelete verifies that Delete events are correctly
// delivered to multiple handlers on the shared informer.
func TestSharedInformerEventDelete(t *testing.T) {
	// Pre-populate the fake clientset with an existing pod
	existingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delete-test-pod",
			Namespace: "default",
			UID:       types.UID("delete-test-uid"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.4",
		},
	}

	clientset := fake.NewSimpleClientset(existingPod)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	podChan := make(chan *v1.Pod, 100)
	podIngester := NewPodIngester(logger, podChan, clientset, factory)
	podIngester.setupPodInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Drain the initial Add event from cache sync
	select {
	case <-podChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive initial add event from cache sync")
	}

	// Delete the pod
	err := clientset.CoreV1().Pods("default").Delete(ctx, "delete-test-pod", metav1.DeleteOptions{})
	require.NoError(t, err)

	// PodIngester should receive the DELETE
	select {
	case p := <-podChan:
		assert.Equal(t, "delete-test-pod", p.Name)
		assert.Equal(t, v1.Action_ACTION_DELETE, p.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("PodIngester did not receive delete event")
	}
}

// TestStringToAction verifies the action string-to-enum helper.
func TestStringToAction(t *testing.T) {
	tests := []struct {
		input    string
		expected v1.Action
	}{
		{"CREATE", v1.Action_ACTION_CREATE},
		{"UPDATE", v1.Action_ACTION_UPDATE},
		{"DELETE", v1.Action_ACTION_DELETE},
		{"UNKNOWN", v1.Action_ACTION_UNSPECIFIED},
		{"", v1.Action_ACTION_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, stringToAction(tt.input))
		})
	}
}

// TestNodeConditionMonitor_IsNodeProblematic verifies the condition detection logic.
func TestNodeConditionMonitor_IsNodeProblematic(t *testing.T) {
	ncm := &NodeConditionMonitor{
		logger:             zap.NewNop(),
		previousConditions: make(map[string]*nodeConditionSnapshot),
	}

	tests := []struct {
		name       string
		conditions []corev1.NodeCondition
		expected   bool
	}{
		{
			name: "healthy node",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
			expected: false,
		},
		{
			name: "not ready node",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
			},
			expected: true,
		},
		{
			name: "memory pressure",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionTrue},
			},
			expected: true,
		},
		{
			name: "disk pressure",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeDiskPressure, Status: corev1.ConditionTrue},
			},
			expected: true,
		},
		{
			name: "pid pressure",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodePIDPressure, Status: corev1.ConditionTrue},
			},
			expected: true,
		},
		{
			name: "network unavailable",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionTrue},
			},
			expected: true,
		},
		{
			name: "no pressure conditions",
			conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
				{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse},
				{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse},
				{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: tt.conditions,
				},
			}
			assert.Equal(t, tt.expected, ncm.isNodeProblematic(node))
		})
	}
}

// TestNodeConditionMonitor_HasSignificantConditionChange verifies that only
// meaningful condition transitions are detected.
func TestNodeConditionMonitor_HasSignificantConditionChange(t *testing.T) {
	ncm := &NodeConditionMonitor{
		logger:             zap.NewNop(),
		previousConditions: make(map[string]*nodeConditionSnapshot),
	}

	// Set up a healthy initial state
	healthyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			},
		},
	}
	ncm.updateNodeConditionSnapshot(healthyNode)

	// Same conditions — no change
	assert.False(t, ncm.hasSignificantConditionChange(healthyNode))

	// Node becomes NotReady — significant change
	notReadyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			},
		},
	}
	assert.True(t, ncm.hasSignificantConditionChange(notReadyNode))

	// Memory pressure starts — significant change
	memPressureNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionTrue},
			},
		},
	}
	assert.True(t, ncm.hasSignificantConditionChange(memPressureNode))

	// Unknown node (no previous snapshot) that is problematic
	unknownNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "new-node"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
			},
		},
	}
	assert.True(t, ncm.hasSignificantConditionChange(unknownNode))

	// Unknown node that is healthy
	unknownHealthyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "new-healthy-node"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
	assert.False(t, ncm.hasSignificantConditionChange(unknownHealthyNode))
}

// TestPodStatusMonitor_IsPodProblematic verifies the pod problem detection logic.
func TestPodStatusMonitor_IsPodProblematic(t *testing.T) {
	psm := &PodStatusMonitor{
		logger:         zap.NewNop(),
		previousStates: make(map[string]*podStateSnapshot),
	}

	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name: "healthy running pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: false,
		},
		{
			name: "failed pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodFailed},
			},
			expected: true,
		},
		{
			name: "unknown phase pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodUnknown},
			},
			expected: true,
		},
		{
			name: "CrashLoopBackOff container",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "app",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: "CrashLoopBackOff",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "ImagePullBackOff container",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "app",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: "ImagePullBackOff",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "OOMKilled container",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "app",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason:   "OOMKilled",
									ExitCode: 137,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "high restart count",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "app",
							RestartCount: 5,
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "low restart count (healthy)",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "app",
							RestartCount: 2,
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, psm.isPodProblematic(tt.pod))
		})
	}
}

// TestEventIngesterOnlyWarnings verifies that the EventIngester correctly
// filters to only Warning events.
func TestEventIngesterOnlyWarnings(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	eventChan := make(chan *v1.KubernetesEvent, 100)
	eventIngester := NewEventIngester(logger, eventChan, clientset, factory)
	eventIngester.setupEventInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Create a Normal event — should be ignored
	normalEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normal-event",
			Namespace: "default",
			UID:       types.UID("normal-event-uid"),
		},
		Type:    corev1.EventTypeNormal,
		Reason:  "Scheduled",
		Message: "Successfully assigned pod",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "test-pod",
			Namespace: "default",
		},
		Source: corev1.EventSource{Component: "scheduler"},
	}

	_, err := clientset.CoreV1().Events("default").Create(ctx, normalEvent, metav1.CreateOptions{})
	require.NoError(t, err)

	// Should NOT receive the normal event
	select {
	case ev := <-eventChan:
		t.Fatalf("EventIngester should NOT send Normal events, got: %s", ev.Name)
	case <-time.After(500 * time.Millisecond):
		// Expected
	}

	// Create a Warning event — should be sent
	warningEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warning-event",
			Namespace: "default",
			UID:       types.UID("warning-event-uid"),
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "FailedScheduling",
		Message: "0/3 nodes are available",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "unschedulable-pod",
			Namespace: "default",
		},
		Source: corev1.EventSource{Component: "scheduler"},
	}

	_, err = clientset.CoreV1().Events("default").Create(ctx, warningEvent, metav1.CreateOptions{})
	require.NoError(t, err)

	select {
	case ev := <-eventChan:
		assert.Equal(t, "warning-event", ev.Name)
		assert.Equal(t, "Warning", ev.EventType)
		assert.Equal(t, "FailedScheduling", ev.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("EventIngester did not receive Warning event")
	}
}

// TestWorkloadIngesterSkipsOwnedPods verifies that WorkloadIngester only
// sends standalone pods (no owner references) as workloads.
func TestWorkloadIngesterSkipsOwnedPods(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	workloadChan := make(chan *v1.Workload, 100)
	workloadIngester := NewWorkloadIngester(logger, workloadChan, clientset, factory)
	workloadIngester.setupPodInformer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	isController := true

	// Create a pod with an owner reference (owned by a ReplicaSet) — should be skipped
	ownedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owned-pod",
			Namespace: "default",
			UID:       types.UID("owned-pod-uid"),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "my-rs-abc123",
					UID:        types.UID("rs-uid"),
					Controller: &isController,
				},
			},
		},
		Spec: corev1.PodSpec{ServiceAccountName: "default"},
	}

	_, err := clientset.CoreV1().Pods("default").Create(ctx, ownedPod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Should NOT receive the owned pod
	select {
	case w := <-workloadChan:
		t.Fatalf("WorkloadIngester should NOT send owned pods as workloads, got: %s", w.Name)
	case <-time.After(500 * time.Millisecond):
		// Expected
	}

	// Create a standalone pod (no owner) — should be sent
	standalonePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-pod",
			Namespace: "default",
			UID:       types.UID("standalone-pod-uid"),
		},
		Spec: corev1.PodSpec{ServiceAccountName: "my-sa"},
	}

	_, err = clientset.CoreV1().Pods("default").Create(ctx, standalonePod, metav1.CreateOptions{})
	require.NoError(t, err)

	select {
	case w := <-workloadChan:
		assert.Equal(t, "standalone-pod", w.Name)
		assert.Equal(t, "Pod", w.Kind)
		assert.Equal(t, "my-sa", w.ServiceAccount)
	case <-time.After(5 * time.Second):
		t.Fatal("WorkloadIngester did not receive standalone pod event")
	}
}

// TestConcurrentHandlerRegistration verifies that registering many handlers
// on the same informer concurrently does not cause races.
func TestConcurrentHandlerRegistration(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(clientset, 0)
	logger := zap.NewNop()

	// Simulate what stream_client does: register handlers from multiple ingesters
	var wg sync.WaitGroup

	podChan := make(chan *v1.Pod, 100)
	workloadChan := make(chan *v1.Workload, 100)
	podStatusChan := make(chan *v1.PodStatusChange, 100)

	pi := NewPodIngester(logger, podChan, clientset, factory)
	wi := NewWorkloadIngester(logger, workloadChan, clientset, factory)
	psm := NewPodStatusMonitor(logger, podStatusChan, clientset, factory)

	// Register handlers concurrently (mirrors goroutine startup in stream_client)
	wg.Add(3)
	go func() { defer wg.Done(); pi.setupPodInformer() }()
	go func() { defer wg.Done(); wi.setupPodInformer() }()
	go func() { defer wg.Done(); psm.setupPodInformer() }()
	wg.Wait()

	// Start the factory after all handlers are registered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// Verify all handlers work by creating a pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "concurrent-test-pod",
			Namespace: "default",
			UID:       types.UID("concurrent-uid"),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	_, err := clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// All should receive the event
	select {
	case p := <-podChan:
		assert.Equal(t, "concurrent-test-pod", p.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("PodIngester did not receive event after concurrent registration")
	}

	select {
	case w := <-workloadChan:
		assert.Equal(t, "concurrent-test-pod", w.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("WorkloadIngester did not receive event after concurrent registration")
	}
}
