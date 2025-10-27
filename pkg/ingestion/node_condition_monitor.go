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

// NodeConditionMonitor monitors node condition changes for incident detection
type NodeConditionMonitor struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	conditionChan   chan *v1.NodeConditionChange
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex

	// Track previous node conditions to detect transitions
	previousConditions map[string]*nodeConditionSnapshot
	conditionsMu       sync.RWMutex
}

// nodeConditionSnapshot captures the relevant conditions of a node for comparison
type nodeConditionSnapshot struct {
	ready            bool
	memoryPressure   bool
	diskPressure     bool
	pidPressure      bool
	networkAvailable bool
	conditions       map[string]corev1.ConditionStatus // condition type -> status
}

// NewNodeConditionMonitor creates a new node condition monitor for incident detection
func NewNodeConditionMonitor(logger *zap.Logger, conditionChan chan *v1.NodeConditionChange) (*NodeConditionMonitor, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory with shorter resync for incident detection
	informerFactory := informers.NewSharedInformerFactory(clientset, 10*time.Second)

	return &NodeConditionMonitor{
		clientset:          clientset,
		logger:             logger,
		conditionChan:      conditionChan,
		informerFactory:    informerFactory,
		stopCh:             make(chan struct{}),
		previousConditions: make(map[string]*nodeConditionSnapshot),
	}, nil
}

// StartSync starts the node condition monitor and signals when initial sync is complete
func (ncm *NodeConditionMonitor) StartSync(ctx context.Context, syncDone chan<- error) error {
	ncm.logger.Info("Starting node condition monitor for incident detection")

	// Set up node informer
	ncm.setupNodeInformer()

	// Send initial inventory (nodes in problematic states)
	if err := ncm.sendInitialNodeConditionInventory(ctx); err != nil {
		ncm.logger.Error("Failed to send initial node condition inventory", zap.Error(err))
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
	ncm.informerFactory.Start(ncm.stopCh)

	// Wait for all caches to sync before processing events
	ncm.logger.Info("Waiting for node condition informer cache to sync...")
	if !cache.WaitForCacheSync(ncm.stopCh,
		ncm.informerFactory.Core().V1().Nodes().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for node condition informer cache to sync")
	}
	ncm.logger.Info("Node condition informer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	ncm.safeClose()
	ncm.logger.Info("Stopped node condition monitor")
	return nil
}

// Stop stops the node condition monitor
func (ncm *NodeConditionMonitor) Stop() {
	ncm.safeClose()
}

// safeClose safely closes the stop channel only once
func (ncm *NodeConditionMonitor) safeClose() {
	ncm.mu.Lock()
	defer ncm.mu.Unlock()
	if !ncm.stopped {
		close(ncm.stopCh)
		ncm.stopped = true
	}
}

// setupNodeInformer sets up the node informer to track condition changes
func (ncm *NodeConditionMonitor) setupNodeInformer() {
	nodeInformer := ncm.informerFactory.Core().V1().Nodes().Informer()

	_, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				// Check if node has problematic conditions
				if ncm.isNodeProblematic(node) {
					ncm.sendNodeCondition(node, "CREATE")
				}
				// Store initial state
				ncm.updateNodeConditionSnapshot(node)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if node, ok := newObj.(*corev1.Node); ok {
				// Check for significant condition changes
				if ncm.hasSignificantConditionChange(node) {
					ncm.sendNodeCondition(node, "UPDATE")
				}
				// Update snapshot
				ncm.updateNodeConditionSnapshot(node)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				// Always send node deletions (critical incident signal)
				ncm.sendNodeCondition(node, "DELETE")
				// Clean up snapshot
				ncm.removeNodeConditionSnapshot(node)
			}
		},
	})
	if err != nil {
		ncm.logger.Error("Failed to add node condition event handler", zap.Error(err))
	}
}

// isNodeProblematic checks if a node has problematic conditions
func (ncm *NodeConditionMonitor) isNodeProblematic(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		switch condition.Type {
		case corev1.NodeReady:
			// Node not ready is a critical incident
			if condition.Status != corev1.ConditionTrue {
				return true
			}
		case corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure:
			// Any pressure condition is problematic
			if condition.Status == corev1.ConditionTrue {
				return true
			}
		case corev1.NodeNetworkUnavailable:
			// Network unavailable is critical
			if condition.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

// hasSignificantConditionChange checks if node conditions changed meaningfully
func (ncm *NodeConditionMonitor) hasSignificantConditionChange(node *corev1.Node) bool {
	ncm.conditionsMu.RLock()
	previousSnapshot, exists := ncm.previousConditions[node.Name]
	ncm.conditionsMu.RUnlock()

	if !exists {
		// First time seeing this node
		return ncm.isNodeProblematic(node)
	}

	// Get current condition snapshot
	currentSnapshot := ncm.buildNodeConditionSnapshot(node)

	// Check for Ready status change
	if currentSnapshot.ready != previousSnapshot.ready {
		return true
	}

	// Check for pressure condition changes
	if currentSnapshot.memoryPressure != previousSnapshot.memoryPressure {
		return true
	}
	if currentSnapshot.diskPressure != previousSnapshot.diskPressure {
		return true
	}
	if currentSnapshot.pidPressure != previousSnapshot.pidPressure {
		return true
	}
	if currentSnapshot.networkAvailable != previousSnapshot.networkAvailable {
		return true
	}

	return false
}

// buildNodeConditionSnapshot builds a snapshot of node conditions
func (ncm *NodeConditionMonitor) buildNodeConditionSnapshot(node *corev1.Node) *nodeConditionSnapshot {
	snapshot := &nodeConditionSnapshot{
		conditions: make(map[string]corev1.ConditionStatus),
	}

	for _, condition := range node.Status.Conditions {
		snapshot.conditions[string(condition.Type)] = condition.Status

		switch condition.Type {
		case corev1.NodeReady:
			snapshot.ready = condition.Status == corev1.ConditionTrue
		case corev1.NodeMemoryPressure:
			snapshot.memoryPressure = condition.Status == corev1.ConditionTrue
		case corev1.NodeDiskPressure:
			snapshot.diskPressure = condition.Status == corev1.ConditionTrue
		case corev1.NodePIDPressure:
			snapshot.pidPressure = condition.Status == corev1.ConditionTrue
		case corev1.NodeNetworkUnavailable:
			snapshot.networkAvailable = condition.Status != corev1.ConditionTrue
		}
	}

	return snapshot
}

// updateNodeConditionSnapshot updates the stored condition snapshot for a node
func (ncm *NodeConditionMonitor) updateNodeConditionSnapshot(node *corev1.Node) {
	snapshot := ncm.buildNodeConditionSnapshot(node)

	ncm.conditionsMu.Lock()
	ncm.previousConditions[node.Name] = snapshot
	ncm.conditionsMu.Unlock()
}

// removeNodeConditionSnapshot removes the condition snapshot for a deleted node
func (ncm *NodeConditionMonitor) removeNodeConditionSnapshot(node *corev1.Node) {
	ncm.conditionsMu.Lock()
	delete(ncm.previousConditions, node.Name)
	ncm.conditionsMu.Unlock()
}

// sendNodeCondition converts a Node to protobuf NodeConditionChange and sends it to the stream
func (ncm *NodeConditionMonitor) sendNodeCondition(node *corev1.Node, action string) {
	// Extract node IPs and hostname
	var internalIPs []string
	var externalIPs []string
	var hostname string

	for _, addr := range node.Status.Addresses {
		switch addr.Type {
		case corev1.NodeInternalIP:
			internalIPs = append(internalIPs, addr.Address)
		case corev1.NodeExternalIP:
			externalIPs = append(externalIPs, addr.Address)
		case corev1.NodeHostName:
			hostname = addr.Address
		}
	}

	// Convert node conditions
	var protoConditions []*v1.NodeCondition
	for _, condition := range node.Status.Conditions {
		protoCondition := &v1.NodeCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  condition.Reason,
			Message: condition.Message,
		}
		if !condition.LastHeartbeatTime.IsZero() {
			protoCondition.LastHeartbeatTime = timestamppb.New(condition.LastHeartbeatTime.Time)
		}
		if !condition.LastTransitionTime.IsZero() {
			protoCondition.LastTransitionTime = timestamppb.New(condition.LastTransitionTime.Time)
		}
		protoConditions = append(protoConditions, protoCondition)
	}

	// Extract instance type and zone from labels
	instanceType := node.Labels["node.kubernetes.io/instance-type"]
	zone := node.Labels["topology.kubernetes.io/zone"]

	// Convert capacity and allocatable resources
	capacity := make(map[string]string)
	for resourceName, quantity := range node.Status.Capacity {
		capacity[string(resourceName)] = quantity.String()
	}

	allocatable := make(map[string]string)
	for resourceName, quantity := range node.Status.Allocatable {
		allocatable[string(resourceName)] = quantity.String()
	}

	protoNodeCondition := &v1.NodeConditionChange{
		Name:               node.Name,
		Uid:                string(node.UID),
		Conditions:         protoConditions,
		InternalIps:        internalIPs,
		ExternalIps:        externalIPs,
		Hostname:           hostname,
		InstanceType:       instanceType,
		Zone:               zone,
		Capacity:           capacity,
		Allocatable:        allocatable,
		CreatedAt:          timestamppb.New(node.CreationTimestamp.Time),
		ConditionTimestamp: timestamppb.Now(),
		Action:             stringToAction(action),
	}

	select {
	case ncm.conditionChan <- protoNodeCondition:
		ncm.logger.Info("Sent node condition change",
			zap.String("name", protoNodeCondition.Name),
			zap.Int("conditions", len(protoNodeCondition.Conditions)),
			zap.String("action", protoNodeCondition.Action.String()))
	default:
		ncm.logger.Warn("Node condition change channel full, dropping event",
			zap.String("name", protoNodeCondition.Name),
			zap.String("action", protoNodeCondition.Action.String()))
	}
}

// sendInitialNodeConditionInventory sends nodes with problematic conditions
func (ncm *NodeConditionMonitor) sendInitialNodeConditionInventory(ctx context.Context) error {
	ncm.logger.Info("Sending initial node condition inventory (problematic nodes only)")

	nodes, err := ncm.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	problematicCount := 0
	for _, node := range nodes.Items {
		if ncm.isNodeProblematic(&node) {
			ncm.sendNodeCondition(&node, "CREATE")
			problematicCount++
		}
		// Store initial state for all nodes
		ncm.updateNodeConditionSnapshot(&node)
	}

	ncm.logger.Info("Completed sending initial node condition inventory",
		zap.Int("total_nodes", len(nodes.Items)),
		zap.Int("problematic_nodes", problematicCount))
	return nil
}
