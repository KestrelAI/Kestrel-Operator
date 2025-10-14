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

type NodeIngester struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	nodeChan        chan *v1.Node
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
}

// NewNodeIngester creates a new node ingester for streaming nodes to the server
func NewNodeIngester(logger *zap.Logger, nodeChan chan *v1.Node) (*NodeIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory
	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	return &NodeIngester{
		clientset:       clientset,
		logger:          logger,
		nodeChan:        nodeChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}, nil
}

// StartSync starts the node ingester and signals when initial sync is complete
func (ni *NodeIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	ni.logger.Info("Starting node ingester for streaming to server")

	// Set up node informer
	ni.setupNodeInformer()

	// Send initial inventory before starting informers
	if err := ni.sendInitialNodeInventory(ctx); err != nil {
		ni.logger.Error("Failed to send initial node inventory", zap.Error(err))
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
	ni.informerFactory.Start(ni.stopCh)

	// Wait for all caches to sync before processing events
	ni.logger.Info("Waiting for node informer cache to sync...")
	if !cache.WaitForCacheSync(ni.stopCh,
		ni.informerFactory.Core().V1().Nodes().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for node informer cache to sync")
	}
	ni.logger.Info("Node informer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	ni.safeClose()
	ni.logger.Info("Stopped node ingester")
	return nil
}

// Stop stops the node ingester
func (ni *NodeIngester) Stop() {
	ni.safeClose()
}

// safeClose safely closes the stop channel only once
func (ni *NodeIngester) safeClose() {
	ni.mu.Lock()
	defer ni.mu.Unlock()
	if !ni.stopped {
		close(ni.stopCh)
		ni.stopped = true
	}
}

// setupNodeInformer sets up the node informer to track all nodes
func (ni *NodeIngester) setupNodeInformer() {
	nodeInformer := ni.informerFactory.Core().V1().Nodes().Informer()

	_, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				ni.sendNode(node, "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if node, ok := newObj.(*corev1.Node); ok {
				ni.sendNode(node, "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if node, ok := obj.(*corev1.Node); ok {
				ni.sendNode(node, "DELETE")
			}
		},
	})
	if err != nil {
		ni.logger.Error("Failed to add node event handler", zap.Error(err))
	}
}

// sendNode converts a Kubernetes node to protobuf and sends it to the stream
func (ni *NodeIngester) sendNode(node *corev1.Node, action string) {
	// Extract node IPs (both internal and external)
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

	// Extract node conditions for ready status
	var ready bool
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			ready = condition.Status == corev1.ConditionTrue
			break
		}
	}

	// Extract instance type and zone from labels (common on EKS and other managed K8s)
	instanceType := node.Labels["node.kubernetes.io/instance-type"]
	zone := node.Labels["topology.kubernetes.io/zone"]

	protoNode := &v1.Node{
		Name:         node.Name,
		Uid:          string(node.UID),
		Labels:       node.Labels,
		InternalIps:  internalIPs,
		ExternalIps:  externalIPs,
		Hostname:     hostname,
		Ready:        ready,
		InstanceType: instanceType,
		Zone:         zone,
		CreatedAt:    timestamppb.New(node.CreationTimestamp.Time),
		Action:       stringToAction(action),
	}

	select {
	case ni.nodeChan <- protoNode:
		ni.logger.Debug("Sent node event",
			zap.String("name", protoNode.Name),
			zap.Strings("internalIPs", protoNode.InternalIps),
			zap.Bool("ready", protoNode.Ready),
			zap.String("action", protoNode.Action.String()))
	default:
		ni.logger.Warn("Node channel full, dropping event",
			zap.String("name", protoNode.Name),
			zap.String("action", protoNode.Action.String()))
	}
}

// sendInitialNodeInventory sends all existing nodes to the server
func (ni *NodeIngester) sendInitialNodeInventory(ctx context.Context) error {
	ni.logger.Info("Sending initial node inventory using direct API calls")

	// Get all existing nodes using direct API calls
	nodes, err := ni.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		ni.sendNode(&node, "CREATE")
	}

	ni.logger.Info("Completed sending initial node inventory", zap.Int("count", len(nodes.Items)))
	return nil
}
