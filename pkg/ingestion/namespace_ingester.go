package ingestion

import (
	"context"
	"fmt"
	"sync"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Helper function to convert string action to enum
func stringToAction(action string) v1.Action {
	switch action {
	case "CREATE":
		return v1.Action_ACTION_CREATE
	case "UPDATE":
		return v1.Action_ACTION_UPDATE
	case "DELETE":
		return v1.Action_ACTION_DELETE
	default:
		return v1.Action_ACTION_UNSPECIFIED
	}
}

type NamespaceIngester struct {
	clientset       kubernetes.Interface
	logger          *zap.Logger
	namespaceChan   chan *v1.Namespace
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
}

// NewNamespaceIngester creates a new namespace ingester using a shared informer factory
func NewNamespaceIngester(logger *zap.Logger, namespaceChan chan *v1.Namespace, clientset kubernetes.Interface, informerFactory informers.SharedInformerFactory) *NamespaceIngester {
	return &NamespaceIngester{
		clientset:       clientset,
		logger:          logger,
		namespaceChan:   namespaceChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}
}

// StartSync starts the namespace ingester and signals when initial sync is complete
func (ni *NamespaceIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	ni.logger.Info("Starting namespace ingester with modern informer factory")

	// Set up namespace informer
	ni.setupNamespaceInformer()

	// Send initial inventory before starting informers
	if err := ni.sendInitialNamespaceInventory(ctx); err != nil {
		ni.logger.Error("Failed to send initial namespace inventory", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Wait for context cancellation
	// Note: factory.Start() and WaitForCacheSync() are handled centrally by stream_client
	<-ctx.Done()
	ni.safeClose()
	ni.logger.Info("Stopped namespace ingester")
	return nil
}

// Stop stops the namespace ingester
func (ni *NamespaceIngester) Stop() {
	ni.safeClose()
}

// safeClose safely closes the stop channel only once
func (ni *NamespaceIngester) safeClose() {
	ni.mu.Lock()
	defer ni.mu.Unlock()
	if !ni.stopped {
		close(ni.stopCh)
		ni.stopped = true
	}
}

// setupNamespaceInformer sets up the modern namespace informer
func (ni *NamespaceIngester) setupNamespaceInformer() {
	namespaceInformer := ni.informerFactory.Core().V1().Namespaces().Informer()

	_, err := namespaceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if namespace, ok := obj.(*corev1.Namespace); ok {
				ni.sendNamespace(namespace.ObjectMeta, "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if namespace, ok := newObj.(*corev1.Namespace); ok {
				ni.sendNamespace(namespace.ObjectMeta, "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if namespace, ok := obj.(*corev1.Namespace); ok {
				ni.sendNamespace(namespace.ObjectMeta, "DELETE")
			}
		},
	})
	if err != nil {
		ni.logger.Error("Failed to add namespace event handler", zap.Error(err))
	}
}

// sendInitialNamespaceInventory sends all existing namespaces to the server
func (ni *NamespaceIngester) sendInitialNamespaceInventory(ctx context.Context) error {
	ni.logger.Info("Sending initial namespace inventory using direct API calls")

	namespaces, err := ni.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, namespace := range namespaces.Items {
		ni.sendNamespace(namespace.ObjectMeta, "CREATE")
	}

	ni.logger.Info("Completed sending initial namespace inventory")
	return nil
}

// sendNamespace sends a namespace event to the stream
func (ni *NamespaceIngester) sendNamespace(meta metav1.ObjectMeta, action string) {
	namespace := &v1.Namespace{
		Name:      meta.Name,
		Uid:       string(meta.UID),
		Labels:    meta.Labels,
		CreatedAt: timestamppb.New(meta.CreationTimestamp.Time),
		Action:    stringToAction(action),
	}

	select {
	case ni.namespaceChan <- namespace:
		ni.logger.Debug("Sent namespace event",
			zap.String("name", namespace.Name),
			zap.String("action", namespace.Action.String()))
	default:
		ni.logger.Warn("Namespace channel full, dropping event",
			zap.String("name", namespace.Name),
			zap.String("action", namespace.Action.String()))
	}
}
