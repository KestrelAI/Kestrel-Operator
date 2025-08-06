package ingestion

import (
	"context"
	"fmt"
	"time"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type ServiceIngester struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	serviceChan     chan *v1.Service
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
}

// NewServiceIngester creates a new service ingester using modern informer factory
func NewServiceIngester(logger *zap.Logger, serviceChan chan *v1.Service) (*ServiceIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory - this is the modern way
	// It automatically handles caching, reconnections, and resource management
	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	return &ServiceIngester{
		clientset:       clientset,
		logger:          logger,
		serviceChan:     serviceChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}, nil
}

// StartSync starts the service ingester and signals when initial sync is complete
func (si *ServiceIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	si.logger.Info("Starting service ingester with modern informer factory")

	// Set up informer for services
	si.setupServiceInformer()

	// Send initial inventory before starting informers
	if err := si.sendInitialServiceInventory(ctx); err != nil {
		si.logger.Error("Failed to send initial service inventory", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers - this replaces the manual controller.Run calls
	si.informerFactory.Start(si.stopCh)

	// Wait for all caches to sync before processing events
	si.logger.Info("Waiting for service informer caches to sync...")
	if !cache.WaitForCacheSync(si.stopCh,
		si.informerFactory.Core().V1().Services().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for service informer caches to sync")
	}
	si.logger.Info("All service informer caches synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	close(si.stopCh)
	si.logger.Info("Stopped service ingester")
	return nil
}

// Stop stops the service ingester
func (si *ServiceIngester) Stop() {
	close(si.stopCh)
}

// setupServiceInformer sets up the service informer with event handlers
func (si *ServiceIngester) setupServiceInformer() {
	serviceInformer := si.informerFactory.Core().V1().Services().Informer()

	_, err := serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if service, ok := obj.(*corev1.Service); ok {
				si.sendService(service, "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if service, ok := newObj.(*corev1.Service); ok {
				si.sendService(service, "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if service, ok := obj.(*corev1.Service); ok {
				si.sendService(service, "DELETE")
			}
		},
	})
	if err != nil {
		si.logger.Error("Failed to add service event handler", zap.Error(err))
	}
}

// sendService sends a service event to the stream
func (si *ServiceIngester) sendService(service *corev1.Service, action string) {
	// Convert Kubernetes service ports to proto service ports
	protoPorts := make([]*v1.ServicePort, 0, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		protoPort := &v1.ServicePort{
			Name:     port.Name,
			Protocol: string(port.Protocol),
			Port:     port.Port,
		}

		// Handle target port (can be int or string)
		if port.TargetPort.Type == intstr.Int {
			protoPort.TargetPort = port.TargetPort.IntVal
		} else if port.TargetPort.Type == intstr.String {
			// For named ports, we'll store as 0 and use the name in annotations
			protoPort.TargetPort = 0
		}

		// NodePort is only set for NodePort and LoadBalancer services
		if port.NodePort != 0 {
			protoPort.NodePort = port.NodePort
		}

		protoPorts = append(protoPorts, protoPort)
	}

	// Handle external IPs
	externalIPs := service.Spec.ExternalIPs
	if externalIPs == nil {
		externalIPs = []string{}
	}

	protoService := &v1.Service{
		Name:        service.Name,
		Namespace:   service.Namespace,
		Uid:         string(service.UID),
		Labels:      service.Labels,
		Annotations: service.Annotations,
		Selector:    service.Spec.Selector,
		Ports:       protoPorts,
		ServiceType: string(service.Spec.Type),
		ClusterIp:   service.Spec.ClusterIP,
		ExternalIps: externalIPs,
		CreatedAt:   timestamppb.New(service.CreationTimestamp.Time),
		Action:      stringToAction(action),
	}

	select {
	case si.serviceChan <- protoService:
		si.logger.Debug("Sent service event",
			zap.String("name", protoService.Name),
			zap.String("namespace", protoService.Namespace),
			zap.String("type", protoService.ServiceType),
			zap.String("action", protoService.Action.String()))
	default:
		si.logger.Warn("Service channel full, dropping event",
			zap.String("name", protoService.Name),
			zap.String("namespace", protoService.Namespace),
			zap.String("type", protoService.ServiceType),
			zap.String("action", protoService.Action.String()))
	}
}

// sendInitialServiceInventory sends all existing services to the server
func (si *ServiceIngester) sendInitialServiceInventory(ctx context.Context) error {
	si.logger.Info("Sending initial service inventory using direct API calls")

	// Get all existing services using direct API calls (not cached)
	services, err := si.clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}

	for _, service := range services.Items {
		si.sendService(&service, "CREATE")
	}

	si.logger.Info("Completed sending initial service inventory", zap.Int("count", len(services.Items)))
	return nil
}
