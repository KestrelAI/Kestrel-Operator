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

type PodIngester struct {
	clientset       kubernetes.Interface
	logger          *zap.Logger
	podChan         chan *v1.Pod
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
}

// NewPodIngester creates a new pod ingester using a shared informer factory
func NewPodIngester(logger *zap.Logger, podChan chan *v1.Pod, clientset kubernetes.Interface, informerFactory informers.SharedInformerFactory) *PodIngester {
	return &PodIngester{
		clientset:       clientset,
		logger:          logger,
		podChan:         podChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}
}

// StartSync starts the pod ingester and signals when initial sync is complete
func (pi *PodIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	pi.logger.Info("Starting pod ingester for streaming to server")

	// Set up pod informer
	pi.setupPodInformer()

	// Send initial inventory before starting informers
	if err := pi.sendInitialPodInventory(ctx); err != nil {
		pi.logger.Error("Failed to send initial pod inventory", zap.Error(err))
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
	pi.safeClose()
	pi.logger.Info("Stopped pod ingester")
	return nil
}

// Stop stops the pod ingester
func (pi *PodIngester) Stop() {
	pi.safeClose()
}

// safeClose safely closes the stop channel only once
func (pi *PodIngester) safeClose() {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	if !pi.stopped {
		close(pi.stopCh)
		pi.stopped = true
	}
}

// setupPodInformer sets up the pod informer to track all pods
func (pi *PodIngester) setupPodInformer() {
	podInformer := pi.informerFactory.Core().V1().Pods().Informer()

	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				pi.sendPod(pod, "CREATE")
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				pi.sendPod(pod, "UPDATE")
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				pi.sendPod(pod, "DELETE")
			}
		},
	})
	if err != nil {
		pi.logger.Error("Failed to add pod event handler", zap.Error(err))
	}
}

// sendPod converts a Kubernetes pod to protobuf and sends it to the stream
func (pi *PodIngester) sendPod(pod *corev1.Pod, action string) {
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

	protoPod := &v1.Pod{
		Name:            pod.Name,
		Namespace:       pod.Namespace,
		Uid:             string(pod.UID),
		PodIp:           pod.Status.PodIP,
		Labels:          pod.Labels,
		Phase:           string(pod.Status.Phase),
		OwnerReferences: protoOwnerRefs,
		CreatedAt:       timestamppb.New(pod.CreationTimestamp.Time),
		Action:          stringToAction(action),
	}

	select {
	case pi.podChan <- protoPod:
		pi.logger.Debug("Sent pod event",
			zap.String("name", protoPod.Name),
			zap.String("namespace", protoPod.Namespace),
			zap.String("phase", protoPod.Phase),
			zap.String("podIP", protoPod.PodIp),
			zap.String("action", protoPod.Action.String()))
	default:
		pi.logger.Warn("Pod channel full, dropping event",
			zap.String("name", protoPod.Name),
			zap.String("namespace", protoPod.Namespace),
			zap.String("action", protoPod.Action.String()))
	}
}

// sendInitialPodInventory sends all existing pods to the server
func (pi *PodIngester) sendInitialPodInventory(ctx context.Context) error {
	pi.logger.Info("Sending initial pod inventory using direct API calls")

	// Get all existing pods using direct API calls
	pods, err := pi.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range pods.Items {
		pi.sendPod(&pod, "CREATE")
	}

	pi.logger.Info("Completed sending initial pod inventory", zap.Int("count", len(pods.Items)))
	return nil
}
