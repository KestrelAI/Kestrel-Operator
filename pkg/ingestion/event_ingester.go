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
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// EventIngester handles the ingestion of Kubernetes Events for incident detection
type EventIngester struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	eventChan       chan *v1.KubernetesEvent
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex
}

// NewEventIngester creates a new event ingester for streaming events to the server
func NewEventIngester(logger *zap.Logger, eventChan chan *v1.KubernetesEvent) (*EventIngester, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory with shorter resync period for events (events are time-sensitive)
	informerFactory := informers.NewSharedInformerFactory(clientset, 10*time.Second)

	return &EventIngester{
		clientset:       clientset,
		logger:          logger,
		eventChan:       eventChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
	}, nil
}

// StartSync starts the event ingester and signals when initial sync is complete
func (ei *EventIngester) StartSync(ctx context.Context, syncDone chan<- error) error {
	ei.logger.Info("Starting Kubernetes event ingester for incident detection")

	// Set up event informer before starting
	// The informer's AddFunc will be called for all existing events during cache sync
	ei.setupEventInformer()

	// Signal that setup is complete (informer will handle all events including existing ones)
	if syncDone != nil {
		syncDone <- nil
	}

	// Start all informers
	// During cache sync, AddFunc will be called for ALL existing Warning events
	ei.informerFactory.Start(ei.stopCh)

	// Wait for all caches to sync before processing events
	ei.logger.Info("Waiting for event informer cache to sync...")
	if !cache.WaitForCacheSync(ei.stopCh,
		ei.informerFactory.Core().V1().Events().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for event informer cache to sync")
	}
	ei.logger.Info("Event informer cache synced successfully - all existing events processed via AddFunc")

	// Wait for context cancellation
	<-ctx.Done()
	ei.safeClose()
	ei.logger.Info("Stopped event ingester")
	return nil
}

// Stop stops the event ingester
func (ei *EventIngester) Stop() {
	ei.safeClose()
}

// safeClose safely closes the stop channel only once
func (ei *EventIngester) safeClose() {
	ei.mu.Lock()
	defer ei.mu.Unlock()
	if !ei.stopped {
		close(ei.stopCh)
		ei.stopped = true
	}
}

// setupEventInformer sets up the event informer to track all events
func (ei *EventIngester) setupEventInformer() {
	eventInformer := ei.informerFactory.Core().V1().Events().Informer()

	_, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if event, ok := obj.(*corev1.Event); ok {
				// Send Warning events for incident detection
				if event.Type == corev1.EventTypeWarning {
					ei.sendEvent(event, "CREATE")
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if event, ok := newObj.(*corev1.Event); ok {
				// Send updates for Warning events (count increases indicate repeated issues)
				if event.Type == corev1.EventTypeWarning {
					ei.sendEvent(event, "UPDATE")
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if event, ok := obj.(*corev1.Event); ok {
				// Track deletion of warning events
				if event.Type == corev1.EventTypeWarning {
					ei.sendEvent(event, "DELETE")
				}
			}
		},
	})
	if err != nil {
		ei.logger.Error("Failed to add event event handler", zap.Error(err))
	}
}

// sendEvent converts a Kubernetes Event to protobuf and sends it to the stream
func (ei *EventIngester) sendEvent(event *corev1.Event, action string) {
	// Convert involved object reference
	protoInvolvedObject := &v1.ObjectReference{
		Kind:            event.InvolvedObject.Kind,
		Namespace:       event.InvolvedObject.Namespace,
		Name:            event.InvolvedObject.Name,
		Uid:             string(event.InvolvedObject.UID),
		ApiVersion:      event.InvolvedObject.APIVersion,
		ResourceVersion: event.InvolvedObject.ResourceVersion,
		FieldPath:       event.InvolvedObject.FieldPath,
	}

	// Convert event source
	protoSource := &v1.EventSource{
		Component: event.Source.Component,
		Host:      event.Source.Host,
	}

	// Convert timestamps
	var firstTimestamp, lastTimestamp *timestamppb.Timestamp
	if !event.FirstTimestamp.IsZero() {
		firstTimestamp = timestamppb.New(event.FirstTimestamp.Time)
	}
	if !event.LastTimestamp.IsZero() {
		lastTimestamp = timestamppb.New(event.LastTimestamp.Time)
	}

	protoEvent := &v1.KubernetesEvent{
		Name:           event.Name,
		Namespace:      event.Namespace,
		Uid:            string(event.UID),
		EventType:      event.Type,
		Reason:         event.Reason,
		Message:        event.Message,
		InvolvedObject: protoInvolvedObject,
		Source:         protoSource,
		FirstTimestamp: firstTimestamp,
		LastTimestamp:  lastTimestamp,
		Count:          event.Count,
		Action:         stringToAction(action),
	}

	select {
	case ei.eventChan <- protoEvent:
		ei.logger.Debug("Sent Kubernetes event",
			zap.String("name", protoEvent.Name),
			zap.String("namespace", protoEvent.Namespace),
			zap.String("type", protoEvent.EventType),
			zap.String("reason", protoEvent.Reason),
			zap.String("involvedObject", fmt.Sprintf("%s/%s", protoEvent.InvolvedObject.Kind, protoEvent.InvolvedObject.Name)),
			zap.Int32("count", protoEvent.Count),
			zap.String("action", protoEvent.Action.String()))
	default:
		ei.logger.Warn("Event channel full, dropping event",
			zap.String("name", protoEvent.Name),
			zap.String("namespace", protoEvent.Namespace),
			zap.String("reason", protoEvent.Reason),
			zap.String("action", protoEvent.Action.String()))
	}
}
