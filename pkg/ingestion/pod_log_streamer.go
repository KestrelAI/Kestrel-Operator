package ingestion

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
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

// PodLogStreamer handles streaming pod logs from all pods in the cluster for incident investigation
type PodLogStreamer struct {
	clientset       *kubernetes.Clientset
	logger          *zap.Logger
	logChan         chan *v1.PodLogs
	informerFactory informers.SharedInformerFactory
	stopCh          chan struct{}
	stopped         bool
	mu              sync.Mutex

	// Track active log streaming goroutines
	activeStreams map[string]context.CancelFunc // pod key -> cancel function
	streamsMu     sync.RWMutex

	// Track which pods we're already streaming logs for
	streamingPods map[string]bool
	streamingMu   sync.RWMutex
}

// NewPodLogStreamer creates a new pod log streamer
func NewPodLogStreamer(logger *zap.Logger, logChan chan *v1.PodLogs) (*PodLogStreamer, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create shared informer factory
	informerFactory := informers.NewSharedInformerFactory(clientset, 30*time.Second)

	return &PodLogStreamer{
		clientset:       clientset,
		logger:          logger,
		logChan:         logChan,
		informerFactory: informerFactory,
		stopCh:          make(chan struct{}),
		activeStreams:   make(map[string]context.CancelFunc),
		streamingPods:   make(map[string]bool),
	}, nil
}

// StartSync starts the pod log streamer
func (pls *PodLogStreamer) StartSync(ctx context.Context, syncDone chan<- error) error {
	pls.logger.Info("Starting pod log streamer for incident investigation")

	// Set up pod informer to detect pods that need log streaming
	pls.setupPodInformer(ctx)

	// Send initial logs from all application pods
	if err := pls.streamInitialPodLogs(ctx); err != nil {
		pls.logger.Error("Failed to stream initial pod logs", zap.Error(err))
		if syncDone != nil {
			syncDone <- err
		}
		return err
	}

	// Signal that initial sync is complete
	if syncDone != nil {
		syncDone <- nil
	}

	// Start informer
	pls.informerFactory.Start(pls.stopCh)

	// Wait for cache sync
	pls.logger.Info("Waiting for pod log streamer cache to sync...")
	if !cache.WaitForCacheSync(pls.stopCh,
		pls.informerFactory.Core().V1().Pods().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to wait for pod log streamer cache to sync")
	}
	pls.logger.Info("Pod log streamer cache synced successfully")

	// Wait for context cancellation
	<-ctx.Done()
	pls.safeClose()
	pls.logger.Info("Stopped pod log streamer")
	return nil
}

// Stop stops the pod log streamer
func (pls *PodLogStreamer) Stop() {
	pls.safeClose()
}

// safeClose safely closes the stop channel and cancels all active log streams
func (pls *PodLogStreamer) safeClose() {
	pls.mu.Lock()
	defer pls.mu.Unlock()
	if !pls.stopped {
		// Cancel all active log streams
		pls.streamsMu.Lock()
		for podKey, cancel := range pls.activeStreams {
			pls.logger.Debug("Cancelling log stream for pod", zap.String("pod", podKey))
			cancel()
		}
		pls.streamsMu.Unlock()

		close(pls.stopCh)
		pls.stopped = true
	}
}

// setupPodInformer sets up pod informer to detect pods needing log streaming
func (pls *PodLogStreamer) setupPodInformer(ctx context.Context) {
	podInformer := pls.informerFactory.Core().V1().Pods().Informer()

	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				// Start streaming logs for problematic pods
				if pls.shouldStreamLogs(pod) {
					go pls.startPodLogStreaming(ctx, pod)
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

				// Check if we should start streaming
				if pls.shouldStreamLogs(pod) {
					pls.streamingMu.RLock()
					alreadyStreaming := pls.streamingPods[podKey]
					pls.streamingMu.RUnlock()

					if !alreadyStreaming {
						go pls.startPodLogStreaming(ctx, pod)
					}
				} else {
					// Pod is in excluded namespace or not running yet, stop streaming if we were
					pls.streamingMu.Lock()
					if pls.streamingPods[podKey] {
						pls.logger.Debug("Pod no longer needs log streaming, stopping",
							zap.String("pod", podKey))
						delete(pls.streamingPods, podKey)

						// Cancel the log stream
						pls.streamsMu.Lock()
						if cancel, exists := pls.activeStreams[podKey]; exists {
							cancel()
							delete(pls.activeStreams, podKey)
						}
						pls.streamsMu.Unlock()
					}
					pls.streamingMu.Unlock()
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

				// Stop streaming logs for deleted pod
				pls.streamingMu.Lock()
				delete(pls.streamingPods, podKey)
				pls.streamingMu.Unlock()

				pls.streamsMu.Lock()
				if cancel, exists := pls.activeStreams[podKey]; exists {
					cancel()
					delete(pls.activeStreams, podKey)
				}
				pls.streamsMu.Unlock()
			}
		},
	})
	if err != nil {
		pls.logger.Error("Failed to add pod log event handler", zap.Error(err))
	}
}

// shouldStreamLogs determines if we should stream logs for a pod
func (pls *PodLogStreamer) shouldStreamLogs(pod *corev1.Pod) bool {
	// Stream logs for ALL application pods for comprehensive incident investigation
	// This enables the AI agent to:
	// 1. Build complete timeline of events (including logs before incident)
	// 2. Correlate healthy pods with problematic pods (e.g., database pod logs when app pod crashes)
	// 3. Detect patterns across multiple pods (e.g., all pods failing to connect to service)
	// 4. Provide context for root cause analysis (what was happening before crash)

	// Skip system/infrastructure pods to reduce noise
	namespace := pod.Namespace
	if namespace == "kube-system" || namespace == "kube-public" || namespace == "kube-node-lease" {
		return false
	}

	// Skip pods that are not yet running (no logs to stream)
	if pod.Status.Phase == corev1.PodPending {
		// Wait until at least one container has started
		hasStarted := false
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Running != nil || cs.State.Terminated != nil {
				hasStarted = true
				break
			}
		}
		if !hasStarted {
			return false
		}
	}

	// Skip completed/succeeded pods (unless they have errors)
	if pod.Status.Phase == corev1.PodSucceeded {
		return false
	}

	// Stream logs for all other application pods (Running, Failed, Unknown)
	return true
}

// streamInitialPodLogs streams logs from all application pods
func (pls *PodLogStreamer) streamInitialPodLogs(ctx context.Context) error {
	pls.logger.Info("Streaming initial logs from all application pods (excluding system namespaces)")

	pods, err := pls.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	streamingCount := 0
	for _, pod := range pods.Items {
		if pls.shouldStreamLogs(&pod) {
			// Start streaming logs for this pod in background
			go pls.startPodLogStreaming(ctx, &pod)
			streamingCount++
		}
	}

	pls.logger.Info("Started streaming logs from application pods",
		zap.Int("total_pods", len(pods.Items)),
		zap.Int("streaming_pods", streamingCount),
		zap.String("strategy", "all_application_pods"))
	return nil
}

// startPodLogStreaming starts streaming logs for a specific pod
func (pls *PodLogStreamer) startPodLogStreaming(parentCtx context.Context, pod *corev1.Pod) {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	// Mark as streaming
	pls.streamingMu.Lock()
	if pls.streamingPods[podKey] {
		// Already streaming this pod
		pls.streamingMu.Unlock()
		return
	}
	pls.streamingPods[podKey] = true
	pls.streamingMu.Unlock()

	// Create cancellable context for this pod's log streaming
	ctx, cancel := context.WithCancel(parentCtx)

	// Store cancel function
	pls.streamsMu.Lock()
	pls.activeStreams[podKey] = cancel
	pls.streamsMu.Unlock()

	// Ensure cleanup
	defer func() {
		pls.streamsMu.Lock()
		delete(pls.activeStreams, podKey)
		pls.streamsMu.Unlock()

		pls.streamingMu.Lock()
		delete(pls.streamingPods, podKey)
		pls.streamingMu.Unlock()
	}()

	pls.logger.Info("Starting log streaming for pod",
		zap.String("pod", podKey),
		zap.String("phase", string(pod.Status.Phase)))

	// Stream logs for each container
	for _, container := range pod.Spec.Containers {
		// Check if container has issues
		containerStatus := pls.getContainerStatus(pod, container.Name)
		if containerStatus == nil {
			continue
		}

		// Stream current container logs
		go pls.streamContainerLogs(ctx, pod, container.Name, false)

		// If container was terminated (crashed), also fetch previous container logs
		if containerStatus.State.Terminated != nil || containerStatus.RestartCount > 0 {
			go pls.streamContainerLogs(ctx, pod, container.Name, true)
		}
	}

	// Keep this function alive until context is cancelled
	<-ctx.Done()
	pls.logger.Debug("Stopped log streaming for pod", zap.String("pod", podKey))
}

// streamContainerLogs streams logs from a specific container
func (pls *PodLogStreamer) streamContainerLogs(ctx context.Context, pod *corev1.Pod, containerName string, previous bool) {
	podKey := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, containerName)
	logType := "current"
	if previous {
		logType = "previous"
	}

	pls.logger.Info("Streaming container logs",
		zap.String("pod", podKey),
		zap.String("logType", logType))

	// Fetch logs from Kubernetes API
	logOptions := &corev1.PodLogOptions{
		Container:  containerName,
		Follow:     !previous, // Only follow current container logs, not previous
		Timestamps: true,      // Include timestamps for correlation
		Previous:   previous,
	}

	// For problematic pods, get last 500 lines of historical context
	if !previous {
		tailLines := int64(500)
		logOptions.TailLines = &tailLines
	}

	// Get log stream from Kubernetes API
	logStream, err := pls.clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, logOptions).Stream(ctx)
	if err != nil {
		pls.logger.Warn("Failed to get log stream",
			zap.String("pod", podKey),
			zap.String("logType", logType),
			zap.Error(err))
		return
	}
	defer logStream.Close()

	// Read logs line by line
	scanner := bufio.NewScanner(logStream)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 1MB max line size

	var logEntries []*v1.LogEntry
	var lineNumber int64 = 0
	batchSize := 100 // Send logs in batches of 100 lines

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			// Context cancelled, stop streaming
			return
		default:
			lineNumber++
			logLine := scanner.Text()

			// Parse log entry
			logEntry := pls.parseLogLine(logLine, lineNumber)
			logEntries = append(logEntries, logEntry)

			// Send batch when we reach batch size
			if len(logEntries) >= batchSize {
				pls.sendLogBatch(ctx, pod, containerName, logEntries, previous, lineNumber, false)
				logEntries = []*v1.LogEntry{} // Reset batch
			}
		}
	}

	// Send remaining logs
	if len(logEntries) > 0 {
		pls.sendLogBatch(ctx, pod, containerName, logEntries, previous, lineNumber, true)
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil && err != io.EOF {
		pls.logger.Warn("Error reading log stream",
			zap.String("pod", podKey),
			zap.String("logType", logType),
			zap.Error(err))
	}

	pls.logger.Debug("Finished streaming container logs",
		zap.String("pod", podKey),
		zap.String("logType", logType),
		zap.Int64("totalLines", lineNumber))
}

// parseLogLine parses a log line and extracts metadata
func (pls *PodLogStreamer) parseLogLine(logLine string, lineNumber int64) *v1.LogEntry {
	entry := &v1.LogEntry{
		LineNumber: lineNumber,
		Message:    logLine,
	}

	// Try to extract timestamp (format: 2024-01-15T10:30:45.123456789Z)
	// Kubernetes log timestamps are at the start of the line
	timestampRegex := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s+(.*)`)
	if matches := timestampRegex.FindStringSubmatch(logLine); len(matches) == 3 {
		timestampStr := matches[1]
		actualMessage := matches[2]

		// Parse timestamp
		if ts, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
			entry.Timestamp = timestamppb.New(ts)
			entry.Message = actualMessage // Remove timestamp from message
		}
	}

	// Detect log level (common patterns)
	entry.Level = pls.detectLogLevel(entry.Message)

	// Parse error information if this is an error log
	if entry.Level == "ERROR" || entry.Level == "FATAL" || entry.Level == "CRITICAL" {
		entry.ErrorInfo = pls.parseErrorInfo(entry.Message)
	}

	return entry
}

// detectLogLevel attempts to detect the log level from the message
func (pls *PodLogStreamer) detectLogLevel(message string) string {
	messageLower := strings.ToLower(message)

	// Common log level patterns
	if strings.Contains(messageLower, "error") || strings.Contains(messageLower, "err:") ||
		strings.Contains(messageLower, "exception") || strings.Contains(messageLower, "fatal") {
		return "ERROR"
	}
	if strings.Contains(messageLower, "warn") || strings.Contains(messageLower, "warning") {
		return "WARN"
	}
	if strings.Contains(messageLower, "debug") || strings.Contains(messageLower, "trace") {
		return "DEBUG"
	}

	// Check for structured logging formats (JSON, logfmt)
	// Look for level field indicators
	levelPatterns := []struct {
		pattern string
		level   string
	}{
		{`"level"\s*:\s*"(error|fatal|critical)"`, "ERROR"},
		{`"level"\s*:\s*"(warn|warning)"`, "WARN"},
		{`"level"\s*:\s*"info"`, "INFO"},
		{`"level"\s*:\s*"debug"`, "DEBUG"},
		{`level=(error|fatal|critical)`, "ERROR"},
		{`level=(warn|warning)`, "WARN"},
		{`level=info`, "INFO"},
		{`level=debug`, "DEBUG"},
	}

	for _, p := range levelPatterns {
		if matched, _ := regexp.MatchString(p.pattern, messageLower); matched {
			return p.level
		}
	}

	return "INFO" // Default level
}

// parseErrorInfo parses error information from an error log message
func (pls *PodLogStreamer) parseErrorInfo(message string) *v1.LogErrorInfo {
	errorInfo := &v1.LogErrorInfo{}

	// Detect common error types
	errorPatterns := []struct {
		pattern   string
		errorType string
	}{
		{`OutOfMemoryException|OOM|out of memory`, "OutOfMemoryException"},
		{`NullPointerException|null pointer|nil pointer`, "NullPointerException"},
		{`SegmentationFault|segfault|SIGSEGV`, "SegmentationFault"},
		{`panic:`, "Panic"},
		{`AssertionError|assertion failed`, "AssertionError"},
		{`TimeoutException|timeout|timed out`, "TimeoutException"},
		{`ConnectionException|connection refused|connection reset`, "ConnectionException"},
		{`FileNotFoundException|no such file`, "FileNotFoundException"},
		{`PermissionDenied|permission denied|access denied`, "PermissionDenied"},
		{`RuntimeError|runtime error`, "RuntimeError"},
	}

	for _, p := range errorPatterns {
		if matched, _ := regexp.MatchString(`(?i)`+p.pattern, message); matched {
			errorInfo.ErrorType = p.errorType
			break
		}
	}

	// Extract error message (first line of multi-line errors)
	lines := strings.Split(message, "\n")
	if len(lines) > 0 {
		errorInfo.ErrorMessage = lines[0]
	}

	// Check for stack trace indicators
	if strings.Contains(message, "at ") || strings.Contains(message, "stack trace") ||
		strings.Contains(message, "goroutine") || strings.Contains(message, "traceback") {
		// Extract stack trace (limited to first 10 lines to avoid huge messages)
		stackLines := lines
		if len(stackLines) > 10 {
			stackLines = stackLines[:10]
		}
		errorInfo.StackTrace = strings.Join(stackLines, "\n")
	}

	// Try to extract exit code
	exitCodeRegex := regexp.MustCompile(`exit code (\d+)|exited with code (\d+)`)
	if matches := exitCodeRegex.FindStringSubmatch(message); len(matches) > 1 {
		for i := 1; i < len(matches); i++ {
			if matches[i] != "" {
				var exitCode int32
				fmt.Sscanf(matches[i], "%d", &exitCode)
				errorInfo.ExitCode = exitCode
				break
			}
		}
	}

	return errorInfo
}

// sendLogBatch sends a batch of log entries to the server
func (pls *PodLogStreamer) sendLogBatch(ctx context.Context, pod *corev1.Pod, containerName string, logEntries []*v1.LogEntry, previous bool, streamOffset int64, streamComplete bool) {
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

	podLogs := &v1.PodLogs{
		PodName:             pod.Name,
		Namespace:           pod.Namespace,
		PodUid:              string(pod.UID),
		ContainerName:       containerName,
		LogEntries:          logEntries,
		PreviousContainer:   previous,
		CollectionTimestamp: timestamppb.Now(),
		TotalLines:          int64(len(logEntries)),
		StreamComplete:      streamComplete,
		StreamOffset:        streamOffset,
		OwnerReferences:     protoOwnerRefs,
		NodeName:            pod.Spec.NodeName,
	}

	select {
	case pls.logChan <- podLogs:
		pls.logger.Debug("Sent pod logs batch",
			zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
			zap.String("container", containerName),
			zap.Int("lines", len(logEntries)),
			zap.Bool("previous", previous),
			zap.Bool("complete", streamComplete))
	case <-ctx.Done():
		return
	default:
		pls.logger.Warn("Log channel full, dropping log batch",
			zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
			zap.String("container", containerName),
			zap.Int("lines", len(logEntries)))
	}
}

// getContainerStatus retrieves container status for a specific container
func (pls *PodLogStreamer) getContainerStatus(pod *corev1.Pod, containerName string) *corev1.ContainerStatus {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName {
			return &cs
		}
	}
	return nil
}
