package s2_streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/s2-streamstore/s2-sdk-go/s2"
	"go.uber.org/zap"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
)

// LogEntry represents a structured log entry to be streamed
type LogEntry struct {
	// Timestamp when the log was generated (Unix milliseconds)
	Timestamp int64 `json:"timestamp"`
	// Log level (DEBUG, INFO, WARN, ERROR)
	Level LogLevel `json:"level"`
	// Log message
	Message string `json:"message"`
	// Source component that generated the log
	Component string `json:"component"`
	// Cluster ID
	ClusterID string `json:"cluster_id"`
	// Pod name
	PodName string `json:"pod_name"`
	// Namespace
	Namespace string `json:"namespace"`
	// Additional structured fields
	Fields map[string]interface{} `json:"fields,omitempty"`
}

// LogStreamer handles streaming of operator logs to S2
type LogStreamer struct {
	logger       *zap.Logger
	s2Client     *S2StreamClient
	logChan      chan LogEntry
	batchSize    uint
	lingerTime   time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	clusterID    string
	podName      string
	namespace    string
	shuttingDown bool
	mu           sync.RWMutex
}

// LogStreamerConfig holds configuration for the log streamer
type LogStreamerConfig struct {
	// Channel buffer size
	BufferSize uint
	// Maximum number of logs to batch before sending
	BatchSize uint
	// How long to wait before sending a partial batch
	LingerTime time.Duration
}

// NewLogStreamer creates a new log streamer
func NewLogStreamer(ctx context.Context, logger *zap.Logger, s2Client *S2StreamClient, config LogStreamerConfig) (*LogStreamer, error) {
	if config.BufferSize == 0 {
		config.BufferSize = 10000 // Default buffer size
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100 // Default batch size
	}
	if config.LingerTime == 0 {
		config.LingerTime = 5 * time.Second // Default linger time
	}

	// Get cluster/pod information from environment
	clusterID := os.Getenv("CLUSTER_ID")
	if clusterID == "" {
		clusterID = "unknown"
	}

	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		hostname, _ := os.Hostname()
		podName = hostname
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "kestrel-ai"
	}

	streamerCtx, cancel := context.WithCancel(ctx)

	streamer := &LogStreamer{
		logger:     logger,
		s2Client:   s2Client,
		logChan:    make(chan LogEntry, config.BufferSize),
		batchSize:  config.BatchSize,
		lingerTime: config.LingerTime,
		ctx:        streamerCtx,
		cancel:     cancel,
		clusterID:  clusterID,
		podName:    podName,
		namespace:  namespace,
	}

	logger.Info("Created log streamer",
		zap.String("cluster_id", clusterID),
		zap.String("pod_name", podName),
		zap.String("namespace", namespace),
		zap.Uint("buffer_size", config.BufferSize),
		zap.Uint("batch_size", config.BatchSize),
		zap.Duration("linger_time", config.LingerTime))

	return streamer, nil
}

// Start begins streaming logs to S2
func (ls *LogStreamer) Start() error {
	ls.logger.Info("Starting log streamer")

	// Start the append session
	sender, receiver, err := ls.s2Client.streamClient.AppendSession(ls.ctx)
	if err != nil {
		ls.logger.Error("Failed to start S2 append session", zap.Error(err))
		return fmt.Errorf("failed to start append session: %w", err)
	}

	// Create batching sender for efficient log streaming
	batchingSender, err := s2.NewAppendRecordBatchingSender(
		sender,
		s2.WithMaxBatchRecords(ls.batchSize),
		s2.WithLinger(ls.lingerTime),
	)
	if err != nil {
		ls.logger.Error("Failed to create batching sender", zap.Error(err))
		return fmt.Errorf("failed to create batching sender: %w", err)
	}

	// Start goroutine to handle sending logs
	ls.wg.Add(1)
	go ls.sendLogs(batchingSender)

	// Start goroutine to handle acknowledgments
	ls.wg.Add(1)
	go ls.handleAcknowledgments(receiver)

	ls.logger.Info("Log streamer started successfully")
	return nil
}

// sendLogs reads from the log channel and sends to S2
func (ls *LogStreamer) sendLogs(sender *s2.AppendRecordBatchingSender) {
	defer ls.wg.Done()
	defer sender.Close()

	ls.logger.Info("Starting log sender goroutine")

	for {
		select {
		case <-ls.ctx.Done():
			ls.logger.Info("Log sender shutting down due to context cancellation")
			return

		case logEntry, ok := <-ls.logChan:
			if !ok {
				ls.logger.Info("Log channel closed, shutting down sender")
				return
			}

			// Convert log entry to S2 append record
			record, err := ls.logEntryToAppendRecord(logEntry)
			if err != nil {
				ls.logger.Error("Failed to convert log entry to append record",
					zap.Error(err),
					zap.String("message", logEntry.Message))
				continue
			}

			// Send the record
			if err := sender.Send(record); err != nil {
				ls.logger.Error("Failed to send log record to S2",
					zap.Error(err),
					zap.String("message", logEntry.Message))
				// Don't return here - continue trying to send other logs
			}
		}
	}
}

// handleAcknowledgments processes acknowledgments from S2
func (ls *LogStreamer) handleAcknowledgments(receiver s2.Receiver[*s2.AppendOutput]) {
	defer ls.wg.Done()

	ls.logger.Info("Starting acknowledgment handler goroutine")

	for {
		select {
		case <-ls.ctx.Done():
			ls.logger.Info("Acknowledgment handler shutting down")
			return

		default:
			ack, err := receiver.Recv()
			if err != nil {
				ls.logger.Error("Error receiving acknowledgment from S2", zap.Error(err))
				return
			}

			ls.logger.Debug("Received acknowledgment from S2",
				zap.Uint64("start_seq_num", ack.StartSeqNum),
				zap.Uint64("end_seq_num", ack.EndSeqNum),
				zap.Uint64("next_seq_num", ack.NextSeqNum))
		}
	}
}

// logEntryToAppendRecord converts a LogEntry to an S2 AppendRecord
func (ls *LogStreamer) logEntryToAppendRecord(entry LogEntry) (s2.AppendRecord, error) {
	// Serialize the log entry to JSON
	body, err := json.Marshal(entry)
	if err != nil {
		return s2.AppendRecord{}, fmt.Errorf("failed to marshal log entry: %w", err)
	}

	// Create timestamp (S2 expects milliseconds since epoch)
	timestamp := uint64(entry.Timestamp)

	// Create headers for metadata (can be used for filtering/indexing)
	headers := []s2.Header{
		{Name: []byte("level"), Value: []byte(entry.Level)},
		{Name: []byte("component"), Value: []byte(entry.Component)},
		{Name: []byte("cluster_id"), Value: []byte(entry.ClusterID)},
		{Name: []byte("pod_name"), Value: []byte(entry.PodName)},
		{Name: []byte("namespace"), Value: []byte(entry.Namespace)},
	}

	return s2.AppendRecord{
		Timestamp: &timestamp,
		Headers:   headers,
		Body:      body,
	}, nil
}

// StreamLog sends a log entry to the stream (non-blocking)
func (ls *LogStreamer) StreamLog(entry LogEntry) error {
	ls.mu.RLock()
	shuttingDown := ls.shuttingDown
	ls.mu.RUnlock()

	if shuttingDown {
		return fmt.Errorf("log streamer is shutting down")
	}

	// Enrich with cluster/pod metadata if not provided
	if entry.ClusterID == "" {
		entry.ClusterID = ls.clusterID
	}
	if entry.PodName == "" {
		entry.PodName = ls.podName
	}
	if entry.Namespace == "" {
		entry.Namespace = ls.namespace
	}

	// Set timestamp if not provided
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	// Try to send to channel (non-blocking)
	select {
	case ls.logChan <- entry:
		return nil
	default:
		// Channel is full, drop the log and return error
		return fmt.Errorf("log channel is full, dropping log entry")
	}
}

// StreamLogBlocking sends a log entry to the stream (blocking)
func (ls *LogStreamer) StreamLogBlocking(entry LogEntry) error {
	ls.mu.RLock()
	shuttingDown := ls.shuttingDown
	ls.mu.RUnlock()

	if shuttingDown {
		return fmt.Errorf("log streamer is shutting down")
	}

	// Enrich with cluster/pod metadata if not provided
	if entry.ClusterID == "" {
		entry.ClusterID = ls.clusterID
	}
	if entry.PodName == "" {
		entry.PodName = ls.podName
	}
	if entry.Namespace == "" {
		entry.Namespace = ls.namespace
	}

	// Set timestamp if not provided
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	// Send to channel (blocking)
	select {
	case ls.logChan <- entry:
		return nil
	case <-ls.ctx.Done():
		return fmt.Errorf("context cancelled while sending log")
	}
}

// Stop gracefully stops the log streamer
func (ls *LogStreamer) Stop() error {
	ls.logger.Info("Stopping log streamer")

	ls.mu.Lock()
	ls.shuttingDown = true
	ls.mu.Unlock()

	// Close the log channel to signal no more logs will be sent
	close(ls.logChan)

	// Cancel the context to stop goroutines
	ls.cancel()

	// Wait for all goroutines to finish
	ls.wg.Wait()

	ls.logger.Info("Log streamer stopped successfully")
	return nil
}

// GetStats returns statistics about the log streamer
func (ls *LogStreamer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"buffer_size":    cap(ls.logChan),
		"queued_logs":    len(ls.logChan),
		"batch_size":     ls.batchSize,
		"linger_time_ms": ls.lingerTime.Milliseconds(),
		"cluster_id":     ls.clusterID,
		"pod_name":       ls.podName,
		"namespace":      ls.namespace,
	}
}
