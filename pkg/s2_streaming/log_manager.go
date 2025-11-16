package s2_streaming

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// LogManager manages the S2 log streaming infrastructure
type LogManager struct {
	logger      *zap.Logger
	s2Client    *S2StreamClient
	logStreamer *LogStreamer
	enabled     bool
	mu          sync.RWMutex
}

// NewLogManager creates a new log manager
func NewLogManager(ctx context.Context, logger *zap.Logger) (*LogManager, error) {
	// Load S2 configuration from environment
	s2Config, err := LoadS2ConfigFromEnv(logger)
	if err != nil {
		logger.Info("S2 streaming not enabled, skipping initialization", zap.Error(err))
		return &LogManager{
			logger:  logger,
			enabled: false,
		}, nil
	}

	// Create S2 client
	s2Client, err := NewS2StreamClient(ctx, logger, *s2Config)
	if err != nil {
		logger.Error("Failed to create S2 client", zap.Error(err))
		return nil, fmt.Errorf("failed to create S2 client: %w", err)
	}

	// Create log streamer
	streamerConfig := LogStreamerConfig{
		BufferSize: 10000,
		BatchSize:  100,
		LingerTime: 0, // Use default (5 seconds)
	}

	logStreamer, err := NewLogStreamer(ctx, logger, s2Client, streamerConfig)
	if err != nil {
		s2Client.Close()
		logger.Error("Failed to create log streamer", zap.Error(err))
		return nil, fmt.Errorf("failed to create log streamer: %w", err)
	}

	// Start the log streamer
	if err := logStreamer.Start(); err != nil {
		s2Client.Close()
		logger.Error("Failed to start log streamer", zap.Error(err))
		return nil, fmt.Errorf("failed to start log streamer: %w", err)
	}

	logger.Info("Log manager initialized successfully with S2 streaming enabled")

	return &LogManager{
		logger:      logger,
		s2Client:    s2Client,
		logStreamer: logStreamer,
		enabled:     true,
	}, nil
}

// IsEnabled returns whether S2 log streaming is enabled
func (lm *LogManager) IsEnabled() bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.enabled
}

// LogDebug streams a debug log entry
func (lm *LogManager) LogDebug(component, message string, fields map[string]interface{}) {
	lm.streamLog(LogLevelDebug, component, message, fields)
}

// LogInfo streams an info log entry
func (lm *LogManager) LogInfo(component, message string, fields map[string]interface{}) {
	lm.streamLog(LogLevelInfo, component, message, fields)
}

// LogWarn streams a warning log entry
func (lm *LogManager) LogWarn(component, message string, fields map[string]interface{}) {
	lm.streamLog(LogLevelWarn, component, message, fields)
}

// LogError streams an error log entry
func (lm *LogManager) LogError(component, message string, fields map[string]interface{}) {
	lm.streamLog(LogLevelError, component, message, fields)
}

// streamLog is a helper method to stream logs
func (lm *LogManager) streamLog(level LogLevel, component, message string, fields map[string]interface{}) {
	if !lm.IsEnabled() {
		return
	}

	entry := LogEntry{
		Level:     level,
		Message:   message,
		Component: component,
		Fields:    fields,
	}

	// Use non-blocking send to avoid impacting operator performance
	if err := lm.logStreamer.StreamLog(entry); err != nil {
		lm.logger.Debug("Failed to stream log to S2",
			zap.Error(err),
			zap.String("component", component),
			zap.String("message", message))
	}
}

// StreamCustomLog allows streaming a fully custom log entry
func (lm *LogManager) StreamCustomLog(entry LogEntry) error {
	if !lm.IsEnabled() {
		return fmt.Errorf("S2 log streaming is not enabled")
	}

	return lm.logStreamer.StreamLog(entry)
}

// GetStats returns statistics about the log streaming
func (lm *LogManager) GetStats() map[string]interface{} {
	if !lm.IsEnabled() {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	stats := lm.logStreamer.GetStats()
	stats["enabled"] = true
	return stats
}

// Close gracefully shuts down the log manager
func (lm *LogManager) Close() error {
	if !lm.IsEnabled() {
		return nil
	}

	lm.logger.Info("Closing log manager")

	lm.mu.Lock()
	lm.enabled = false
	lm.mu.Unlock()

	// Stop the log streamer
	if lm.logStreamer != nil {
		if err := lm.logStreamer.Stop(); err != nil {
			lm.logger.Error("Failed to stop log streamer", zap.Error(err))
		}
	}

	// Close the S2 client
	if lm.s2Client != nil {
		if err := lm.s2Client.Close(); err != nil {
			lm.logger.Error("Failed to close S2 client", zap.Error(err))
		}
	}

	lm.logger.Info("Log manager closed successfully")
	return nil
}
