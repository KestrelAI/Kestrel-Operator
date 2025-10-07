package s2_streaming

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitializeLogStreaming initializes S2 log streaming if configured
// Returns nil if S2 is not configured (graceful degradation)
func InitializeLogStreaming(ctx context.Context, logger *zap.Logger) (*LogManager, error) {
	logManager, err := NewLogManager(ctx, logger)
	if err != nil {
		logger.Error("Failed to initialize S2 log streaming", zap.Error(err))
		return nil, err
	}

	if logManager.IsEnabled() {
		logger.Info("S2 log streaming initialized successfully")
	} else {
		logger.Info("S2 log streaming not enabled")
	}

	return logManager, nil
}

// InitializeLogStreamingWithLogger initializes S2 log streaming and returns a wrapped logger
// that will stream all logs at debug level or above to S2
func InitializeLogStreamingWithLogger(ctx context.Context, logger *zap.Logger) (*LogManager, *zap.Logger, error) {
	logManager, err := NewLogManager(ctx, logger)
	if err != nil {
		logger.Error("Failed to initialize S2 log streaming", zap.Error(err))
		return nil, logger, err
	}

	if !logManager.IsEnabled() {
		logger.Info("S2 log streaming not enabled")
		return logManager, logger, nil
	}

	logger.Info("S2 log streaming initialized successfully")

	// Wrap the logger to also stream to S2 at debug level
	wrappedLogger := WrapZapLogger(logger, logManager, zapcore.DebugLevel)
	wrappedLogger.Info("Wrapped logger now streaming to S2")

	return logManager, wrappedLogger, nil
}

// WrapZapLogger wraps a zap.Logger to also stream logs to S2
func WrapZapLogger(logger *zap.Logger, logManager *LogManager, minLevel zapcore.Level) *zap.Logger {
	if logManager == nil || !logManager.IsEnabled() {
		return logger
	}

	// Get the core from the logger
	core := logger.Core()

	// Wrap it with S2 core
	s2Core := NewS2Core(logManager, minLevel)
	wrappedCore := zapcore.NewTee(core, s2Core)

	// Return new logger with wrapped core
	return zap.New(wrappedCore)
}

// LogOperatorEvent is a convenience function to log operator events to S2
func LogOperatorEvent(logManager *LogManager, level LogLevel, component, message string, fields map[string]interface{}) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	switch level {
	case LogLevelDebug:
		logManager.LogDebug(component, message, fields)
	case LogLevelInfo:
		logManager.LogInfo(component, message, fields)
	case LogLevelWarn:
		logManager.LogWarn(component, message, fields)
	case LogLevelError:
		logManager.LogError(component, message, fields)
	}
}

// Example helper functions for common operator events

// LogFlowEvent logs a network flow event
func LogFlowEvent(logManager *LogManager, srcIP, dstIP string, port uint32, protocol string, allowed bool) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("flow_collector", "Network flow observed", map[string]interface{}{
		"src_ip":   srcIP,
		"dst_ip":   dstIP,
		"port":     port,
		"protocol": protocol,
		"allowed":  allowed,
	})
}

// LogPolicyEvent logs a network policy event
func LogPolicyEvent(logManager *LogManager, action, policyName, namespace, message string) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("policy_manager", message, map[string]interface{}{
		"action":      action,
		"policy_name": policyName,
		"namespace":   namespace,
	})
}

// LogWorkloadEvent logs a workload event
func LogWorkloadEvent(logManager *LogManager, action, workloadName, namespace, kind string) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("workload_ingester", "Workload event", map[string]interface{}{
		"action":    action,
		"workload":  workloadName,
		"namespace": namespace,
		"kind":      kind,
	})
}

// LogL7AccessLog logs an L7 access log event
func LogL7AccessLog(logManager *LogManager, srcName, dstName, method, path string, responseCode uint32) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("l7_collector", "L7 access log", map[string]interface{}{
		"src_name":      srcName,
		"dst_name":      dstName,
		"method":        method,
		"path":          path,
		"response_code": responseCode,
	})
}

// LogStreamHealth logs stream health status
func LogStreamHealth(logManager *LogManager, healthy bool, errorMessage string) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	level := LogLevelInfo
	if !healthy {
		level = LogLevelWarn
	}

	fields := map[string]interface{}{
		"healthy": healthy,
	}
	if errorMessage != "" {
		fields["error"] = errorMessage
	}

	switch level {
	case LogLevelWarn:
		logManager.LogWarn("stream_health", "Stream health status", fields)
	default:
		logManager.LogInfo("stream_health", "Stream health status", fields)
	}
}

// LogOperatorStartup logs operator startup event
func LogOperatorStartup(logManager *LogManager, version string, features map[string]bool) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("operator", "Operator startup", map[string]interface{}{
		"version":  version,
		"features": features,
	})
}

// LogOperatorShutdown logs operator shutdown event
func LogOperatorShutdown(logManager *LogManager, reason string) {
	if logManager == nil || !logManager.IsEnabled() {
		return
	}

	logManager.LogInfo("operator", "Operator shutdown", map[string]interface{}{
		"reason": reason,
	})
}
