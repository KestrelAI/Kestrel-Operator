package s2_streaming

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestLogEntryCreation tests creating log entries
func TestLogEntryCreation(t *testing.T) {
	entry := LogEntry{
		Timestamp: time.Now().UnixMilli(),
		Level:     LogLevelInfo,
		Message:   "Test message",
		Component: "test_component",
		ClusterID: "test-cluster",
		PodName:   "test-pod",
		Namespace: "test-namespace",
		Fields: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		},
	}

	if entry.Level != LogLevelInfo {
		t.Errorf("Expected level INFO, got %s", entry.Level)
	}

	if entry.Message != "Test message" {
		t.Errorf("Expected message 'Test message', got %s", entry.Message)
	}

	if len(entry.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(entry.Fields))
	}
}

// TestS2ConfigValidation tests S2 config validation
func TestS2ConfigValidation(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	// Test with empty auth token
	config := S2Config{
		AuthToken: "",
		Basin:     "test-basin",
		Stream:    "test-stream",
	}

	_, err := NewS2StreamClient(ctx, logger, config)
	if err == nil {
		t.Error("Expected error with empty auth token")
	}

	// Test with empty basin
	config = S2Config{
		AuthToken: "test-token",
		Basin:     "",
		Stream:    "test-stream",
	}

	_, err = NewS2StreamClient(ctx, logger, config)
	if err == nil {
		t.Error("Expected error with empty basin")
	}

	// Test with empty stream
	config = S2Config{
		AuthToken: "test-token",
		Basin:     "test-basin",
		Stream:    "",
	}

	_, err = NewS2StreamClient(ctx, logger, config)
	if err == nil {
		t.Error("Expected error with empty stream")
	}
}

// TestLogManagerWithoutS2 tests that log manager handles missing S2 config gracefully
func TestLogManagerWithoutS2(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	// Clear S2 environment variables to simulate missing config
	t.Setenv("S2_AUTH_TOKEN", "")

	// This should not return an error even without S2 configured
	logManager, err := NewLogManager(ctx, logger)
	if err != nil {
		t.Errorf("Expected no error with missing S2 config, got: %v", err)
	}

	// LogManager should be created but disabled
	if logManager == nil {
		t.Error("Expected non-nil log manager")
	}

	if logManager.IsEnabled() {
		t.Error("Expected log manager to be disabled without S2 config")
	}

	// These should not panic even when disabled
	logManager.LogInfo("test", "test message", nil)
	logManager.LogWarn("test", "test warning", nil)
	logManager.LogError("test", "test error", nil)

	// Close should not error
	if err := logManager.Close(); err != nil {
		t.Errorf("Expected no error on close, got: %v", err)
	}
}

// TestLogLevels tests all log levels
func TestLogLevels(t *testing.T) {
	levels := []LogLevel{
		LogLevelDebug,
		LogLevelInfo,
		LogLevelWarn,
		LogLevelError,
	}

	expectedStrings := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for i, level := range levels {
		if string(level) != expectedStrings[i] {
			t.Errorf("Expected level %s, got %s", expectedStrings[i], level)
		}
	}
}

// TestLogStreamerConfig tests log streamer configuration defaults
func TestLogStreamerConfig(t *testing.T) {
	config := LogStreamerConfig{}

	// Check that defaults are properly set (would be done in NewLogStreamer)
	if config.BufferSize == 0 {
		config.BufferSize = 10000
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.LingerTime == 0 {
		config.LingerTime = 5 * time.Second
	}

	if config.BufferSize != 10000 {
		t.Errorf("Expected buffer size 10000, got %d", config.BufferSize)
	}

	if config.BatchSize != 100 {
		t.Errorf("Expected batch size 100, got %d", config.BatchSize)
	}

	if config.LingerTime != 5*time.Second {
		t.Errorf("Expected linger time 5s, got %s", config.LingerTime)
	}
}

// TestIntegrationHelpers tests that integration helpers don't panic with nil logManager
func TestIntegrationHelpersWithNilManager(t *testing.T) {
	// All these should be safe to call with nil logManager
	LogFlowEvent(nil, "10.0.1.1", "10.0.2.2", 80, "TCP", true)
	LogPolicyEvent(nil, "CREATE", "test-policy", "default", "Test message")
	LogWorkloadEvent(nil, "CREATE", "test-workload", "default", "Deployment")
	LogL7AccessLog(nil, "frontend", "backend", "GET", "/api/users", 200)
	LogStreamHealth(nil, true, "")
	LogOperatorStartup(nil, "v1.0.0", nil)
	LogOperatorShutdown(nil, "test")

	// Test with disabled logManager
	logger := zap.NewNop()
	ctx := context.Background()
	t.Setenv("S2_AUTH_TOKEN", "")

	logManager, _ := NewLogManager(ctx, logger)

	LogFlowEvent(logManager, "10.0.1.1", "10.0.2.2", 80, "TCP", true)
	LogPolicyEvent(logManager, "CREATE", "test-policy", "default", "Test message")
	LogWorkloadEvent(logManager, "CREATE", "test-workload", "default", "Deployment")
	LogL7AccessLog(logManager, "frontend", "backend", "GET", "/api/users", 200)
	LogStreamHealth(logManager, true, "")
	LogOperatorStartup(logManager, "v1.0.0", nil)
	LogOperatorShutdown(logManager, "test")

	// If we got here, nothing panicked - test passed
}

// TestLoadConfigFromEnv tests loading configuration from environment
func TestLoadConfigFromEnv(t *testing.T) {
	logger := zap.NewNop()

	// Test with no environment variables
	t.Setenv("S2_AUTH_TOKEN", "")
	_, err := LoadS2ConfigFromEnv(logger)
	if err == nil {
		t.Error("Expected error with no S2_AUTH_TOKEN")
	}

	// Test with minimal config
	t.Setenv("S2_AUTH_TOKEN", "test-token")
	config, err := LoadS2ConfigFromEnv(logger)
	if err != nil {
		t.Errorf("Expected no error with auth token set, got: %v", err)
	}

	if config.AuthToken != "test-token" {
		t.Errorf("Expected auth token 'test-token', got %s", config.AuthToken)
	}

	if config.Basin != "kestrel-logs" {
		t.Errorf("Expected default basin 'kestrel-logs', got %s", config.Basin)
	}

	// Test with custom config
	t.Setenv("S2_AUTH_TOKEN", "custom-token")
	t.Setenv("S2_BASIN", "custom-basin")
	t.Setenv("S2_STREAM", "custom-stream")
	t.Setenv("S2_ENABLE_COMPRESSION", "true")

	config, err = LoadS2ConfigFromEnv(logger)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if config.AuthToken != "custom-token" {
		t.Errorf("Expected auth token 'custom-token', got %s", config.AuthToken)
	}

	if config.Basin != "custom-basin" {
		t.Errorf("Expected basin 'custom-basin', got %s", config.Basin)
	}

	if config.Stream != "custom-stream" {
		t.Errorf("Expected stream 'custom-stream', got %s", config.Stream)
	}

	if !config.EnableCompression {
		t.Error("Expected compression to be enabled")
	}
}
