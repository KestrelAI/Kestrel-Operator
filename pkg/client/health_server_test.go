package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestHealthServer(t *testing.T) {
	// Create a mock logger
	logger := zap.NewNop()

	// Create a mock StreamClient
	streamClient := &StreamClient{
		Logger:          logger,
		streamHealthy:   1, // Start healthy
		lastHealthyTime: time.Now().Unix(),
		eofErrorCount:   0,
	}

	// Create health server
	healthServer := NewHealthServer(logger, streamClient, 8081)

	// Test liveness endpoint with healthy stream
	t.Run("liveness_healthy", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health/live", nil)
		w := httptest.NewRecorder()

		healthServer.livenessHandler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response HealthResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response.Status != "healthy" {
			t.Errorf("Expected status 'healthy', got '%s'", response.Status)
		}

		if !response.StreamHealthy {
			t.Error("Expected stream_healthy to be true")
		}
	})

	// Test status endpoint
	t.Run("status", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health/status", nil)
		w := httptest.NewRecorder()

		healthServer.statusHandler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response HealthResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response.Status != "healthy" {
			t.Errorf("Expected status 'healthy', got '%s'", response.Status)
		}
	})

	// Test EOF simulation endpoint
	t.Run("simulate_eof", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/health/test/simulate-eof", nil)
		w := httptest.NewRecorder()

		healthServer.simulateEOFHandler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		// Verify that the stream is now in EOF loop
		if !streamClient.IsStreamInEOFLoop() {
			t.Error("Expected stream to be in EOF loop after simulation")
		}
	})

	// Test liveness endpoint with EOF loop (after simulation)
	t.Run("liveness_eof_loop", func(t *testing.T) {
		// Stream should already be in EOF loop from previous test
		req := httptest.NewRequest("GET", "/health/live", nil)
		w := httptest.NewRecorder()

		healthServer.livenessHandler(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status 503, got %d", w.Code)
		}

		var response HealthResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response.Status != "unhealthy" {
			t.Errorf("Expected status 'unhealthy', got '%s'", response.Status)
		}

		if !response.InEOFLoop {
			t.Error("Expected in_eof_loop to be true")
		}
	})
}

func TestStreamHealthTracking(t *testing.T) {
	logger := zap.NewNop()

	streamClient := &StreamClient{
		Logger:          logger,
		streamHealthy:   0,
		lastHealthyTime: 0,
		eofErrorCount:   0,
	}

	// Test initial state
	if streamClient.IsStreamHealthy() {
		t.Error("Stream should start unhealthy")
	}

	// Test recording healthy state
	streamClient.recordStreamHealthy()
	if !streamClient.IsStreamHealthy() {
		t.Error("Stream should be healthy after recordStreamHealthy")
	}

	healthy, lastHealthy, eofCount, lastErr := streamClient.GetStreamHealthInfo()
	if !healthy {
		t.Error("GetStreamHealthInfo should report healthy")
	}
	if eofCount != 0 {
		t.Errorf("EOF count should be 0, got %d", eofCount)
	}
	if lastErr != nil {
		t.Errorf("Last error should be nil, got %v", lastErr)
	}
	if lastHealthy.IsZero() {
		t.Error("Last healthy time should not be zero")
	}

	// Test EOF loop detection
	if streamClient.IsStreamInEOFLoop() {
		t.Error("Should not be in EOF loop when healthy")
	}

	// Simulate EOF errors
	for i := 0; i < 6; i++ {
		streamClient.recordStreamError(&mockEOFError{})
	}

	if streamClient.IsStreamHealthy() {
		t.Error("Stream should be unhealthy after errors")
	}

	// Should not be in EOF loop yet (not enough time passed)
	if streamClient.IsStreamInEOFLoop() {
		t.Error("Should not be in EOF loop immediately after errors")
	}

	// Simulate time passing
	streamClient.lastHealthyTime = time.Now().Add(-10 * time.Minute).Unix()

	// Now should be in EOF loop
	if !streamClient.IsStreamInEOFLoop() {
		t.Error("Should be in EOF loop after time passes")
	}
}

// Mock error type for testing
type mockEOFError struct{}

func (e *mockEOFError) Error() string {
	return "rpc error: code = Unavailable desc = EOF"
}
