package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// HealthServer provides HTTP endpoints for Kubernetes health checks
type HealthServer struct {
	logger       *zap.Logger
	streamClient *StreamClient
	server       *http.Server
}

// HealthResponse represents the JSON response for health checks
type HealthResponse struct {
	Status           string    `json:"status"`
	StreamHealthy    bool      `json:"stream_healthy"`
	LastHealthyTime  time.Time `json:"last_healthy_time,omitempty"`
	EOFErrorCount    int64     `json:"eof_error_count"`
	TotalEOFErrors   int64     `json:"total_eof_errors"`
	EOFTrackingStart time.Time `json:"eof_tracking_start,omitempty"`
	LastEOFTime      time.Time `json:"last_eof_time,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
	InEOFLoop        bool      `json:"in_eof_loop"`
	Timestamp        time.Time `json:"timestamp"`
}

// NewHealthServer creates a new health server
func NewHealthServer(logger *zap.Logger, streamClient *StreamClient, port int) *HealthServer {
	mux := http.NewServeMux()

	hs := &HealthServer{
		logger:       logger,
		streamClient: streamClient,
		server: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		},
	}

	// Register health check endpoints
	mux.HandleFunc("/health/live", hs.livenessHandler)
	mux.HandleFunc("/health/status", hs.statusHandler)
	mux.HandleFunc("/health/test/simulate-eof", hs.simulateEOFHandler)

	return hs
}

// Start starts the health server
func (hs *HealthServer) Start() error {
	hs.logger.Info("Starting health server", zap.String("addr", hs.server.Addr))
	return hs.server.ListenAndServe()
}

// Stop gracefully stops the health server
func (hs *HealthServer) Stop() error {
	hs.logger.Info("Stopping health server")
	return hs.server.Close()
}

// livenessHandler handles liveness probe requests
// Returns 200 OK if the operator should continue running
// Returns 503 Service Unavailable if the operator should be restarted
func (hs *HealthServer) livenessHandler(w http.ResponseWriter, r *http.Request) {
	// Check if the stream is in an EOF loop - if so, restart immediately
	if hs.streamClient.IsStreamInEOFLoop() {
		_, _, eofCount, lastErr := hs.streamClient.GetStreamHealthInfo()
		logFields := []zap.Field{
			zap.Int64("eof_count", eofCount),
		}
		if lastErr != nil {
			logFields = append(logFields, zap.Error(lastErr))
		}
		hs.logger.Warn("Liveness check failed: stream is in EOF loop, triggering immediate restart", logFields...)
		hs.writeHealthResponse(w, http.StatusServiceUnavailable, "unhealthy", "Stream is in EOF loop")
		return
	}

	// Stream is not in EOF loop - operator should continue running
	healthy, _, _, _ := hs.streamClient.GetStreamHealthInfo()
	var status string
	if healthy {
		status = "healthy"
	} else {
		status = "recovering"
	}

	hs.writeHealthResponse(w, http.StatusOK, status, "")
}

// statusHandler provides detailed status information for debugging
func (hs *HealthServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	healthy, lastHealthyTime, eofCount, totalEOFCount, eofTrackingStart, lastEOFTime, lastErr := hs.streamClient.GetDetailedStreamHealthInfo()
	inEOFLoop := hs.streamClient.IsStreamInEOFLoop()

	response := HealthResponse{
		StreamHealthy:    healthy,
		LastHealthyTime:  lastHealthyTime,
		EOFErrorCount:    eofCount,
		TotalEOFErrors:   totalEOFCount,
		EOFTrackingStart: eofTrackingStart,
		LastEOFTime:      lastEOFTime,
		InEOFLoop:        inEOFLoop,
		Timestamp:        time.Now(),
	}

	if healthy {
		response.Status = "healthy"
	} else if inEOFLoop {
		response.Status = "eof_loop"
	} else {
		response.Status = "unhealthy"
	}

	if lastErr != nil {
		response.LastError = lastErr.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		hs.logger.Error("Failed to encode health response", zap.Error(err))
	}
}

// simulateEOFHandler simulates an EOF error by closing the stream
func (hs *HealthServer) simulateEOFHandler(w http.ResponseWriter, r *http.Request) {
	hs.logger.Warn("Simulating EOF error by closing stream")
	hs.streamClient.SimulateEOF()
	hs.writeHealthResponse(w, http.StatusOK, "healthy", "Simulated EOF error")
}

// writeHealthResponse writes a standardized health response
func (hs *HealthServer) writeHealthResponse(w http.ResponseWriter, statusCode int, status, message string) {
	healthy, lastHealthyTime, eofCount, totalEOFCount, eofTrackingStart, lastEOFTime, lastErr := hs.streamClient.GetDetailedStreamHealthInfo()

	response := HealthResponse{
		Status:           status,
		StreamHealthy:    healthy,
		LastHealthyTime:  lastHealthyTime,
		EOFErrorCount:    eofCount,
		TotalEOFErrors:   totalEOFCount,
		EOFTrackingStart: eofTrackingStart,
		LastEOFTime:      lastEOFTime,
		InEOFLoop:        hs.streamClient.IsStreamInEOFLoop(),
		Timestamp:        time.Now(),
	}

	if lastErr != nil {
		response.LastError = lastErr.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		hs.logger.Error("Failed to encode health response", zap.Error(err))
		// Fallback to plain text response
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "status: %s\n", status)
		if message != "" {
			fmt.Fprintf(w, "message: %s\n", message)
		}
	}
}
