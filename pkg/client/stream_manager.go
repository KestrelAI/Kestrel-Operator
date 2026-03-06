package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamType identifies one of the 5 independent gRPC streams.
type StreamType string

const (
	StreamTypeData    StreamType = "data"
	StreamTypeFlows   StreamType = "flows"
	StreamTypeControl StreamType = "control"
	StreamTypeEvents  StreamType = "events"
	StreamTypeLogs    StreamType = "logs"
)

// AllStreamTypes lists every stream in the order they should be opened.
var AllStreamTypes = []StreamType{StreamTypeData, StreamTypeFlows, StreamTypeControl, StreamTypeEvents, StreamTypeLogs}

// StreamHealth holds per-stream health information.
type StreamHealth struct {
	Healthy   bool   `json:"healthy"`
	LastError string `json:"last_error,omitempty"`
}

// StreamStatus represents the overall connection status.
type StreamStatus string

const (
	StreamStatusConnected    StreamStatus = "connected"
	StreamStatusDegraded     StreamStatus = "degraded"
	StreamStatusDisconnected StreamStatus = "disconnected"
)

// managedStream tracks the state of a single stream.
type managedStream struct {
	streamType StreamType
	healthy    atomic.Bool
	lastError  atomic.Value // stores string

	// fallbackToData is true when the server doesn't support this stream type
	// (returns Unimplemented). In this case, messages are sent on the data stream.
	fallbackToData atomic.Bool
}

// StreamManager manages 4 independent gRPC stream lifecycles.
// Each stream can fail and reconnect independently without affecting the others.
type StreamManager struct {
	client       *StreamClient
	streamClient v1.StreamServiceClient

	mu      sync.RWMutex
	streams map[StreamType]*managedStream

	logger *zap.Logger
}

// NewStreamManager creates a new stream manager.
func NewStreamManager(client *StreamClient, streamClient v1.StreamServiceClient, logger *zap.Logger) *StreamManager {
	sm := &StreamManager{
		client:       client,
		streamClient: streamClient,
		streams:      make(map[StreamType]*managedStream),
		logger:       logger,
	}
	for _, st := range AllStreamTypes {
		ms := &managedStream{streamType: st}
		sm.streams[st] = ms
	}
	return sm
}

// setHealthy marks a stream as healthy.
func (sm *StreamManager) setHealthy(st StreamType) {
	if ms, ok := sm.streams[st]; ok {
		ms.healthy.Store(true)
		ms.lastError.Store("")
	}
}

// setUnhealthy marks a stream as unhealthy with an error.
func (sm *StreamManager) setUnhealthy(st StreamType, err error) {
	if ms, ok := sm.streams[st]; ok {
		ms.healthy.Store(false)
		if err != nil {
			ms.lastError.Store(err.Error())
		}
	}
}

// setFallback marks a stream as using data stream fallback (old server).
func (sm *StreamManager) setFallback(st StreamType) {
	if ms, ok := sm.streams[st]; ok {
		ms.fallbackToData.Store(true)
		// Fallback streams are considered healthy — the data stream handles their messages.
		ms.healthy.Store(true)
		ms.lastError.Store("")
	}
}

// IsFallback returns true if a stream is using data stream fallback.
func (sm *StreamManager) IsFallback(st StreamType) bool {
	if ms, ok := sm.streams[st]; ok {
		return ms.fallbackToData.Load()
	}
	return false
}

// GetHealth returns the health of all streams.
func (sm *StreamManager) GetHealth() map[StreamType]StreamHealth {
	health := make(map[StreamType]StreamHealth)
	for st, ms := range sm.streams {
		lastErr, _ := ms.lastError.Load().(string)
		health[st] = StreamHealth{
			Healthy:   ms.healthy.Load(),
			LastError: lastErr,
		}
	}
	return health
}

// GetStatus returns the overall connection status based on stream health.
func (sm *StreamManager) GetStatus() StreamStatus {
	healthyCount := 0
	for _, ms := range sm.streams {
		if ms.healthy.Load() {
			healthyCount++
		}
	}
	switch {
	case healthyCount == len(AllStreamTypes):
		return StreamStatusConnected
	case healthyCount > 0:
		return StreamStatusDegraded
	default:
		return StreamStatusDisconnected
	}
}

// IsStreamHealthy returns true if a specific stream is healthy.
func (sm *StreamManager) IsStreamHealthy(st StreamType) bool {
	if ms, ok := sm.streams[st]; ok {
		return ms.healthy.Load()
	}
	return false
}

// isUnimplemented checks if a gRPC error indicates the server doesn't support the RPC.
func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unimplemented
}

// openFlowsStream opens the StreamFlows RPC. Returns nil stream if server doesn't support it.
func (sm *StreamManager) openFlowsStream(ctx context.Context) (v1.StreamService_StreamFlowsClient, error) {
	ctxWithMetadata, err := sm.client.createContextWithTenantMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant context for flows stream: %w", err)
	}
	stream, err := sm.streamClient.StreamFlows(ctxWithMetadata)
	if err != nil {
		if isUnimplemented(err) {
			sm.logger.Warn("Server does not support StreamFlows, falling back to data stream")
			sm.setFallback(StreamTypeFlows)
			return nil, nil
		}
		return nil, err
	}
	sm.setHealthy(StreamTypeFlows)
	sm.logger.Info("Flows stream established successfully")
	return stream, nil
}

// openEventsStream opens the StreamEvents RPC. Returns nil stream if server doesn't support it.
func (sm *StreamManager) openEventsStream(ctx context.Context) (v1.StreamService_StreamEventsClient, error) {
	ctxWithMetadata, err := sm.client.createContextWithTenantMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant context for events stream: %w", err)
	}
	stream, err := sm.streamClient.StreamEvents(ctxWithMetadata)
	if err != nil {
		if isUnimplemented(err) {
			sm.logger.Warn("Server does not support StreamEvents, falling back to data stream")
			sm.setFallback(StreamTypeEvents)
			return nil, nil
		}
		return nil, err
	}
	sm.setHealthy(StreamTypeEvents)
	sm.logger.Info("Events stream established successfully")
	return stream, nil
}

// handleFlowsHeartbeats receives heartbeat messages on the flows stream.
// If the stream dies, marks it unhealthy but does NOT tear down other streams.
func (sm *StreamManager) handleFlowsHeartbeats(ctx context.Context, stream v1.StreamService_StreamFlowsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := stream.Recv()
			if err != nil {
				sm.setUnhealthy(StreamTypeFlows, err)
				sm.logger.Warn("Flows stream error", zap.Error(err))
				return
			}
			// Heartbeat received — stream is alive
		}
	}
}

// handleEventsHeartbeats receives heartbeat messages on the events stream.
// If the stream dies, marks it unhealthy but does NOT tear down other streams.
func (sm *StreamManager) handleEventsHeartbeats(ctx context.Context, stream v1.StreamService_StreamEventsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := stream.Recv()
			if err != nil {
				sm.setUnhealthy(StreamTypeEvents, err)
				sm.logger.Warn("Events stream error", zap.Error(err))
				return
			}
			// Heartbeat received — stream is alive
		}
	}
}

// openLogsStream opens the StreamLogs RPC. Returns nil stream if server doesn't support it.
func (sm *StreamManager) openLogsStream(ctx context.Context) (v1.StreamService_StreamLogsClient, error) {
	ctxWithMetadata, err := sm.client.createContextWithTenantMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant context for logs stream: %w", err)
	}
	stream, err := sm.streamClient.StreamLogs(ctxWithMetadata)
	if err != nil {
		if isUnimplemented(err) {
			sm.logger.Warn("Server does not support StreamLogs, falling back to data stream")
			sm.setFallback(StreamTypeLogs)
			return nil, nil
		}
		return nil, err
	}
	sm.setHealthy(StreamTypeLogs)
	sm.logger.Info("Logs stream established successfully")
	return stream, nil
}

// handleLogsHeartbeats receives heartbeat messages on the logs stream.
// If the stream dies, marks it unhealthy but does NOT tear down other streams.
func (sm *StreamManager) handleLogsHeartbeats(ctx context.Context, stream v1.StreamService_StreamLogsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := stream.Recv()
			if err != nil {
				sm.setUnhealthy(StreamTypeLogs, err)
				sm.logger.Warn("Logs stream error", zap.Error(err))
				return
			}
		}
	}
}

// WaitForReconnect waits for a backoff period, then returns.
// Used by individual stream reconnect loops.
func (sm *StreamManager) WaitForReconnect(ctx context.Context, streamType StreamType, attempt int) error {
	backoff := time.Duration(1<<uint(min(attempt, 5))) * time.Second // 1s, 2s, 4s, 8s, 16s, 32s max
	sm.logger.Info("Stream reconnecting",
		zap.String("stream", string(streamType)),
		zap.Int("attempt", attempt),
		zap.Duration("backoff", backoff))
	select {
	case <-time.After(backoff):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
