package client

import (
	"context"
	"sync"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/smart_cache"

	"go.uber.org/zap"
)

// flowsStreamSendMu protects concurrent Send calls on the flows stream.
var flowsStreamSendMu sync.Mutex

// protectedFlowsSend sends a message on the flows stream with mutex protection.
func (s *StreamClient) protectedFlowsSend(stream v1.StreamService_StreamFlowsClient, msg *v1.StreamFlowsRequest) error {
	flowsStreamSendMu.Lock()
	defer flowsStreamSendMu.Unlock()
	return stream.Send(msg)
}

// sendFlowDataOnFlowsStream sends L3/L4 flow data on the dedicated flows stream.
// If the flows stream dies, it marks the stream as unhealthy and drains the channel
// to prevent the smart cache producer from blocking. Does NOT trigger a full reconnect.
func (s *StreamClient) sendFlowDataOnFlowsStream(ctx context.Context, stream v1.StreamService_StreamFlowsClient, sm *StreamManager, flowChan <-chan smartcache.FlowCount) {
	for {
		select {
		case <-ctx.Done():
			return
		case flowData, ok := <-flowChan:
			if !ok {
				s.Logger.Warn("Flow channel closed unexpectedly")
				sm.setUnhealthy(StreamTypeFlows, nil)
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
				flowMsg := &v1.StreamFlowsRequest{
					Request: &v1.StreamFlowsRequest_Flow{
						Flow: convertToProtoFlow(flowData),
					},
				}
				if err := s.protectedFlowsSend(stream, flowMsg); err != nil {
					s.Logger.Error("Failed to send flow data on flows stream, draining channel", zap.Error(err))
					sm.setUnhealthy(StreamTypeFlows, err)
					s.drainFlowChan(ctx, flowChan)
					return
				}
			}
		}
	}
}

// drainFlowChan discards all remaining items from flowChan until ctx is cancelled.
// This prevents the smart cache producer from blocking when the flows stream is dead.
func (s *StreamClient) drainFlowChan(ctx context.Context, flowChan <-chan smartcache.FlowCount) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-flowChan:
			if !ok {
				return
			}
		}
	}
}

// sendL7FlowsOnFlowsStream sends L7 access logs on the dedicated flows stream.
func (s *StreamClient) sendL7FlowsOnFlowsStream(ctx context.Context, stream v1.StreamService_StreamFlowsClient, sm *StreamManager, l7FlowChan <-chan smartcache.L7Flow) {
	for {
		select {
		case <-ctx.Done():
			return
		case l7Flow, ok := <-l7FlowChan:
			if !ok {
				s.Logger.Info("L7 flow channel closed")
				return
			}
			if l7Flow.AccessLog != nil {
				logMsg := &v1.StreamFlowsRequest{
					Request: &v1.StreamFlowsRequest_L7AccessLog{
						L7AccessLog: l7Flow.AccessLog,
					},
				}
				if err := s.protectedFlowsSend(stream, logMsg); err != nil {
					s.Logger.Error("Failed to send L7 access log on flows stream, draining channel", zap.Error(err))
					sm.setUnhealthy(StreamTypeFlows, err)
					s.drainL7FlowChan(ctx, l7FlowChan)
					return
				}
			}
		}
	}
}

// drainL7FlowChan discards all remaining items from l7FlowChan until ctx is cancelled.
func (s *StreamClient) drainL7FlowChan(ctx context.Context, l7FlowChan <-chan smartcache.L7Flow) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-l7FlowChan:
			if !ok {
				return
			}
		}
	}
}
