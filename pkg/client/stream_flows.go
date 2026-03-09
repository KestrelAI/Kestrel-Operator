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
// If the flows stream dies, it falls back to the data stream for the rest of the connection.
func (s *StreamClient) sendFlowDataOnFlowsStream(ctx context.Context, flowsStream v1.StreamService_StreamFlowsClient, dataStream v1.StreamService_StreamDataClient, sm *StreamManager, done chan<- error, flowChan <-chan smartcache.FlowCount) {
	useFallback := false
	for {
		select {
		case <-ctx.Done():
			return
		case flowData, ok := <-flowChan:
			if !ok {
				s.Logger.Warn("Flow channel closed unexpectedly")
				if !useFallback {
					sm.setUnhealthy(StreamTypeFlows, nil)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
				if useFallback {
					// Send on data stream (fallback mode)
					if err := s.sendFlowToServer(dataStream, flowData); err != nil {
						s.Logger.Error("Failed to send flow data on data stream (fallback)", zap.Error(err))
						select {
						case done <- err:
						default:
						}
						return
					}
				} else {
					// Send on dedicated flows stream
					flowMsg := &v1.StreamFlowsRequest{
						Request: &v1.StreamFlowsRequest_Flow{
							Flow: convertToProtoFlow(flowData),
						},
					}
					if err := s.protectedFlowsSend(flowsStream, flowMsg); err != nil {
						s.Logger.Warn("Flows stream send failed, falling back to data stream", zap.Error(err))
						sm.setUnhealthy(StreamTypeFlows, err)
						useFallback = true
						// Retry this flow on the data stream immediately
						if err := s.sendFlowToServer(dataStream, flowData); err != nil {
							s.Logger.Error("Failed to send flow data on data stream (fallback)", zap.Error(err))
							select {
							case done <- err:
							default:
							}
							return
						}
					}
				}
			}
		}
	}
}

// sendL7FlowsOnFlowsStream sends L7 access logs on the dedicated flows stream.
// If the flows stream dies, it falls back to the data stream for the rest of the connection.
func (s *StreamClient) sendL7FlowsOnFlowsStream(ctx context.Context, flowsStream v1.StreamService_StreamFlowsClient, dataStream v1.StreamService_StreamDataClient, sm *StreamManager, done chan<- error, l7FlowChan <-chan smartcache.L7Flow) {
	useFallback := false
	for {
		select {
		case <-ctx.Done():
			return
		case l7Flow, ok := <-l7FlowChan:
			if !ok {
				s.Logger.Info("L7 flow channel closed")
				return
			}
			if l7Flow.AccessLog == nil {
				continue
			}
			if useFallback {
				// Send on data stream (fallback mode)
				logMsg := &v1.StreamDataRequest{
					Request: &v1.StreamDataRequest_L7AccessLog{
						L7AccessLog: l7Flow.AccessLog,
					},
				}
				if err := s.protectedSend(dataStream, logMsg); err != nil {
					s.Logger.Error("Failed to send L7 access log on data stream (fallback)", zap.Error(err))
					select {
					case done <- err:
					default:
					}
					return
				}
			} else {
				logMsg := &v1.StreamFlowsRequest{
					Request: &v1.StreamFlowsRequest_L7AccessLog{
						L7AccessLog: l7Flow.AccessLog,
					},
				}
				if err := s.protectedFlowsSend(flowsStream, logMsg); err != nil {
					s.Logger.Warn("Flows stream L7 send failed, falling back to data stream", zap.Error(err))
					sm.setUnhealthy(StreamTypeFlows, err)
					useFallback = true
					// Retry this log on the data stream immediately
					dataMsg := &v1.StreamDataRequest{
						Request: &v1.StreamDataRequest_L7AccessLog{
							L7AccessLog: l7Flow.AccessLog,
						},
					}
					if err := s.protectedSend(dataStream, dataMsg); err != nil {
						s.Logger.Error("Failed to send L7 access log on data stream (fallback)", zap.Error(err))
						select {
						case done <- err:
						default:
						}
						return
					}
				}
			}
		}
	}
}
