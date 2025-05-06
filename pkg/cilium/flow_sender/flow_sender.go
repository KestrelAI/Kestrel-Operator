package flow_sender

import (
	smartcache "github.com/auto-np/client/pkg/smart_cache"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type FlowSender struct {
	Logger   *zap.Logger
	Client   *grpc.ClientConn
	flowChan chan smartcache.FlowKeyCount
}

func NewFlowSender(logger *zap.Logger, client *grpc.ClientConn) *FlowSender {
	return &FlowSender{
		Logger: logger,
		Client: client,
	}
}

func (s *FlowSender) startFlowSender(flowKey string) error {
	// open stream here
	for {
		select {
		case flow := <-s.flowChan:
			// Send flow here
		}
	}
}
