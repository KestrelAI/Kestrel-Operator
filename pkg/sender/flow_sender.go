package flow_sender

import (
	smartcache "operator/pkg/smart_cache"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type FlowSender struct {
	Logger   *zap.Logger
	Client   *grpc.ClientConn
	flowChan chan smartcache.FlowCount
}

func NewFlowSender(logger *zap.Logger, client *grpc.ClientConn) *FlowSender {
	return &FlowSender{
		Logger:   logger,
		Client:   client,
		flowChan: make(chan smartcache.FlowCount, 100),
	}
}

// func (s *FlowSender) startFlowSender(ctx context.Context, flowKey string) error {

// 	if err != nil {
// 		return err
// 	}
// 	for {
// 		select {
// 		case flow := <-s.flowChan:
// 			s.Logger.Info("Sending flow to server",
// 				zap.String("flowKey", flowKey),
// 				zap.Int64("cousnt", flow.Count),
// 			)

// 		}
// 	}
// }
