package client

import (
	"context"

	v1 "operator/api/cloud/v1"

	cilium "operator/pkg/cilium"

	smartcache "operator/pkg/smart_cache"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type StreamClient struct {
	Logger *zap.Logger
	Client *grpc.ClientConn
}

// startOperator starts the operator and begins to stream data to the server and listens to cilium flows.
func (s *StreamClient) StartOperator(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	flowChan := make(chan smartcache.FlowCount)

	cache := smartcache.InitFlowCache(ctx, flowChan)
	flowCollector, err := cilium.NewFlowCollector(ctx, s.Logger, "kube-system", cache)
	if err != nil {
		s.Logger.Error("Failed to create flow collector", zap.Error(err))
		return err
	}

	go flowCollector.ExportCiliumFlows(ctx)

	// Create a new stream service client
	streamClient := v1.NewStreamServiceClient(s.Client)

	// Define the stream function that will be retried
	streamFunc := func(ctx context.Context) error {
		// Start the bidirectional stream
		stream, err := streamClient.StreamData(ctx)
		if err != nil {
			s.Logger.Error("Failed to establish stream", zap.Error(err))
			return err
		}

		// Create a channel to handle stream closure
		done := make(chan error, 1)

		// Start goroutine to handle incoming messages
		go func() {
			for {
				response, err := stream.Recv()
				if err != nil {
					done <- err
					return
				}

				// Handle the response based on its type
				switch resp := response.Response.(type) {
				case *v1.StreamDataResponse_Ack:
					s.Logger.Debug("Received acknowledgment from server")
				case *v1.StreamDataResponse_NetworkPolicy:
					s.Logger.Info("Received network policy from server", zap.String("name", resp.NetworkPolicy.String()))
				}
			}
		}()

		// Wait for stream to end or context cancellation
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Use the retry logic to handle stream reconnection
	return WithReconnect(ctx, streamFunc)
}
