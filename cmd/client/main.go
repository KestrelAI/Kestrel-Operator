package main

import (
	"context"
	"log"

	"operator/pkg/client"

	"go.uber.org/zap"
)

func main() {

	log.Println("Starting auto-np client...")

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newgRPCClient, err := client.NewGRPCClient(ctx, logger, "https://localhost:8080/oauth/token", "jwt-secret", "default", "", false)
	if err != nil {
		logger.Error("Error creating gRPC client", zap.Error(err))
		return
	}

	streamClient := &client.StreamClient{
		Logger: logger,
		Client: newgRPCClient,
	}

	streamClient.StartOperator(ctx)
}
