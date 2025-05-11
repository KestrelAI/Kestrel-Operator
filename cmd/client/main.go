package main

import (
	"context"
	"log"
	"os"

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

	// read jwt-secret from k8s secret
	jwt, err := client.ReadJWTTokenFromSecret(ctx, logger, "jwt-secret", "default")
	if err != nil {
		logger.Error("Error reading jwt-secret", zap.Error(err))
		return
	}
	SERVER_ADDRESS := os.Getenv("SERVER_ADDRESS")
	SERVER_PORT := os.Getenv("SERVER_PORT")
	newgRPCClient, err := client.NewGRPCClient(ctx, logger, SERVER_ADDRESS+":"+SERVER_PORT, jwt, "default", false)
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
