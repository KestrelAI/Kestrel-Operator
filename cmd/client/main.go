package main

import (
	"context"
	"log"
	"os"

	"github.com/auto-np/client/pkg/client"
	"go.uber.org/zap"
)

func main() {

	log.Println("Starting auto-np client...")

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	credentials := &client.Credentials{
		ClientID:     os.Getenv("CLIENT_ID"),
		ClientSecret: os.Getenv("CLIENT_SECRET"),
	}
	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newgRPCClient, err := client.NewGRPCClient(ctx, logger, *credentials, "https://localhost:8080/oauth/token", false)
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
