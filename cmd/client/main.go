package main

import (
	"context"
	"log"

	"operator/pkg/client"

	"go.uber.org/zap"
)

func main() {
	log.Println("Starting AutoNP Operator...")

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration from environment variables (populated by Helm)
	config, err := client.LoadConfigFromEnv()
	if err != nil {
		logger.Error("Error loading Operator config from env variables", zap.Error(err))
		return
	}
	logger.Info("Loaded server configuration",
		zap.String("host", config.Host),
		zap.Int("port", config.Port),
		zap.Bool("useTLS", config.UseTLS))

	// Create StreamClient with the loaded configuration
	streamClient, err := client.NewStreamClient(ctx, logger, *config)
	if err != nil {
		logger.Error("Error creating stream client", zap.Error(err))
		return
	}
	defer streamClient.Client.Close()

	// Start the operator
	if err := streamClient.StartOperator(ctx); err != nil {
		logger.Error("Error starting operator", zap.Error(err))
	}
}
