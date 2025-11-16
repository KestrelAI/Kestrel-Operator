package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"operator/pkg/client"
	"operator/pkg/s2_streaming"

	"go.uber.org/zap"
)

func main() {
	log.Println("Starting Kestrel AI Operator...")

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize S2 log streaming and wrap logger to stream runtime logs
	logManager, wrappedLogger, err := s2_streaming.InitializeLogStreamingWithLogger(ctx, logger)
	if err != nil {
		logger.Warn("S2 log streaming initialization failed, continuing without it", zap.Error(err))
	} else if logManager != nil && logManager.IsEnabled() {
		logger = wrappedLogger // Use wrapped logger for all subsequent logging
		defer logManager.Close()
	}

	// Load configuration from environment variables (populated by Helm)
	config, err := client.LoadConfigFromEnv()
	if err != nil {
		logger.Error("Error loading Operator config from env variables", zap.Error(err))
		return
	}
	logger.Info("Loaded server configuration",
		zap.String("host", config.Host),
		zap.Int("port", config.Port))

	// Create StreamClient with the loaded configuration
	streamClient, err := client.NewStreamClient(ctx, logger, *config)
	if err != nil {
		logger.Error("Error creating stream client", zap.Error(err))
		return
	}
	defer streamClient.Client.Close()

	// Get health server port from environment variable (default to 8081)
	healthPortStr := getEnvOrDefault("HEALTH_PORT", "8081")
	healthPort, err := strconv.Atoi(healthPortStr)
	if err != nil {
		logger.Warn("Invalid HEALTH_PORT, using default 8081", zap.String("port", healthPortStr))
		healthPort = 8081
	}

	// Create and start health server
	healthServer := client.NewHealthServer(logger, streamClient, healthPort)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server failed", zap.Error(err))
		}
	}()
	defer healthServer.Stop()

	logger.Info("Health server started", zap.Int("port", healthPort))

	// Start the operator
	if err := streamClient.StartOperator(ctx); err != nil {
		logger.Error("Error starting operator", zap.Error(err))
	}
}

// Helper function to get environment variable with default value
func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
