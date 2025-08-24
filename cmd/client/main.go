package main

import (
	"context"
	"log"
	"time"

	"operator/pkg/client"

	"go.uber.org/zap"
)

func main() {
	log.Println("Starting Kestrel AI Operator...")

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Restart interval: 3 hours
	restartInterval := 3 * time.Hour

	for {
		logger.Info("Starting new operator session", zap.Duration("restart_interval", restartInterval))

		// Create a context with cancel for this session
		ctx, cancel := context.WithCancel(context.Background())

		// Load configuration from environment variables (populated by Helm)
		config, err := client.LoadConfigFromEnv()
		if err != nil {
			logger.Error("Error loading Operator config from env variables", zap.Error(err))
			cancel()
			time.Sleep(30 * time.Second) // Wait before retry
			continue
		}

		logger.Info("Loaded server configuration",
			zap.String("host", config.Host),
			zap.Int("port", config.Port),
			zap.Bool("useTLS", config.UseTLS))

		// Create StreamClient with the loaded configuration
		streamClient, err := client.NewStreamClient(ctx, logger, *config)
		if err != nil {
			logger.Error("Error creating stream client", zap.Error(err))
			cancel()
			time.Sleep(30 * time.Second) // Wait before retry
			continue
		}

		// Start the operator in a goroutine so we can control the restart timing
		operatorDone := make(chan error, 1)
		go func() {
			defer streamClient.Client.Close()
			err := streamClient.StartOperator(ctx)
			operatorDone <- err
		}()

		// Wait for either the operator to finish or the restart timer
		select {
		case err := <-operatorDone:
			if err != nil {
				logger.Error("Operator exited with error", zap.Error(err))
			} else {
				logger.Info("Operator exited cleanly")
			}
			cancel()

		case <-time.After(restartInterval):
			logger.Info("Restarting operator on regular interval", zap.Duration("session_duration", restartInterval))
			cancel()
			// Make sure goroutine fully stops and closes resources before looping
			if err := <-operatorDone; err != nil {
				logger.Warn("Operator stopped during scheduled restart", zap.Error(err))
			}
		}

	}
}
