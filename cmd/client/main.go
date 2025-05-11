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
	// Load configuration from environment variables (populated by Helm)
	config := client.LoadConfigFromEnv()
	logger.Info("Loaded server configuration",
		zap.String("host", config.Host),
		zap.Int("port", config.Port),
		zap.Bool("useTLS", config.UseTLS))

	// Read JWT token from Kubernetes secret if token is not provided in environment
	if config.Token == "" {
		secretName := os.Getenv("AUTH_SECRET_NAME")
		namespace := os.Getenv("POD_NAMESPACE")
		if secretName != "" && namespace != "" {
			jwt, err := client.ReadJWTTokenFromSecret(ctx, logger, secretName, namespace)
			if err != nil {
				logger.Warn("Error reading JWT token from secret", zap.Error(err))
			} else {
				config.Token = jwt
			}
		}
	}

	// Create StreamClient with the loaded configuration
	streamClient, err := client.NewStreamClient(logger, config)
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
