package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	v1 "operator/api/gen/cloud/v1"
	"operator/pkg/client"
	"operator/pkg/operatorlog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	log.Println("Starting Kestrel AI Operator...")

	// Set up operator log streaming channels (long-lived, survive gRPC reconnections)
	operatorLogEntryCh := make(chan *v1.LogEntry, operatorlog.DefaultEntryChanSize)
	operatorLogBatchCh := make(chan *v1.PodLogs, operatorlog.DefaultBatchChanSize)
	// Two-phase operator log streaming:
	// - streamingEnabled: controls log capture into the channel (true from start
	//   so startup logs are buffered and not lost)
	// - sendingEnabled:   controls gRPC delivery (false until initial inventory
	//   sync completes, to avoid contending with the sync on sendMu)
	streamingEnabled := &atomic.Bool{}
	streamingEnabled.Store(true)
	sendingEnabled := &atomic.Bool{}
	sendingEnabled.Store(false)

	// Build tee core: logs go to both stderr (JSON) and the streaming channel
	baseCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.Lock(os.Stderr),
		zapcore.InfoLevel,
	)
	streamingCore := operatorlog.NewStreamingCore(zapcore.InfoLevel, operatorLogEntryCh, streamingEnabled)
	teeCore := zapcore.NewTee(baseCore, streamingCore)
	logger := zap.New(teeCore, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the operator log batcher (runs for the process lifetime)
	batcher := operatorlog.NewOperatorLogBatcher(operatorLogEntryCh, operatorLogBatchCh)
	go batcher.Run(ctx)

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

	// Connect operator log streaming to the stream client
	streamClient.SetOperatorLogStreaming(operatorLogBatchCh, sendingEnabled)

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
