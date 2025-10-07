package s2_streaming

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/s2-streamstore/s2-sdk-go/s2"
	"go.uber.org/zap"
)

// S2Config holds the configuration for S2 streaming
type S2Config struct {
	// Authentication token for S2
	AuthToken string
	// Basin name (e.g., "kestrel-logs")
	Basin string
	// Stream name (e.g., "operator-logs/<cluster-id>")
	Stream string
	// Enable compression for requests
	EnableCompression bool
	// Connect timeout
	ConnectTimeout time.Duration
	// Max retry attempts
	MaxRetryAttempts uint
	// Retry backoff duration
	RetryBackoff time.Duration
}

// S2StreamClient wraps the S2 SDK StreamClient for operator log streaming
type S2StreamClient struct {
	logger       *zap.Logger
	config       S2Config
	streamClient *s2.StreamClient
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewS2StreamClient creates a new S2 stream client for log streaming
func NewS2StreamClient(ctx context.Context, logger *zap.Logger, config S2Config) (*S2StreamClient, error) {
	if config.AuthToken == "" {
		return nil, fmt.Errorf("S2 auth token is required")
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("S2 basin name is required")
	}
	if config.Stream == "" {
		return nil, fmt.Errorf("S2 stream name is required")
	}

	// Set defaults
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.MaxRetryAttempts == 0 {
		config.MaxRetryAttempts = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}

	logger.Info("Creating S2 stream client",
		zap.String("basin", config.Basin),
		zap.String("stream", config.Stream),
		zap.Bool("compression", config.EnableCompression))

	// Build client config params
	var params []s2.ClientConfigParam
	params = append(params, s2.WithConnectTimeout(config.ConnectTimeout))
	params = append(params, s2.WithMaxAttempts(config.MaxRetryAttempts))
	params = append(params, s2.WithRetryBackoffDuration(config.RetryBackoff))
	params = append(params, s2.WithCompression(config.EnableCompression))
	params = append(params, s2.WithUserAgent("kestrel-operator/1.0"))

	// Create S2 stream client
	streamClient, err := s2.NewStreamClient(config.Basin, config.Stream, config.AuthToken, params...)
	if err != nil {
		logger.Error("Failed to create S2 stream client", zap.Error(err))
		return nil, fmt.Errorf("failed to create S2 stream client: %w", err)
	}

	logger.Info("Successfully created S2 stream client")

	clientCtx, cancel := context.WithCancel(ctx)

	return &S2StreamClient{
		logger:       logger,
		config:       config,
		streamClient: streamClient,
		ctx:          clientCtx,
		cancel:       cancel,
	}, nil
}

// Close closes the S2 stream client
func (c *S2StreamClient) Close() error {
	c.logger.Info("Closing S2 stream client")
	c.cancel()
	return nil
}

// GetStreamClient returns the underlying S2 StreamClient
func (c *S2StreamClient) GetStreamClient() *s2.StreamClient {
	return c.streamClient
}

// LoadS2ConfigFromEnv loads S2 configuration from environment variables
func LoadS2ConfigFromEnv() (*S2Config, error) {
	authToken := os.Getenv("S2_AUTH_TOKEN")
	if authToken == "" {
		return nil, fmt.Errorf("S2_AUTH_TOKEN environment variable is not set")
	}

	basin := os.Getenv("S2_BASIN")
	if basin == "" {
		basin = "kestrel-logs" // Default basin
	}

	stream := os.Getenv("S2_STREAM")
	if stream == "" {
		// Use cluster ID or pod namespace as part of stream name
		clusterID := os.Getenv("CLUSTER_ID")
		if clusterID == "" {
			clusterID = os.Getenv("POD_NAMESPACE")
		}
		if clusterID == "" {
			clusterID = "default"
		}
		stream = fmt.Sprintf("operator-logs/%s", clusterID)
	}

	enableCompression := os.Getenv("S2_ENABLE_COMPRESSION") == "true"

	return &S2Config{
		AuthToken:         authToken,
		Basin:             basin,
		Stream:            stream,
		EnableCompression: enableCompression,
	}, nil
}
