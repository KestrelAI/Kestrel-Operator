package client

import (
	"context"
	"log"
	"math/rand"
	"time"
)

// BackoffConfig holds configuration for exponential backoff
type BackoffConfig struct {
	// InitialDelay is the first delay after a failure
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between retries
	MaxDelay time.Duration
	// Factor is the multiplier for each successive backoff
	Factor float64
	// Jitter is the percentage of randomness added to backoff delays
	Jitter float64
}

// DefaultBackoff provides a default configuration for retry backoff
var DefaultBackoff = BackoffConfig{
	InitialDelay: 1 * time.Second,
	MaxDelay:     1 * time.Minute,
	Factor:       1.5,
	Jitter:       0.2,
}

// calculateBackoff returns the next backoff duration with jitter
func calculateBackoff(config BackoffConfig, attempt int) time.Duration {
	// Calculate exponential backoff
	backoff := float64(config.InitialDelay)
	for i := 0; i < attempt; i++ {
		backoff *= config.Factor
	}

	// Apply maximum delay cap
	if backoff > float64(config.MaxDelay) {
		backoff = float64(config.MaxDelay)
	}

	// Apply jitter (random adjustment to avoid thundering herd)
	if config.Jitter > 0 {
		jitter := rand.Float64() * config.Jitter * backoff
		if rand.Float64() > 0.5 {
			backoff += jitter
		} else {
			backoff -= jitter
		}
	}

	return time.Duration(backoff)
}

// StreamWithRetry handles reconnection of streams with exponential backoff
func StreamWithRetry(ctx context.Context, config BackoffConfig, connect func(context.Context) error) {
	var attempt int
	for {
		// Check if context is cancelled before attempting connection
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping retry loop")
			return
		default:
		}

		// Try to connect
		err := connect(ctx)
		if err == nil {
			// Connection successful, reset attempt counter
			attempt = 0
			continue
		}

		// Connection failed, log error and backoff
		log.Printf("Stream connection error (attempt %d): %v", attempt, err)

		// Calculate backoff duration
		backoff := calculateBackoff(config, attempt)
		log.Printf("Retrying in %v", backoff)

		// Wait for backoff duration or context cancellation
		select {
		case <-time.After(backoff):
			// Proceed to next attempt
			attempt++
		case <-ctx.Done():
			log.Println("Context cancelled during backoff")
			return
		}
	}
}

// WithReconnect wraps a stream function with reconnection logic
func WithReconnect(ctx context.Context, streamFunc func(context.Context) error) error {
	// Create a cancellable context for the retry loop
	retryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run the stream with retry
	StreamWithRetry(retryCtx, DefaultBackoff, streamFunc)
	return nil
}
