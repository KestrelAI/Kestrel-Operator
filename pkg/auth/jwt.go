package auth

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Config represents authentication configuration
type Config struct {
	Token string
}

// LoadConfigFromEnv loads auth config from environment variables
func LoadConfigFromEnv() Config {
	token := os.Getenv("JWT_TOKEN")
	return Config{
		Token: token,
	}
}

// GetAuthInterceptor returns a gRPC client interceptor that adds JWT token to requests
func GetAuthInterceptor(config Config) grpc.UnaryClientInterceptor {
	return grpc.UnaryClientInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if config.Token == "" {
			return fmt.Errorf("JWT token is required but not provided")
		}
		// Add JWT token to outgoing context
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", config.Token))
		return invoker(ctx, method, req, reply, cc, opts...)
	})
}

// GetStreamAuthInterceptor returns a gRPC streaming client interceptor that adds JWT token to requests
func GetStreamAuthInterceptor(config Config) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if config.Token == "" {
			return nil, fmt.Errorf("JWT token is required but not provided")
		}
		// Add JWT token to outgoing context
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", config.Token))
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// CreateDialOptions creates gRPC dial options with or without TLS and authentication
func CreateDialOptions(config Config, useTLS bool) []grpc.DialOption {
	var opts []grpc.DialOption

	// Handle TLS - using insecure for simplicity, but should use proper TLS in production
	if !useTLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add auth interceptors
	opts = append(opts,
		grpc.WithUnaryInterceptor(GetAuthInterceptor(config)),
		grpc.WithStreamInterceptor(GetStreamAuthInterceptor(config)))

	return opts
}
