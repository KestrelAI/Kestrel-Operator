package auth

import (
	"context"
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Config represents authentication configuration
type Config struct {
	Token string
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

// GetTLSConfig returns a TLS configuration.
func GetTLSConfig(skipTLS bool) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipTLS,
	}
}

// CreateDialOptions creates gRPC dial options with or without TLS and authentication
func CreateDialOptions(config Config, useTLS bool) []grpc.DialOption {
	var opts []grpc.DialOption
	tlsConfig := GetTLSConfig(useTLS)
	// Handle TLS - using insecure for simplicity, but should use proper TLS in production
	if !useTLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Add auth interceptors
	opts = append(opts,
		grpc.WithUnaryInterceptor(GetAuthInterceptor(config)),
		grpc.WithStreamInterceptor(GetStreamAuthInterceptor(config)))

	return opts
}
