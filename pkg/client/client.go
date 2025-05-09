package client

import (
	"context"
	"crypto/tls"
	"errors"

	"operator/pkg/k8s_helper"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReadJWTTokenFromSecret reads a JWT token from a Kubernetes secret
func ReadJWTTokenFromSecret(ctx context.Context, logger *zap.Logger, secretName string, podNamespace string) (string, error) {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		logger.Error("Failed to create clientSet", zap.Error(err))
		return "", err
	}

	secret, err := clientset.CoreV1().Secrets(podNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		logger.Error("Failed to get secret", zap.Error(err))
		return "", err
	}

	token := string(secret.Data["jwt_token"])
	if token == "" {
		return "", errors.New("jwt_token not found in secret")
	}
	return token, nil
}

func DoesK8sSecretExist(ctx context.Context, logger *zap.Logger, secretName string, podNamespace string) bool {
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		logger.Error("Failed to create clientSet", zap.Error(err))
	}

	_, err = clientset.CoreV1().Secrets(podNamespace).Get(ctx, secretName, metav1.GetOptions{})
	return err == nil
}

// GetTLSConfig returns a TLS configuration.
func GetTLSConfig(skipTLS bool) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipTLS,
	}
}

// Unary interceptor
func authInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		md := metadata.New(map[string]string{
			"authorization": "Bearer " + token,
		})
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// Stream interceptor to attach JWT to metadata
func streamAuthInterceptor(token string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer " + token,
		})
		ctx = metadata.NewOutgoingContext(ctx, md)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// NewGRPCClient creates a new gRPC client connection with authentication
func NewGRPCClient(ctx context.Context, logger *zap.Logger, serverAddress string, jwtSecretName string, podNamespace string, tenantID string, skipTLS bool) (*grpc.ClientConn, error) {
	// Read JWT token from Kubernetes secret
	token, err := ReadJWTTokenFromSecret(ctx, logger, jwtSecretName, podNamespace)
	if err != nil {
		logger.Error("Failed to read JWT token from secret",
			zap.String("secret", jwtSecretName),
			zap.Error(err))
		return nil, err
	}

	// Configure TLS
	tlsConfig := GetTLSConfig(skipTLS)
	var transportCreds grpc.DialOption
	if skipTLS {
		transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		transportCreds = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	// Create gRPC connection with interceptors
	conn, err := grpc.NewClient(
		serverAddress,
		transportCreds,
		grpc.WithUnaryInterceptor(authInterceptor(token)),
		grpc.WithStreamInterceptor(streamAuthInterceptor(token)),
	)
	if err != nil {
		logger.Error("Failed to create gRPC connection",
			zap.String("server", serverAddress),
			zap.Error(err))
		return nil, err
	}

	return conn, nil
}
