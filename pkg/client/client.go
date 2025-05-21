package client

import (
	"context"
	"errors"
	"os"

	"operator/pkg/auth"
	"operator/pkg/k8s_helper"

	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReadJWTTokenFromSecret reads a JWT token from a Kubernetes secret
func ReadJWTTokenFromSecret(ctx context.Context, logger *zap.Logger, secretName string, podNamespace string) (string, error) {
	// Get the secret key name from environment or use default
	secretKey := os.Getenv("AUTH_SECRET_KEY")
	if secretKey == "" {
		secretKey = "token"
	}

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

	token := string(secret.Data["token"])
	if token == "" {
		logger.Error("JWT token not found in secret",
			zap.String("secretName", secretName),
			zap.String("namespace", podNamespace),
			zap.String("expectedKey", secretKey))
		return "", errors.New("JWT token not found in secret")
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

// NewGRPCClient creates a new gRPC client connection with authentication
func NewGRPCClient(ctx context.Context, logger *zap.Logger, serverAddress string, jwtSecretName string, podNamespace string, skipTLS bool) (*grpc.ClientConn, error) {
	// Read JWT token from Kubernetes secret
	token, err := ReadJWTTokenFromSecret(ctx, logger, jwtSecretName, podNamespace)
	if err != nil {
		logger.Error("Failed to read JWT token from secret",
			zap.String("secret", jwtSecretName),
			zap.Error(err))
		return nil, err
	}
	opts := auth.CreateDialOptions(auth.Config{
		Token: token,
	}, skipTLS)

	// Create gRPC connection with interceptors
	conn, err := grpc.NewClient(
		serverAddress,
		opts...,
	)
	if err != nil {
		logger.Error("Failed to create gRPC connection",
			zap.String("server", serverAddress),
			zap.Error(err))
		return nil, err
	}

	return conn, nil
}

// ParseToken parses the JWT token and returns the claims.
func ParseToken(tokenString string) (jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(tokenString, claims)
	return claims, err
}
