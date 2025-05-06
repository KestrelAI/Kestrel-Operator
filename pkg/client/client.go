package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"

	"github.com/auto-np/client/pkg/k8s_helper"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Credentials struct {
	ClientID     string
	ClientSecret string
}

// ReadK8sSecret takes a secretName and reads the file.
func ReadCredentialsK8sSecrets(ctx context.Context, logger *zap.Logger, secretName string, podNamespace string) (string, string, error) {
	// Create a new clientset
	clientset, err := k8s_helper.NewClientSet()
	if err != nil {
		logger.Error("Failed to create clientSet", zap.Error(err))
		return "", "", err
	}

	// Get the secret
	secret, err := clientset.CoreV1().Secrets(podNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		logger.Error("Failed to get secret", zap.Error(err))
		return "", "", err
	}

	// Assuming your secret data has a "client_id" and "client_secret" key.
	clientID := string(secret.Data["client_id"])
	if clientID == "" {
		return "", "", errors.New("clientID not found in secret")
	}
	clientSecret := string(secret.Data["client_secret"])
	if clientSecret == "" {
		return "", "", errors.New("clientSecret not found in secret")
	}
	return clientID, clientSecret, nil
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

func NewGRPCClient(ctx context.Context, logger *zap.Logger, Oauth2Credentials Credentials, tokenURL string, skipTLS bool) (*grpc.ClientConn, error) {
	tlsConfig := GetTLSConfig(skipTLS)
	oauthConfig := clientcredentials.Config{
		ClientID:     Oauth2Credentials.ClientID,
		ClientSecret: Oauth2Credentials.ClientSecret,
		TokenURL:     tokenURL,
		AuthStyle:    oauth2.AuthStyleInParams,
	}
	tokenSource := GetTokenSource(ctx, oauthConfig, tlsConfig)
	token, err := tokenSource.Token()
	if err != nil {
		logger.Error("Error retrieving a valid token", zap.Error(err))
		return nil, err
	}

	claims, err := ParseToken(token.AccessToken)
	if err != nil {
		logger.Error("Error parsing token", zap.Error(err))
		return nil, err
	}

	aud, err := getFirstAudience(logger, claims)
	if err != nil {
		logger.Error("Error pulling audience out of token", zap.Error(err))
		return nil, err
	}

	tokenSource = GetTokenSource(ctx, oauthConfig, tlsConfig)
	creds := credentials.NewTLS(tlsConfig)
	conn, err := grpc.NewClient(
		aud,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetTokenSource returns an OAuth2 token source.
func GetTokenSource(ctx context.Context, config clientcredentials.Config, tlsConfig *tls.Config) oauth2.TokenSource {
	return config.TokenSource(context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}))
}

// ParseToken parses the JWT token and returns the claims.
func ParseToken(tokenString string) (jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(tokenString, claims)
	return claims, err
}

func getFirstAudience(logger *zap.Logger, claims jwt.MapClaims) (string, error) {
	aud, ok := claims["aud"].([]interface{})
	if !ok {
		return "", errors.New("audience not found in token")
	}

	firstAudience, ok := aud[0].(string)
	if !ok {
		return "", errors.New("first audience not found in token")
	}

	return firstAudience, nil
}
