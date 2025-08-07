package auth

import (
	"context"
	"log"
	"os"
	"time"

	serverv1 "server/api/gen/server/v1"

	"github.com/golang-jwt/jwt/v4"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// grpcTokenSource implements oauth2.TokenSource and refreshes itself by calling
// RenewClusterToken on the server.
type grpcTokenSource struct {
	ctx    context.Context
	client serverv1.AutonpServerServiceClient

	cur *oauth2.Token
}

const (
	RuntimeSecretName = "kestrel-operator-jwt-runtime"
)

// NewTokenSource builds a TokenSource from the *first* token obtained during onboarding.
func NewTokenSource(
	ctx context.Context,
	cli serverv1.AutonpServerServiceClient,
	token string,
) (oauth2.TokenSource, error) {

	exp := parseExpiry(token)
	src := &grpcTokenSource{
		ctx:    ctx,
		client: cli,
		cur: &oauth2.Token{
			AccessToken: token,
			TokenType:   "Bearer",
			Expiry:      exp,
		},
	}
	return oauth2.ReuseTokenSource(src.cur, src), nil
}

func (g *grpcTokenSource) Token() (*oauth2.Token, error) {
	if g.cur.Valid() && time.Until(g.cur.Expiry) > 1*time.Hour {
		return g.cur, nil
	}

	// Renew token if it expires within the next hour (proactive renewal)
	resp, err := g.client.RenewClusterToken(
		g.ctx,
		&serverv1.RenewClusterTokenRequest{CurrentToken: g.cur.AccessToken})
	if err != nil {
		return nil, err
	}
	g.cur = &oauth2.Token{
		AccessToken: resp.AccessToken,
		TokenType:   resp.TokenType,
		Expiry:      time.Now().Add(time.Duration(resp.ExpiresIn) * time.Second),
	}

	// Update the runtime secret with the new token
	if err := g.updateRuntimeTokenSecret(resp.AccessToken); err != nil {
		log.Printf("Failed to update runtime token secret: %v", err)
	}

	return g.cur, nil
}

func parseExpiry(raw string) time.Time {
	claims := jwt.MapClaims{}
	_, _, _ = new(jwt.Parser).ParseUnverified(raw, claims)
	if v, ok := claims["exp"].(float64); ok {
		return time.Unix(int64(v), 0)
	}
	// pessimist fallback – refresh in 10 min
	return time.Now().Add(10 * time.Minute)
}

// updateRuntimeTokenSecret updates the runtime secret (not the Helm-managed one)
func (g *grpcTokenSource) updateRuntimeTokenSecret(newToken string) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	namespace := os.Getenv("POD_NAMESPACE")

	// Create or update the runtime secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RuntimeSecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(newToken),
		},
	}

	// Try to update first, create if it doesn't exist
	_, err = clientset.CoreV1().Secrets(namespace).Update(g.ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		// If update failed, try to create
		_, err = clientset.CoreV1().Secrets(namespace).Create(g.ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		log.Printf("Created runtime token secret: %s", RuntimeSecretName)
	} else {
		log.Printf("Successfully updated runtime token secret: %s", RuntimeSecretName)
	}

	return nil
}
