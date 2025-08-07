package auth

import (
	"context"
	"time"

	serverv1 "server/api/gen/server/v1"

	"github.com/golang-jwt/jwt/v4"
	"golang.org/x/oauth2"
)

// grpcTokenSource implements oauth2.TokenSource and refreshes itself by calling
// RenewClusterToken on the server.
type grpcTokenSource struct {
	ctx    context.Context
	client serverv1.AutonpServerServiceClient

	cur *oauth2.Token
}

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
	// Simply return the current token - renewal is handled by periodic goroutine in stream client
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
