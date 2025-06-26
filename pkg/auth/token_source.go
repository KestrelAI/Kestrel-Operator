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
	initialJWT string,
) (oauth2.TokenSource, error) {

	exp := parseExpiry(initialJWT)
	src := &grpcTokenSource{
		ctx:    ctx,
		client: cli,
		cur: &oauth2.Token{
			AccessToken: initialJWT,
			TokenType:   "Bearer",
			Expiry:      exp,
		},
	}
	return oauth2.ReuseTokenSource(src.cur, src), nil
}

func (g *grpcTokenSource) Token() (*oauth2.Token, error) {
	if g.cur.Valid() {
		return g.cur, nil
	}
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
