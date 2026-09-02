package auth

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
)

type Principal struct {
	Subject string
	Tenant  string
	Role    string
}
type contextKey struct{}

var ErrUnauthorized = errors.New("unauthorized")
var ErrForbidden = errors.New("forbidden")

type Authenticator struct {
	tokens  map[string]Principal
	Require bool
}

func New(tokens map[string]Principal) *Authenticator {
	return &Authenticator{tokens: tokens, Require: len(tokens) > 0}
}
func FromEnv(key string) *Authenticator {
	raw := os.Getenv(key)
	tokens := map[string]Principal{}
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(item, ":")
		if len(parts) >= 3 {
			tokens[parts[0]] = Principal{Subject: parts[0], Tenant: parts[1], Role: parts[2]}
		}
	}
	return New(tokens)
}
func (a *Authenticator) Authenticate(token string) (Principal, error) {
	if !a.Require {
		return Principal{Subject: "local", Tenant: "local", Role: "admin"}, nil
	}
	p, ok := a.tokens[token]
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	return p, nil
}
func (a *Authenticator) PrincipalFrom(r *http.Request) (Principal, error) {
	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}
	return a.Authenticate(raw)
}
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(contextKey{}).(Principal)
	return p, ok
}
func (a *Authenticator) RequireHTTP(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := a.PrincipalFrom(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !allowed(p.Role, role) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
	})
}
func allowed(actual, required string) bool {
	rank := map[string]int{"viewer": 1, "operator": 2, "admin": 3}
	return rank[actual] >= rank[required]
}
