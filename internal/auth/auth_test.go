package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatorRBAC(t *testing.T) {
	a := New(map[string]Principal{"token": {Subject: "u", Tenant: "t", Role: "operator"}})
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	a.RequireHTTP("operator", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok || p.Tenant != "t" {
			t.Fatal("principal missing")
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	req.Header.Set("Authorization", "Bearer bad")
	rec = httptest.NewRecorder()
	a.RequireHTTP("viewer", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", rec.Code)
	}
}

func TestAuthenticatorRoleAndHeaderEdges(t *testing.T) {
	a := New(map[string]Principal{"viewer": {Subject: "v", Tenant: "tenant", Role: "viewer"}})
	if _, err := a.Authenticate(""); err == nil {
		t.Fatal("empty token accepted")
	}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rec := httptest.NewRecorder()
	a.RequireHTTP("operator", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer operator status=%d", rec.Code)
	}
	if allowed("superuser", "viewer") {
		t.Fatal("unknown role granted access")
	}
}
