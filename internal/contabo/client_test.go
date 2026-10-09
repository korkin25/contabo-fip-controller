package contabo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type fakeAPI struct {
	tokens    atomic.Int32
	reject401 atomic.Int32 // answer this many API calls with 401
	t         *testing.T
	handler   func(w http.ResponseWriter, r *http.Request)
}

func newFake(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*fakeAPI, *Client) {
	f := &fakeAPI{t: t, handler: handler}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	c := New(Credentials{ClientID: "cid", ClientSecret: "cs", Username: "u", Password: "p"})
	c.APIURL = srv.URL
	c.AuthURL = srv.URL + "/token"
	return f, c
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		if err := r.ParseForm(); err != nil {
			f.t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "password" || r.Form.Get("username") != "u" || r.Form.Get("client_secret") != "cs" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		n := f.tokens.Add(1)
		_, _ = io.WriteString(w, `{"access_token":"tok`+string(rune('0'+n))+`","expires_in":300}`)
		return
	}
	if !uuidRe.MatchString(r.Header.Get("x-request-id")) {
		f.t.Errorf("bad x-request-id %q", r.Header.Get("x-request-id"))
	}
	if f.reject401.Load() > 0 {
		f.reject401.Add(-1)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.handler(w, r)
}

func TestGetVIP(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/vips/203.0.113.10" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer tok1" {
			t.Errorf("auth header %q", r.Header.Get("Authorization"))
		}
		_, _ = io.WriteString(w, `{"data":[{"type":"floating","dataCenter":"European Union 2","resourceType":"instances","resourceId":"203650802","resourceName":"vmi3650802","v4":{"ip":"203.0.113.10"}}],"_links":{}}`)
	})
	v, err := c.GetVIP(context.Background(), "203.0.113.10")
	if err != nil {
		t.Fatal(err)
	}
	if v.InstanceID() != "203650802" || v.Type != "floating" || v.IP != "203.0.113.10" {
		t.Fatalf("got %+v", v)
	}
}

func TestGetVIPUnassignedAndMissing(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/vips/203.0.113.10":
			_, _ = io.WriteString(w, `{"data":[{"type":"floating","resourceType":"null","resourceId":""}]}`)
		case "/v1/vips/203.0.113.11":
			_, _ = io.WriteString(w, `{"data":[]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	v, err := c.GetVIP(context.Background(), "203.0.113.10")
	if err != nil || v.InstanceID() != "" {
		t.Fatalf("unassigned: %+v %v", v, err)
	}
	for _, ip := range []string{"203.0.113.11", "203.0.113.12"} {
		if _, err := c.GetVIP(context.Background(), ip); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: want ErrNotFound, got %v", ip, err)
		}
	}
}

func TestAssignSendsJSONBody(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/vips/203.0.113.10/instances/42" || string(body) != "{}" {
			t.Errorf("unexpected %s %s %q", r.Method, r.URL.Path, body)
		}
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	if err := c.Assign(context.Background(), "203.0.113.10", "42"); err != nil {
		t.Fatal(err)
	}
}

func TestAPIError(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"message":"already assigned"}`)
	})
	err := c.Assign(context.Background(), "203.0.113.10", "42")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
		t.Fatalf("got %v", err)
	}
}

func TestTokenCachedAndRefreshedOn401(t *testing.T) {
	f, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"resourceType":"instances","resourceId":"1"}]}`)
	})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.GetVIP(ctx, "203.0.113.10"); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.tokens.Load(); got != 1 {
		t.Fatalf("token fetched %d times, want 1", got)
	}
	f.reject401.Store(1)
	if _, err := c.GetVIP(ctx, "203.0.113.10"); err != nil {
		t.Fatal(err)
	}
	if got := f.tokens.Load(); got != 2 {
		t.Fatalf("token fetched %d times after 401, want 2", got)
	}
}

func TestTokenExpiry(t *testing.T) {
	f, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"resourceType":"instances","resourceId":"1"}]}`)
	})
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := c.GetVIP(ctx, "203.0.113.10"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(300*time.Second - tokenSkew + time.Second)
	if _, err := c.GetVIP(ctx, "203.0.113.10"); err != nil {
		t.Fatal(err)
	}
	if got := f.tokens.Load(); got != 2 {
		t.Fatalf("token fetched %d times, want 2", got)
	}
}

func TestBadCredentials(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {})
	c.Creds.Password = "wrong"
	c.Creds.Username = "x"
	if _, err := c.GetVIP(context.Background(), "203.0.113.10"); err == nil {
		t.Fatal("want error")
	}
}
