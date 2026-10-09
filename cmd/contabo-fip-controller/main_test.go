package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/korkin25/contabo-fip-controller/internal/controller"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCredentialsFromEnv(t *testing.T) {
	t.Setenv("CONTABO_CLIENT_ID", "id")
	t.Setenv("CONTABO_CLIENT_SECRET", "secret")
	t.Setenv("CONTABO_API_USER", "")
	t.Setenv("CONTABO_API_PASSWORD", "pw")
	if _, err := credentialsFromEnv(); err == nil || !strings.Contains(err.Error(), "CONTABO_API_USER") {
		t.Fatalf("want missing CONTABO_API_USER, got %v", err)
	}
	t.Setenv("CONTABO_API_USER", "user")
	c, err := credentialsFromEnv()
	if err != nil || c.Username != "user" || c.Password != "pw" {
		t.Fatalf("got %+v, %v", c, err)
	}
}

func TestServiceList(t *testing.T) {
	var s serviceList
	if err := s.Set("envoy/public, netbird/relay"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("bad"); err == nil {
		t.Fatal("want error")
	}
	if s.String() != "envoy/public,netbird/relay" {
		t.Fatalf("got %q", s.String())
	}
}

func TestServiceListDedupes(t *testing.T) {
	var s serviceList
	for _, v := range []string{"envoy/public", "envoy/public,netbird/relay", " envoy/public "} {
		if err := s.Set(v); err != nil {
			t.Fatal(err)
		}
	}
	if s.String() != "envoy/public,netbird/relay" {
		t.Fatalf("got %q, want each Service once", s.String())
	}
}

func TestValidate(t *testing.T) {
	good := controller.Config{
		FloatingIP:    "203.0.113.10",
		Services:      []types.NamespacedName{{Namespace: "a", Name: "b"}},
		AddressType:   corev1.NodeInternalIP,
		Interval:      2 * time.Second,
		VIPRefresh:    30 * time.Second,
		MaxBackoff:    time.Minute,
		VerifyTimeout: 15 * time.Second,
		Quarantine:    5 * time.Minute,
	}
	if err := validate(good, time.Second, 2, 2); err != nil {
		t.Fatalf("good config: %v", err)
	}
	tests := []struct {
		name  string
		mod   func(*controller.Config)
		probe time.Duration
		rise  int
		want  string
	}{
		{"ipv6", func(c *controller.Config) { c.FloatingIP = "2001:db8::1" }, time.Second, 2, "--floating-ip"},
		{"no services", func(c *controller.Config) { c.Services = nil }, time.Second, 2, "--service"},
		{"rise 0", func(*controller.Config) {}, time.Second, 0, "--rise"},
		{"interval 0", func(c *controller.Config) { c.Interval = 0 }, time.Second, 2, "--interval"},
		{"negative vip refresh", func(c *controller.Config) { c.VIPRefresh = -time.Second }, time.Second, 2, "--vip-refresh"},
		{"max backoff 0", func(c *controller.Config) { c.MaxBackoff = 0 }, time.Second, 2, "--max-backoff"},
		{"verify timeout 0", func(c *controller.Config) { c.VerifyTimeout = 0 }, time.Second, 2, "--verify-timeout"},
		{"quarantine 0", func(c *controller.Config) { c.Quarantine = 0 }, time.Second, 2, "--quarantine"},
		{"probe timeout 0", func(*controller.Config) {}, 0, 2, "--probe-timeout"},
		{"address type", func(c *controller.Config) { c.AddressType = "InternalIp" }, time.Second, 2, "--node-address-type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := good
			tt.mod(&cfg)
			err := validate(cfg, tt.probe, tt.rise, 2)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want an error about %s", err, tt.want)
			}
		})
	}
	for _, at := range []corev1.NodeAddressType{corev1.NodeHostName, corev1.NodeExternalIP, corev1.NodeInternalDNS, corev1.NodeExternalDNS} {
		cfg := good
		cfg.AddressType = at
		if err := validate(cfg, time.Second, 2, 2); err != nil {
			t.Errorf("%s: %v", at, err)
		}
	}
}

func TestHealthz(t *testing.T) {
	var fail atomic.Bool
	mux := httpMux(prometheus.NewRegistry(), func() error {
		if fail.Load() {
			return errors.New("no successful reconcile")
		}
		return nil
	})
	get := func() int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return rec.Code
	}
	if code := get(); code != http.StatusOK {
		t.Fatalf("healthy: %d", code)
	}
	fail.Store(true)
	if code := get(); code != http.StatusInternalServerError {
		t.Fatalf("unhealthy: %d", code)
	}
}

// Finding 6: shutdown waits for the loop before the lease may be released.
func TestRunGateWaitsForLoop(t *testing.T) {
	g := newRunGate()
	var finished atomic.Bool
	started := make(chan struct{})
	go g.run(context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // an in-flight move finishing
		finished.Store(true)
	})
	<-started
	g.stopAndWait()
	if !finished.Load() {
		t.Fatal("stopAndWait returned before the loop did")
	}
	// After shutdown began, a late leadership does not start the loop.
	ran := false
	g.run(context.Background(), func(context.Context) { ran = true })
	if ran {
		t.Fatal("loop started after stopAndWait")
	}
}

func TestRunGateFollowsParent(t *testing.T) {
	g := newRunGate()
	parent, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		g.run(parent, func(ctx context.Context) { <-ctx.Done() })
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop with its parent context")
	}
}
