package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObserveRiseFall(t *testing.T) {
	p := NewProber(time.Second, 2, 3)
	seq := []struct {
		ok, want bool
	}{
		{true, false}, {true, true}, // rise 2
		{false, true}, {false, true}, {true, true}, // streak broken
		{false, true}, {false, true}, {false, false}, // fall 3
		{true, false}, {false, false}, {true, false}, {true, true},
	}
	for i, s := range seq {
		if got := p.Observe("k", s.ok); got != s.want {
			t.Fatalf("step %d: Observe(%v) = %v, want %v", i, s.ok, got, s.want)
		}
	}
}

func TestRetain(t *testing.T) {
	p := NewProber(time.Second, 1, 1)
	p.Observe("a", true)
	p.Observe("b", true)
	p.Retain(map[string]bool{"a": true})
	if !p.Observe("a", true) {
		t.Fatal("a lost its state")
	}
	p.Rise = 2
	if p.Observe("b", true) {
		t.Fatal("b kept its state")
	}
}

func TestProbe(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	defer srv.Close()
	p := NewProber(time.Second, 1, 1)
	if !p.Probe(context.Background(), srv.URL) {
		t.Fatal("200 should be healthy")
	}
	status = http.StatusServiceUnavailable
	if p.Probe(context.Background(), srv.URL) {
		t.Fatal("503 should be unhealthy")
	}
	if p.Probe(context.Background(), "http://127.0.0.1:1/") {
		t.Fatal("refused connection should be unhealthy")
	}
}
