// Package health probes Service health check node ports and debounces the
// results with rise/fall thresholds.
package health

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// Prober probes URLs and keeps a debounced healthy state per key.
type Prober struct {
	HTTP *http.Client
	// Rise is the number of consecutive successes that make a key healthy,
	// Fall the number of consecutive failures that make it unhealthy.
	Rise int
	Fall int

	mu    sync.Mutex
	state map[string]*tracker
}

type tracker struct {
	healthy bool
	streak  int // consecutive results that disagree with healthy
}

// NewProber returns a prober with the given per-request timeout.
func NewProber(timeout time.Duration, rise, fall int) *Prober {
	return &Prober{
		HTTP: &http.Client{
			Timeout: timeout,
			// Probes go to node IPs: never through a proxy, never reusing a
			// connection to a node that may have died.
			Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		},
		Rise:  rise,
		Fall:  fall,
		state: map[string]*tracker{},
	}
}

// Probe reports whether url answers 200. A health check node port answers
// 200 when the node has a ready local endpoint and 503 otherwise.
func (p *Prober) Probe(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Observe records one probe result for key and returns its debounced state.
// A new key starts unhealthy and needs Rise successes.
func (p *Prober) Observe(key string, ok bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	t := p.state[key]
	if t == nil {
		t = &tracker{}
		p.state[key] = t
	}
	if ok == t.healthy {
		t.streak = 0
		return t.healthy
	}
	t.streak++
	need := p.Fall
	if ok {
		need = p.Rise
	}
	if t.streak >= need {
		t.healthy = ok
		t.streak = 0
	}
	return t.healthy
}

// Retain forgets every key not in keep, so departed nodes start over.
func (p *Prober) Retain(keep map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.state {
		if !keep[k] {
			delete(p.state, k)
		}
	}
}
