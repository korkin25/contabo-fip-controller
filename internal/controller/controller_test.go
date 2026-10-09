package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/korkin25/contabo-fip-controller/internal/contabo"
	"github.com/korkin25/contabo-fip-controller/internal/health"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

const fip = "203.0.113.10"

type fakeAPI struct {
	mu          sync.Mutex
	holder      string
	calls       []string
	assignErr   error // returned by Assign while the IP is assigned elsewhere
	failAssign  bool  // every Assign fails
	lagVerifies int   // GetVIP calls after a move that still show the old holder
	oldHolder   string
}

func (f *fakeAPI) GetVIP(_ context.Context, ip string) (contabo.VIP, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "get")
	h := f.holder
	if f.lagVerifies > 0 {
		f.lagVerifies--
		h = f.oldHolder
	}
	if h == "" {
		return contabo.VIP{IP: ip, Type: "floating", ResourceType: "null"}, nil
	}
	return contabo.VIP{IP: ip, Type: "floating", ResourceType: "instances", ResourceID: h}, nil
}

func (f *fakeAPI) Assign(_ context.Context, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "assign "+id)
	if f.failAssign {
		return errors.New("boom")
	}
	if f.assignErr != nil && f.holder != "" && f.holder != id {
		return f.assignErr
	}
	f.holder = id
	return nil
}

func (f *fakeAPI) Unassign(_ context.Context, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "unassign "+id)
	if f.holder == id {
		f.holder = ""
	}
	return nil
}

func (f *fakeAPI) mutating() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if c != "get" {
			out = append(out, c)
		}
	}
	return out
}

func node(name, id, ip string, lbls ...string) *corev1.Node {
	l := map[string]string{"contabo.com/instance-id": id}
	for _, k := range lbls {
		l[k] = ""
	}
	if id == "" {
		delete(l, "contabo.com/instance-id")
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeExternalIP, Address: "198.51.100." + ip},
			{Type: corev1.NodeInternalIP, Address: "10.0.0." + ip},
		}},
	}
}

func lbService(ns, name string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			HealthCheckNodePort:   port,
		},
	}
}

type harness struct {
	c        *Controller
	api      *fakeAPI
	recorder *record.FakeRecorder
	metrics  *Metrics
	mu       sync.Mutex
	healthy  map[string]bool // "ip:port" -> answer
}

func newHarness(t *testing.T, api *fakeAPI, objs ...runtime.Object) *harness {
	t.Helper()
	cs := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(cs, 0)
	h := &harness{
		api:      api,
		recorder: record.NewFakeRecorder(100),
		metrics:  NewMetrics(prometheus.NewRegistry()),
		healthy:  map[string]bool{},
	}
	cfg := Config{
		FloatingIP: fip,
		Services: []types.NamespacedName{
			{Namespace: "envoy", Name: "public"},
			{Namespace: "netbird", Name: "relay"},
		},
		InstanceLabel: "contabo.com/instance-id",
		AddressType:   corev1.NodeInternalIP,
		Interval:      2 * time.Second,
		VIPRefresh:    30 * time.Second,
		MaxBackoff:    time.Minute,
		VerifyTimeout: 3 * time.Second,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := New(cfg, api, health.NewProber(time.Second, 1, 1), factory.Core().V1().Nodes(),
		factory.Core().V1().Services(), h.recorder, h.metrics, log)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	c.verifyEvery = time.Millisecond
	c.probeURL = func(ip string, port int32) string { return ip + ":" + strconv.Itoa(int(port)) }
	c.probe = func(_ context.Context, url string) bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.healthy[url]
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			t.Fatalf("cache %v not synced", typ)
		}
	}
	h.c = c
	return h
}

// set marks a node (by last octet) healthy for both services.
func (h *harness) set(ip string, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy["10.0.0."+ip+":"+"31001"] = ok
	h.healthy["10.0.0."+ip+":"+"31002"] = ok
}

func (h *harness) advance(d time.Duration) {
	now := h.c.now().Add(d)
	h.c.now = func() time.Time { return now }
}

func cluster() []runtime.Object {
	return []runtime.Object{
		node("cp-01", "101", "1"),
		node("cp-02", "102", "2"),
		node("cp-03", "103", "3"),
		lbService("envoy", "public", 31001),
		lbService("netbird", "relay", 31002),
	}
}

func TestHealthyHolderIsKept(t *testing.T) {
	api := &fakeAPI{holder: "103"}
	h := newHarness(t, api, cluster()...)
	for _, ip := range []string{"1", "2", "3"} {
		h.set(ip, true)
	}
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 0 {
		t.Fatalf("unexpected API writes %v", m)
	}
	if got := testutil.ToFloat64(h.metrics.Holder.WithLabelValues("cp-03", "103")); got != 1 {
		t.Fatalf("holder metric = %v", got)
	}
	if got := testutil.ToFloat64(h.metrics.EligibleNodes); got != 3 {
		t.Fatalf("eligible nodes = %v", got)
	}
}

func TestUnhealthyHolderMoves(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.set("2", true)
	h.set("3", true)
	// cp-01 answers for one service only: it is not eligible.
	h.healthy["10.0.0.1:31001"] = true
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 1 || m[0] != "assign 102" {
		t.Fatalf("API writes %v, want [assign 102]", m)
	}
	select {
	case e := <-h.recorder.Events:
		if !strings.Contains(e, "Reassigned") {
			t.Fatalf("event %q", e)
		}
	default:
		t.Fatal("no event")
	}
	// The next cycle sees cp-02 as a healthy holder and does nothing.
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 1 {
		t.Fatalf("second cycle wrote %v", m)
	}
}

func TestNoEligibleNodeDoesNothing(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 0 {
		t.Fatalf("unexpected API writes %v", m)
	}
}

func TestExcludedAndUnlabelledNodesAreSkipped(t *testing.T) {
	api := &fakeAPI{holder: ""}
	h := newHarness(t, api,
		node("a-excluded", "101", "1", ExcludeLabel),
		node("b-nolabel", "", "2"),
		node("c-ok", "103", "3"),
		lbService("envoy", "public", 31001),
		lbService("netbird", "relay", 31002),
	)
	for _, ip := range []string{"1", "2", "3"} {
		h.set(ip, true)
	}
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 1 || m[0] != "assign 103" {
		t.Fatalf("API writes %v, want [assign 103]", m)
	}
}

func TestAssignRefusedFallsBackToUnassign(t *testing.T) {
	api := &fakeAPI{holder: "101", assignErr: errors.New("already assigned")}
	h := newHarness(t, api, cluster()...)
	h.set("3", true)
	h.c.reconcile(context.Background())
	want := []string{"assign 103", "unassign 101", "assign 103"}
	if m := api.mutating(); strings.Join(m, ",") != strings.Join(want, ",") {
		t.Fatalf("API writes %v, want %v", m, want)
	}
	if api.holder != "103" {
		t.Fatalf("holder %q", api.holder)
	}
}

func TestVerifyWaitsForLaggingAPI(t *testing.T) {
	api := &fakeAPI{holder: "101", lagVerifies: 2, oldHolder: "101"}
	h := newHarness(t, api, cluster()...)
	// The first GetVIP (cache fill) consumes one lag; the verify loop the other.
	h.set("2", true)
	h.c.reconcile(context.Background())
	if got := testutil.ToFloat64(h.metrics.Reassignments.WithLabelValues("success")); got != 1 {
		t.Fatalf("success = %v", got)
	}
}

func TestFailureBacksOff(t *testing.T) {
	api := &fakeAPI{holder: "", failAssign: true}
	h := newHarness(t, api, cluster()...)
	h.set("1", true)
	ctx := context.Background()
	h.c.reconcile(ctx) // fails, backoff 2s
	h.c.reconcile(ctx) // still in backoff
	if n := len(api.mutating()); n != 1 {
		t.Fatalf("%d assign attempts, want 1", n)
	}
	h.advance(2 * time.Second)
	h.c.reconcile(ctx) // second failure, backoff 4s
	h.advance(3 * time.Second)
	h.c.reconcile(ctx)
	if n := len(api.mutating()); n != 2 {
		t.Fatalf("%d assign attempts, want 2", n)
	}
	h.advance(time.Second)
	h.c.reconcile(ctx)
	if n := len(api.mutating()); n != 3 {
		t.Fatalf("%d assign attempts, want 3", n)
	}
	if got := testutil.ToFloat64(h.metrics.Reassignments.WithLabelValues("failure")); got != 3 {
		t.Fatalf("failures = %v", got)
	}
}

func TestDryRunDoesNotWrite(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.c.cfg.DryRun = true
	h.set("2", true)
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 0 {
		t.Fatalf("dry run wrote %v", m)
	}
	if got := testutil.ToFloat64(h.metrics.Reassignments.WithLabelValues("dry_run")); got != 1 {
		t.Fatalf("dry_run = %v", got)
	}
}

func TestMisconfiguredServiceStopsDecisions(t *testing.T) {
	bad := lbService("netbird", "relay", 31002)
	bad.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyCluster
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api,
		node("cp-01", "101", "1"), node("cp-02", "102", "2"),
		lbService("envoy", "public", 31001), bad)
	h.set("2", true)
	h.c.reconcile(context.Background())
	if m := api.mutating(); len(m) != 0 {
		t.Fatalf("unexpected API writes %v", m)
	}
	if got := testutil.ToFloat64(h.metrics.ReconcileErrors); got != 1 {
		t.Fatalf("reconcile errors = %v", got)
	}
}

func TestBackoff(t *testing.T) {
	c := &Controller{cfg: Config{Interval: 2 * time.Second, MaxBackoff: 30 * time.Second}}
	want := []time.Duration{2, 4, 8, 16, 30, 30}
	for i, w := range want {
		c.failures = i + 1
		if got := c.backoff(); got != w*time.Second {
			t.Fatalf("failures=%d: backoff %v, want %vs", i+1, got, w)
		}
	}
}

func TestNodeChangedIgnoresHeartbeats(t *testing.T) {
	c := &Controller{cfg: Config{InstanceLabel: "contabo.com/instance-id", AddressType: corev1.NodeInternalIP}}
	o := node("n", "1", "1")
	n := o.DeepCopy()
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if c.nodeChanged(o, n) {
		t.Fatal("status heartbeat counted as change")
	}
	n.Labels[ExcludeLabel] = ""
	if !c.nodeChanged(o, n) {
		t.Fatal("exclude label not noticed")
	}
}
