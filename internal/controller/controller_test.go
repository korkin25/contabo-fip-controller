package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	mu     sync.Mutex
	holder string
	calls  []string
	// assignErr, when set, runs under mu before an Assign takes effect and
	// may fail it or change f.holder.
	assignErr   func(f *fakeAPI, id string) error
	getErr      error  // every GetVIP fails
	onUnassign  func() // runs before an Unassign takes effect
	lagVerifies int    // GetVIP calls after a move that still show the old holder
	oldHolder   string
}

// refuseWhileAssigned fails an Assign with an API error of the given status
// while the IP is held by another instance.
func refuseWhileAssigned(status int) func(*fakeAPI, string) error {
	return func(f *fakeAPI, id string) error {
		if f.holder != "" && f.holder != id {
			return &contabo.APIError{Method: "POST", Path: "/v1/vips/x/instances/" + id, Status: status}
		}
		return nil
	}
}

func alwaysFail(err error) func(*fakeAPI, string) error {
	return func(*fakeAPI, string) error { return err }
}

func (f *fakeAPI) GetVIP(ctx context.Context, ip string) (contabo.VIP, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "get")
	if err := ctx.Err(); err != nil {
		return contabo.VIP{}, err
	}
	if f.getErr != nil {
		return contabo.VIP{}, f.getErr
	}
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

func (f *fakeAPI) Assign(ctx context.Context, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "assign "+id)
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.assignErr != nil {
		if err := f.assignErr(f, id); err != nil {
			return err
		}
	}
	f.holder = id
	return nil
}

func (f *fakeAPI) Unassign(ctx context.Context, _, id string) error {
	if f.onUnassign != nil {
		f.onUnassign()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "unassign "+id)
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.holder != id {
		return &contabo.APIError{Method: "DELETE", Status: 404}
	}
	f.holder = ""
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

func (f *fakeAPI) gets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == "get" {
			n++
		}
	}
	return n
}

func (f *fakeAPI) currentHolder() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holder
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

// clock is a fake time source safe for concurrent use.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type harness struct {
	c        *Controller
	api      *fakeAPI
	recorder *record.FakeRecorder
	metrics  *Metrics
	reg      *prometheus.Registry
	clock    *clock
	logs     *syncBuffer
	mu       sync.Mutex
	healthy  map[string]bool // "ip:port" -> answer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) count(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.b.String(), sub)
}

func newHarness(t *testing.T, api *fakeAPI, objs ...runtime.Object) *harness {
	t.Helper()
	cs := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(cs, 0)
	reg := prometheus.NewRegistry()
	h := &harness{
		api:      api,
		recorder: record.NewFakeRecorder(100),
		metrics:  NewMetrics(reg),
		reg:      reg,
		clock:    &clock{now: time.Unix(1_000_000, 0)},
		logs:     &syncBuffer{},
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
		Quarantine:    5 * time.Minute,
	}
	log := slog.New(slog.NewTextHandler(h.logs, nil))
	c, err := New(cfg, api, health.NewProber(time.Second, 1, 1), factory.Core().V1().Nodes(),
		factory.Core().V1().Services(), h.recorder, h.metrics, log)
	if err != nil {
		t.Fatal(err)
	}
	c.now = h.clock.Now
	// Waiting in verify advances the fake clock, so a verify that never
	// sees the target ends at its deadline instead of hanging.
	c.after = func(d time.Duration) <-chan time.Time {
		h.clock.Advance(d)
		ch := make(chan time.Time, 1)
		ch <- h.clock.Now()
		return ch
	}
	c.verifyEvery = 100 * time.Millisecond
	// Tests that call reconcile directly start warmed up; TestWarmUp* reset it.
	c.rounds = 1 << 20
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

func (h *harness) advance(d time.Duration) { h.clock.Advance(d) }

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
	api := &fakeAPI{holder: "101", assignErr: refuseWhileAssigned(409)}
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
	api := &fakeAPI{holder: "", assignErr: alwaysFail(errors.New("boom"))}
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
		if got := c.backoff(i + 1); got != w*time.Second {
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

// Finding 1: only an API refusal may lead to an unassign.
func TestAssignErrorsThatNeverUnassign(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"503", &contabo.APIError{Status: 503}},
		{"500", &contabo.APIError{Status: 500}},
		{"404", &contabo.APIError{Status: 404}},
		{"401", &contabo.APIError{Status: 401}},
		{"429", &contabo.APIError{Status: 429}},
		{"token refused", &contabo.TokenError{Status: 400}},
		{"transport", errors.New("connection reset")},
		{"timeout", context.DeadlineExceeded},
		{"cancel", context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &fakeAPI{holder: "101", assignErr: alwaysFail(tt.err)}
			h := newHarness(t, api, cluster()...)
			h.set("3", true)
			h.c.reconcile(context.Background())
			if m := api.mutating(); strings.Join(m, ",") != "assign 103" {
				t.Fatalf("API writes %v, want only [assign 103]", m)
			}
			if api.currentHolder() != "101" {
				t.Fatalf("holder %q", api.currentHolder())
			}
		})
	}
}

func TestRefused(t *testing.T) {
	for status, want := range map[int]bool{400: true, 403: true, 409: true, 422: true,
		401: false, 404: false, 429: false, 500: false, 503: false, 302: false} {
		if got := refused(fmt.Errorf("wrapped: %w", &contabo.APIError{Status: status})); got != want {
			t.Errorf("refused(%d) = %v, want %v", status, got, want)
		}
	}
}

func TestAssignRefusedUnassignsLiveHolderNotCachedOne(t *testing.T) {
	api := &fakeAPI{holder: "101", assignErr: refuseWhileAssigned(409)}
	h := newHarness(t, api, cluster()...)
	for _, ip := range []string{"1", "2", "3"} {
		h.set(ip, true)
	}
	ctx := context.Background()
	h.c.reconcile(ctx) // caches holder 101
	// Someone else moves the IP to 102; the cache still says 101.
	api.mu.Lock()
	api.holder = "102"
	api.mu.Unlock()
	h.set("1", false)
	h.set("2", false)
	h.c.reconcile(ctx)
	want := []string{"assign 103", "unassign 102", "assign 103"}
	if m := api.mutating(); strings.Join(m, ",") != strings.Join(want, ",") {
		t.Fatalf("API writes %v, want %v", m, want)
	}
	if api.currentHolder() != "103" {
		t.Fatalf("holder %q", api.currentHolder())
	}
}

func TestAssignRefusedButLiveHolderIsTarget(t *testing.T) {
	api := &fakeAPI{holder: "101", assignErr: func(f *fakeAPI, id string) error {
		f.holder = id // the POST took effect but the answer was an error
		return &contabo.APIError{Status: 409}
	}}
	h := newHarness(t, api, cluster()...)
	h.set("3", true)
	h.c.reconcile(context.Background())
	if m := api.mutating(); strings.Join(m, ",") != "assign 103" {
		t.Fatalf("API writes %v, want only [assign 103]", m)
	}
	if got := testutil.ToFloat64(h.metrics.Reassignments.WithLabelValues("success")); got != 1 {
		t.Fatalf("success = %v", got)
	}
}

func TestAssignRefusedWhileUnassignedDoesNotUnassign(t *testing.T) {
	api := &fakeAPI{holder: "", assignErr: alwaysFail(&contabo.APIError{Status: 422})}
	h := newHarness(t, api, cluster()...)
	h.set("3", true)
	h.c.reconcile(context.Background())
	if m := api.mutating(); strings.Join(m, ",") != "assign 103" {
		t.Fatalf("API writes %v, want only [assign 103]", m)
	}
}

// Finding 6: a cancel between unassign and assign must not strand the IP.
func TestFallbackPairSurvivesCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeAPI{holder: "101", assignErr: refuseWhileAssigned(409), onUnassign: cancel}
	h := newHarness(t, api, cluster()...)
	h.set("3", true)
	h.c.reconcile(ctx)
	want := []string{"assign 103", "unassign 101", "assign 103"}
	if m := api.mutating(); strings.Join(m, ",") != strings.Join(want, ",") {
		t.Fatalf("API writes %v, want %v", m, want)
	}
	if api.currentHolder() != "103" {
		t.Fatalf("holder %q: the IP was stranded", api.currentHolder())
	}
}

func TestFallbackNotStartedAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeAPI{holder: "101", assignErr: func(f *fakeAPI, id string) error {
		cancel() // the signal arrives during the first POST
		return &contabo.APIError{Status: 409}
	}}
	h := newHarness(t, api, cluster()...)
	h.set("3", true)
	h.c.reconcile(ctx)
	if m := api.mutating(); strings.Join(m, ",") != "assign 103" {
		t.Fatalf("API writes %v, want only [assign 103]", m)
	}
}

// Finding 2: a target that keeps failing is quarantined and the next
// eligible node is tried.
func TestFailingTargetIsQuarantined(t *testing.T) {
	api := &fakeAPI{holder: "101", assignErr: func(f *fakeAPI, id string) error {
		if id == "102" {
			return errors.New("boom")
		}
		return nil
	}}
	h := newHarness(t, api, cluster()...)
	h.set("2", true)
	h.set("3", true)
	ctx := context.Background()
	for range QuarantineAfter {
		h.c.reconcile(ctx)
		h.advance(time.Minute)
	}
	h.c.reconcile(ctx)
	want := []string{"assign 102", "assign 102", "assign 102", "assign 103"}
	if m := api.mutating(); strings.Join(m, ",") != strings.Join(want, ",") {
		t.Fatalf("API writes %v, want %v", m, want)
	}
	if api.currentHolder() != "103" {
		t.Fatalf("holder %q", api.currentHolder())
	}
	// When cp-03 fails later, 102 is still quarantined and nothing moves.
	h.set("3", false)
	h.c.reconcile(ctx)
	if n := len(api.mutating()); n != 4 {
		t.Fatalf("moved to a quarantined instance: %v", api.mutating())
	}
	// After the quarantine 102 is a target again.
	h.advance(5 * time.Minute)
	h.c.reconcile(ctx)
	if m := api.mutating(); m[len(m)-1] != "assign 102" {
		t.Fatalf("API writes %v, want a retry of 102", m)
	}
}

func TestInvalidInstanceLabelsAreIgnored(t *testing.T) {
	api := &fakeAPI{holder: ""}
	objs := []runtime.Object{
		node("a-word", "abc", "1"),
		node("b-zero", "0", "2"),
		node("c-negative", "-5", "3"),
		node("d-leading-zero", "0104", "4"),
		node("e-fraction", "1.5", "5"),
		node("f-plus", "+7", "6"),
		node("g-ok", "107", "7"),
		lbService("envoy", "public", 31001),
		lbService("netbird", "relay", 31002),
	}
	h := newHarness(t, api, objs...)
	for _, ip := range []string{"1", "2", "3", "4", "5", "6", "7"} {
		h.set(ip, true)
	}
	ctx := context.Background()
	h.c.reconcile(ctx)
	h.c.reconcile(ctx)
	if m := api.mutating(); strings.Join(m, ",") != "assign 107" {
		t.Fatalf("API writes %v, want [assign 107]", m)
	}
	if n := h.logs.count("invalid instance label"); n != 6 {
		t.Fatalf("%d warnings over two cycles, want one per bad node (6)", n)
	}
}

func TestValidInstanceID(t *testing.T) {
	for id, want := range map[string]bool{"1": true, "203650802": true, "": false, "0": false,
		"007": false, "-1": false, "+1": false, "1e3": false, " 1": false, "99999999999999999999": false} {
		if got := validInstanceID(id); got != want {
			t.Errorf("validInstanceID(%q) = %v, want %v", id, got, want)
		}
	}
}

// Minor: a new decision starts the backoff over.
func TestNewDecisionResetsFailures(t *testing.T) {
	api := &fakeAPI{holder: "101", assignErr: alwaysFail(errors.New("boom"))}
	h := newHarness(t, api, cluster()...)
	h.set("2", true)
	ctx := context.Background()
	h.c.reconcile(ctx)
	h.advance(2 * time.Second)
	h.c.reconcile(ctx)
	if h.c.failures != 2 {
		t.Fatalf("failures = %d, want 2", h.c.failures)
	}
	// The holder recovers: no move is needed any more.
	h.set("1", true)
	h.c.reconcile(ctx)
	if h.c.failures != 0 || !h.c.notBefore.IsZero() {
		t.Fatalf("failures = %d, notBefore = %v after the decision changed", h.c.failures, h.c.notBefore)
	}
	// Days later the holder fails again: the first retry waits Interval.
	h.advance(72 * time.Hour)
	h.set("1", false)
	h.c.reconcile(ctx)
	if want := h.clock.Now().Add(2 * time.Second); !h.c.notBefore.Equal(want) {
		t.Fatalf("notBefore = %v, want %v", h.c.notBefore, want)
	}
}

// Finding 3: during warm-up a holder that missed its first probe keeps the IP.
func TestWarmUpKeepsHolder(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.c.prober = health.NewProber(time.Second, 2, 2)
	h.c.rounds = 0
	ctx := context.Background()
	h.set("2", true)
	h.set("3", true)
	h.c.reconcile(ctx) // cp-01 misses its first probe
	h.set("1", true)
	for range 4 {
		h.c.reconcile(ctx)
	}
	if m := api.mutating(); len(m) != 0 {
		t.Fatalf("IP moved during warm-up: %v", m)
	}
}

func TestWarmUpStillMovesDeadHolderAfterwards(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.c.prober = health.NewProber(time.Second, 2, 2)
	h.c.rounds = 0
	h.set("2", true)
	ctx := context.Background()
	for i := range 4 {
		h.c.reconcile(ctx)
		if m := api.mutating(); len(m) != 0 && i < 3 {
			t.Fatalf("round %d: moved during warm-up: %v", i+1, m)
		}
	}
	if m := api.mutating(); strings.Join(m, ",") != "assign 102" {
		t.Fatalf("API writes %v, want [assign 102] after warm-up", m)
	}
}

func TestWarmUpDoesNotDelayUnassignedIP(t *testing.T) {
	api := &fakeAPI{holder: ""}
	h := newHarness(t, api, cluster()...)
	h.c.prober = health.NewProber(time.Second, 1, 3)
	h.c.rounds = 0
	h.set("2", true)
	h.c.reconcile(context.Background())
	if m := api.mutating(); strings.Join(m, ",") != "assign 102" {
		t.Fatalf("API writes %v, want [assign 102] at once", m)
	}
}

// Finding 4: only the replica that runs the loop exports the state metrics.
func TestStateMetricsAreLeaderOnly(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.set("1", true)
	stateMetrics := []string{"contabo_fip_eligible_nodes", "contabo_fip_last_reconcile_timestamp_seconds",
		"contabo_fip_holder", "contabo_fip_node_eligible"}
	count := func() int {
		n, err := testutil.GatherAndCount(h.reg, stateMetrics...)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	h.metrics.Leader.Set(0)
	if n := count(); n != 0 {
		t.Fatalf("follower exports %d state series", n)
	}
	if n, _ := testutil.GatherAndCount(h.reg, "contabo_fip_leader"); n != 1 {
		t.Fatalf("contabo_fip_leader missing on follower")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.c.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	// 1 eligible_nodes + 1 timestamp + 1 holder + 3 node_eligible.
	for count() != 6 {
		if time.Now().After(deadline) {
			t.Fatalf("leader exports %d state series, want 6", count())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if n := count(); n != 0 {
		t.Fatalf("%d state series left after the loop stopped", n)
	}
}

// Finding 5: series stay in place across cycles and departed ones go.
func TestStateSeriesNeverDisappear(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	for _, ip := range []string{"1", "2", "3"} {
		h.set(ip, true)
	}
	h.metrics.setActive(true)
	ctx := context.Background()
	h.c.reconcile(ctx)

	stop := make(chan struct{})
	errs := make(chan string, 1)
	go func() {
		for {
			select {
			case <-stop:
				close(errs)
				return
			default:
			}
			n, err := testutil.GatherAndCount(h.reg, "contabo_fip_node_eligible", "contabo_fip_holder")
			if err != nil || n != 4 {
				errs <- fmt.Sprintf("scrape saw %d series (%v), want 4", n, err)
				close(errs)
				return
			}
		}
	}()
	for range 300 {
		h.c.reconcile(ctx)
	}
	close(stop)
	if msg, ok := <-errs; ok {
		t.Fatal(msg)
	}

	// A departed node and a moved holder drop their old series only.
	h.c.recordState("102", []NodeState{{Name: "cp-01", InstanceID: "101"}, {Name: "cp-02", InstanceID: "102", Eligible: true}})
	if n, _ := testutil.GatherAndCount(h.reg, "contabo_fip_node_eligible"); n != 2 {
		t.Fatalf("node_eligible series = %d, want 2", n)
	}
	want := `
# HELP contabo_fip_holder 1 for the instance the floating IP is assigned to; node is empty when the instance is not a known node. Leader only.
# TYPE contabo_fip_holder gauge
contabo_fip_holder{instance_id="102",node="cp-02"} 1
`
	if err := testutil.GatherAndCompare(h.reg, strings.NewReader(want), "contabo_fip_holder"); err != nil {
		t.Fatal(err)
	}
	h.c.recordState("", nil)
	if n, _ := testutil.GatherAndCount(h.reg, "contabo_fip_node_eligible", "contabo_fip_holder"); n != 0 {
		t.Fatalf("%d series left for an empty cluster", n)
	}
}

// Finding 7: failed VIP reads back off.
func TestVIPReadFailuresBackOff(t *testing.T) {
	api := &fakeAPI{holder: "101", getErr: errors.New("token refused")}
	h := newHarness(t, api, cluster()...)
	h.set("1", true)
	ctx := context.Background()
	// Interval 2s: reads at 0s, 2s, 6s, 14s; one cycle per second.
	for range 15 {
		h.c.reconcile(ctx)
		h.advance(time.Second)
	}
	if n := api.gets(); n != 4 {
		t.Fatalf("%d VIP reads in 15s, want 4", n)
	}
	api.mu.Lock()
	api.getErr = nil
	api.mu.Unlock()
	h.advance(time.Minute)
	h.c.reconcile(ctx)
	if h.c.vipFailures != 0 || !h.c.vipKnown {
		t.Fatalf("read did not recover: failures=%d known=%v", h.c.vipFailures, h.c.vipKnown)
	}
}

// Minor: verify ends at its deadline and does not print a stale holder.
func TestVerifyGivesUpAtDeadline(t *testing.T) {
	api := &fakeAPI{holder: "101", lagVerifies: 1 << 30, oldHolder: "101"}
	h := newHarness(t, api, cluster()...)
	h.set("2", true)
	h.c.reconcile(context.Background())
	if got := testutil.ToFloat64(h.metrics.Reassignments.WithLabelValues("failure")); got != 1 {
		t.Fatalf("failure = %v", got)
	}
	e := <-h.recorder.Events
	if !strings.Contains(e, `API still reports instance "101"`) {
		t.Fatalf("event %q", e)
	}
}

func TestVerifyReadErrorIsReported(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.set("2", true)
	h.c.reconcile(context.Background()) // fills the cache, then moves
	api.mu.Lock()
	api.holder = "101"
	api.calls = nil
	api.mu.Unlock()
	h.c.resetMove()
	h.c.vip = contabo.VIP{Type: "floating", ResourceType: "instances", ResourceID: "999"}
	api.assignErr = func(f *fakeAPI, id string) error {
		f.getErr = errors.New("api down")
		return nil
	}
	h.c.move(context.Background(), "101", NodeState{Name: "cp-02", InstanceID: "102"})
	var last string
	for len(h.recorder.Events) > 0 {
		last = <-h.recorder.Events
	}
	if !strings.Contains(last, "cannot read floating IP") || strings.Contains(last, "999") {
		t.Fatalf("event %q", last)
	}
}

// Minor: the type warning is logged once per change.
func TestNotFloatingWarningOnce(t *testing.T) {
	h := newHarness(t, &fakeAPI{}, cluster()...)
	for _, typ := range []string{"additional", "additional", "additional", "floating", "additional"} {
		h.c.setVIP(contabo.VIP{Type: typ})
	}
	if n := h.logs.count("not a floating IP"); n != 2 {
		t.Fatalf("%d warnings, want 2", n)
	}
}

// Minor: Healthy fails only on a running loop without recent success.
func TestHealthy(t *testing.T) {
	api := &fakeAPI{holder: "101"}
	h := newHarness(t, api, cluster()...)
	h.set("1", true)
	h.c.lastOK.Store(h.clock.Now().UnixNano())
	if err := h.c.Healthy(); err != nil {
		t.Fatalf("follower: %v", err)
	}
	h.c.active.Store(true)
	h.advance(h.c.StaleAfter() + time.Second)
	if err := h.c.Healthy(); err == nil {
		t.Fatal("stale leader reported healthy")
	}
	// A failed cycle does not count.
	api.mu.Lock()
	api.getErr = errors.New("down")
	api.mu.Unlock()
	h.c.vipKnown = false
	h.c.reconcile(context.Background())
	if err := h.c.Healthy(); err == nil {
		t.Fatal("failed cycle counted as success")
	}
	if got := testutil.ToFloat64(h.metrics.LastReconcile); got != 0 {
		t.Fatalf("LastReconcile set by a failed cycle: %v", got)
	}
	api.mu.Lock()
	api.getErr = nil
	api.mu.Unlock()
	h.c.vipNotBefore = time.Time{}
	h.c.reconcile(context.Background())
	if err := h.c.Healthy(); err != nil {
		t.Fatalf("after a good cycle: %v", err)
	}
	if got := testutil.ToFloat64(h.metrics.LastReconcile); got != float64(h.clock.Now().Unix()) {
		t.Fatalf("LastReconcile = %v", got)
	}
	h.c.active.Store(false)
	h.advance(time.Hour)
	if err := h.c.Healthy(); err != nil {
		t.Fatalf("stopped loop: %v", err)
	}
}

func TestStaleAfter(t *testing.T) {
	c := &Controller{cfg: Config{Interval: 2 * time.Second, VerifyTimeout: 15 * time.Second, MaxBackoff: time.Minute}}
	if got := c.StaleAfter(); got != 77*time.Second {
		t.Fatalf("StaleAfter = %v", got)
	}
	c.cfg = Config{Interval: 30 * time.Second, VerifyTimeout: time.Second, MaxBackoff: time.Second}
	if got := c.StaleAfter(); got != 300*time.Second {
		t.Fatalf("StaleAfter = %v", got)
	}
}
