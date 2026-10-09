// Package controller keeps a Contabo floating IP on a node where the
// watched Services have ready local endpoints.
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/korkin25/contabo-fip-controller/internal/contabo"
	"github.com/korkin25/contabo-fip-controller/internal/health"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

// ExcludeLabel is the well-known label that keeps a node out of external
// load balancers. The controller honours it the same way.
const ExcludeLabel = "node.kubernetes.io/exclude-from-external-load-balancers"

const (
	// QuarantineAfter consecutive failed moves to one instance put it in
	// quarantine, so the next eligible node gets a chance.
	QuarantineAfter = 3
	// pairTimeout bounds the unassign+assign pair of the move fallback. The
	// pair ignores cancellation so a shutdown cannot leave the IP between
	// the two calls; the pod's termination grace period must exceed it.
	pairTimeout = 45 * time.Second
)

// VIPAPI is the part of the Contabo client the controller uses.
type VIPAPI interface {
	GetVIP(ctx context.Context, ip string) (contabo.VIP, error)
	Assign(ctx context.Context, ip, instanceID string) error
	Unassign(ctx context.Context, ip, instanceID string) error
}

// Config holds the controller settings.
type Config struct {
	FloatingIP string
	// Services must all be type LoadBalancer with
	// externalTrafficPolicy Local; a node is eligible when each of their
	// healthCheckNodePorts answers 200 on it.
	Services      []types.NamespacedName
	InstanceLabel string
	AddressType   corev1.NodeAddressType
	Interval      time.Duration
	VIPRefresh    time.Duration
	MaxBackoff    time.Duration
	// VerifyTimeout bounds how long a move waits for the API to report the
	// new holder.
	VerifyTimeout time.Duration
	// Quarantine is how long an instance is not chosen as a target after
	// QuarantineAfter consecutive failed moves to it.
	Quarantine time.Duration
	DryRun     bool
}

// Controller runs the reconcile loop. Only the leader runs it.
type Controller struct {
	cfg      Config
	api      VIPAPI
	prober   *health.Prober
	nodes    corelisters.NodeLister
	services corelisters.ServiceLister
	recorder record.EventRecorder
	metrics  *Metrics
	log      *slog.Logger
	trigger  chan struct{}

	// Overridable in tests.
	now         func() time.Time
	after       func(time.Duration) <-chan time.Time
	probe       func(ctx context.Context, url string) bool
	probeURL    func(ip string, port int32) string
	verifyEvery time.Duration

	// Loop state, touched only by the reconcile goroutine.
	vip          contabo.VIP
	vipKnown     bool
	vipAt        time.Time
	vipType      string // last seen VIP type, to warn once per change
	vipErr       error  // last failed read
	vipFailures  int    // consecutive failed reads
	vipNotBefore time.Time
	moveTarget   string // instance the current move decision targets
	failures     int    // consecutive failed moves to moveTarget
	notBefore    time.Time
	quarantine   map[string]time.Time // instance ID -> end of quarantine
	rounds       int                  // probe rounds completed since Run started
	badLabels    map[string]string    // node -> invalid instance label already logged
	eligibleSeen map[string]bool      // node label sets of contabo_fip_node_eligible
	holderLabels []string             // label set of contabo_fip_holder
	lastReason   string
	svcObjs      []*corev1.Service

	// Read by Healthy from the HTTP server.
	active atomic.Bool  // Run is running
	lastOK atomic.Int64 // UnixNano of the last successful cycle or of Run start
}

// New creates a controller and registers informer handlers that wake it up.
// Call it before the informer factory is started.
func New(cfg Config, api VIPAPI, prober *health.Prober, nodeInformer coreinformers.NodeInformer,
	svcInformer coreinformers.ServiceInformer, recorder record.EventRecorder, metrics *Metrics, log *slog.Logger,
) (*Controller, error) {
	c := &Controller{
		cfg:         cfg,
		api:         api,
		prober:      prober,
		nodes:       nodeInformer.Lister(),
		services:    svcInformer.Lister(),
		recorder:    recorder,
		metrics:     metrics,
		log:         log,
		trigger:     make(chan struct{}, 1),
		now:         time.Now,
		after:       time.After,
		probe:       prober.Probe,
		probeURL:    defaultProbeURL,
		verifyEvery: time.Second,
		quarantine:  map[string]time.Time{},
		badLabels:   map[string]string{},
	}
	if _, err := nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(any) { c.kick() },
		UpdateFunc: func(oldObj, newObj any) {
			o, ok1 := oldObj.(*corev1.Node)
			n, ok2 := newObj.(*corev1.Node)
			if !ok1 || !ok2 || c.nodeChanged(o, n) {
				c.kick()
			}
		},
		DeleteFunc: func(any) { c.kick() },
	}); err != nil {
		return nil, err
	}
	watched := map[types.NamespacedName]bool{}
	for _, s := range cfg.Services {
		watched[s] = true
	}
	if _, err := svcInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj any) bool {
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			s, ok := obj.(*corev1.Service)
			return ok && watched[types.NamespacedName{Namespace: s.Namespace, Name: s.Name}]
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { c.kick() },
			UpdateFunc: func(any, any) { c.kick() },
			DeleteFunc: func(any) { c.kick() },
		},
	}); err != nil {
		return nil, err
	}
	return c, nil
}

func defaultProbeURL(ip string, port int32) string {
	return "http://" + net.JoinHostPort(ip, strconv.Itoa(int(port))) + "/healthz"
}

// nodeChanged reports whether an update touches anything the controller
// reads. Status heartbeats are ignored so they do not add probe rounds.
func (c *Controller) nodeChanged(o, n *corev1.Node) bool {
	if o.Labels[c.cfg.InstanceLabel] != n.Labels[c.cfg.InstanceLabel] {
		return true
	}
	_, ox := o.Labels[ExcludeLabel]
	_, nx := n.Labels[ExcludeLabel]
	if ox != nx || (o.DeletionTimestamp == nil) != (n.DeletionTimestamp == nil) {
		return true
	}
	return nodeAddress(o, c.cfg.AddressType) != nodeAddress(n, c.cfg.AddressType)
}

func (c *Controller) kick() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

// Run reconciles every Interval and on informer events until ctx ends.
// While it runs, this replica exports the leader-only metrics and Healthy
// checks that cycles keep succeeding.
func (c *Controller) Run(ctx context.Context) {
	c.rounds = 0
	c.lastOK.Store(c.now().UnixNano())
	c.active.Store(true)
	c.metrics.setActive(true)
	defer func() {
		c.active.Store(false)
		c.metrics.setActive(false)
		c.eligibleSeen, c.holderLabels = nil, nil
	}()

	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		c.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.trigger:
		}
	}
}

// Healthy returns an error when the loop runs but has not completed a
// successful cycle within StaleAfter. A replica that does not run the loop
// (a follower) is always healthy.
func (c *Controller) Healthy() error {
	if !c.active.Load() {
		return nil
	}
	age := c.now().Sub(time.Unix(0, c.lastOK.Load()))
	if limit := c.StaleAfter(); age > limit {
		return fmt.Errorf("no successful reconcile for %s (limit %s)", age.Round(time.Second), limit)
	}
	return nil
}

// StaleAfter is how old the last successful cycle may get before Healthy
// fails: ten intervals, or one verify plus the longest backoff if longer.
func (c *Controller) StaleAfter() time.Duration {
	return max(10*c.cfg.Interval, c.cfg.VerifyTimeout+c.cfg.MaxBackoff+c.cfg.Interval)
}

func (c *Controller) reconcile(ctx context.Context) {
	if !c.cycle(ctx) {
		return
	}
	now := c.now()
	c.lastOK.Store(now.UnixNano())
	c.metrics.LastReconcile.Set(float64(now.UnixNano()) / 1e9)
}

// cycle runs one reconcile and reports whether it succeeded: every node was
// evaluated, the last VIP read worked and no failed move is being retried.
func (c *Controller) cycle(ctx context.Context) bool {
	states, err := c.nodeStates(ctx)
	if err != nil {
		c.metrics.ReconcileErrors.Inc()
		c.setReason(slog.LevelError, "cannot evaluate nodes: "+err.Error())
		return false
	}
	c.rounds++

	now := c.now()
	if (!c.vipKnown || now.Sub(c.vipAt) >= c.cfg.VIPRefresh) && !now.Before(c.vipNotBefore) {
		c.refreshVIP(ctx)
	}
	if !c.vipKnown {
		c.metrics.ReconcileErrors.Inc()
		c.setReason(slog.LevelError, fmt.Sprintf("cannot read floating IP: %v", c.vipErr))
		return false
	}
	ok := c.vipFailures == 0
	holder := c.vip.InstanceID()
	c.recordState(holder, states)

	d := Decide(holder, states, c.quarantined(now))
	if !d.Assign {
		c.resetMove()
		level := slog.LevelInfo
		if d.Reason == ReasonNoEligible {
			level = slog.LevelWarn
		}
		c.setReason(level, d.Reason)
		return ok
	}
	if holder != "" && c.rounds < c.warmupRounds() {
		// Fresh probe state starts unhealthy: a holder that missed one probe
		// right after start would lose the IP to nodes that did not.
		c.setReason(slog.LevelInfo, d.Reason+"; warming up before moving to "+d.Target.Name)
		return ok
	}
	if d.Target.InstanceID != c.moveTarget {
		c.resetMove()
		c.moveTarget = d.Target.InstanceID
	}
	c.setReason(slog.LevelWarn, d.Reason+"; moving to "+d.Target.Name)
	if now.Before(c.notBefore) {
		return ok && c.failures == 0
	}
	return c.move(ctx, holder, d.Target) && ok
}

// warmupRounds is how many probe rounds must complete after Run starts
// before an assigned IP may move: enough for a holder that missed its first
// Fall probes to still reach Rise successes.
func (c *Controller) warmupRounds() int {
	return c.prober.Rise + c.prober.Fall
}

// resetMove forgets the failures of the previous move decision.
func (c *Controller) resetMove() {
	c.moveTarget, c.failures, c.notBefore = "", 0, time.Time{}
}

// quarantined returns the instances that must not be move targets now.
func (c *Controller) quarantined(now time.Time) map[string]bool {
	out := map[string]bool{}
	for id, until := range c.quarantine {
		if now.Before(until) {
			out[id] = true
		} else {
			delete(c.quarantine, id)
			c.log.Info("instance quarantine ended", "instance", id)
		}
	}
	return out
}

// setReason logs the decision only when it changes, so a steady state does
// not log every Interval.
func (c *Controller) setReason(level slog.Level, reason string) {
	if reason == c.lastReason {
		return
	}
	c.lastReason = reason
	c.log.Log(context.Background(), level, reason, "floatingIP", c.cfg.FloatingIP)
}

// refreshVIP re-reads the holder. Failed reads back off like failed moves,
// so a broken API or rotated credentials are not hit every Interval.
func (c *Controller) refreshVIP(ctx context.Context) {
	v, err := c.api.GetVIP(ctx, c.cfg.FloatingIP)
	if err != nil {
		c.metrics.APIErrors.WithLabelValues("get").Inc()
		c.vipErr = err
		c.vipFailures++
		retry := c.backoff(c.vipFailures)
		c.vipNotBefore = c.now().Add(retry)
		c.log.Warn("get floating IP failed", "err", err, "retryIn", retry)
		return
	}
	c.setVIP(v)
}

func (c *Controller) setVIP(v contabo.VIP) {
	if v.Type != c.vipType {
		c.vipType = v.Type
		if v.Type != "" && v.Type != "floating" {
			c.log.Warn("VIP is not a floating IP; it may not be movable", "type", v.Type)
		}
	}
	c.vip, c.vipKnown, c.vipAt = v, true, c.now()
	c.vipErr, c.vipFailures, c.vipNotBefore = nil, 0, time.Time{}
}

// validInstanceID reports whether id is a positive decimal integer without
// leading zeros, the form the API uses. Anything else could never match the
// holder the API reports.
func validInstanceID(id string) bool {
	if id == "" || id[0] == '0' {
		return false
	}
	_, err := strconv.ParseUint(id, 10, 64)
	return err == nil
}

// nodeStates probes every candidate node and returns the debounced verdicts.
func (c *Controller) nodeStates(ctx context.Context) ([]NodeState, error) {
	ports, err := c.healthPorts()
	if err != nil {
		return nil, err
	}
	nodes, err := c.nodes.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	type candidate struct {
		state NodeState
		ip    string
	}
	var cands []candidate
	bad := map[string]string{}
	for _, n := range nodes {
		id := n.Labels[c.cfg.InstanceLabel]
		if id == "" || n.DeletionTimestamp != nil {
			continue
		}
		if !validInstanceID(id) {
			bad[n.Name] = id
			if c.badLabels[n.Name] != id {
				c.log.Warn("ignoring node with an invalid instance label", "node", n.Name, "label", c.cfg.InstanceLabel, "value", id)
			}
			continue
		}
		if _, excluded := n.Labels[ExcludeLabel]; excluded {
			continue
		}
		ip := nodeAddress(n, c.cfg.AddressType)
		if ip == "" {
			continue
		}
		cands = append(cands, candidate{state: NodeState{Name: n.Name, InstanceID: id}, ip: ip})
	}
	c.badLabels = bad

	// results[i][j] is the raw probe of port j on candidate i.
	results := make([][]bool, len(cands))
	var wg sync.WaitGroup
	for i, cand := range cands {
		results[i] = make([]bool, len(ports))
		for j, port := range ports {
			wg.Go(func() { results[i][j] = c.probe(ctx, c.probeURL(cand.ip, port)) })
		}
	}
	wg.Wait()

	keep := map[string]bool{}
	states := make([]NodeState, len(cands))
	for i, cand := range cands {
		eligible := true
		for j, port := range ports {
			key := cand.state.Name + "/" + c.cfg.Services[j].String() + "/" + strconv.Itoa(int(port))
			keep[key] = true
			if !c.prober.Observe(key, results[i][j]) {
				eligible = false
			}
		}
		cand.state.Eligible = eligible
		states[i] = cand.state
	}
	c.prober.Retain(keep)
	return states, nil
}

// healthPorts returns the healthCheckNodePort of each watched Service, in
// Config order.
func (c *Controller) healthPorts() ([]int32, error) {
	ports := make([]int32, 0, len(c.cfg.Services))
	objs := make([]*corev1.Service, 0, len(c.cfg.Services))
	for _, name := range c.cfg.Services {
		s, err := c.services.Services(name.Namespace).Get(name.Name)
		if err != nil {
			return nil, fmt.Errorf("service %s: %w", name, err)
		}
		switch {
		case s.Spec.Type != corev1.ServiceTypeLoadBalancer:
			return nil, fmt.Errorf("service %s: type is %s, want LoadBalancer", name, s.Spec.Type)
		case s.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal:
			return nil, fmt.Errorf("service %s: externalTrafficPolicy is %s, want Local", name, s.Spec.ExternalTrafficPolicy)
		case s.Spec.HealthCheckNodePort == 0:
			return nil, fmt.Errorf("service %s: no healthCheckNodePort allocated", name)
		}
		ports = append(ports, s.Spec.HealthCheckNodePort)
		objs = append(objs, s)
	}
	c.svcObjs = objs
	return ports, nil
}

// recordState updates the state metrics in place. Label sets that are gone
// are deleted one by one; a Reset would let a scrape see no series at all.
func (c *Controller) recordState(holder string, states []NodeState) {
	seen := make(map[string]bool, len(states))
	eligible := 0
	holderNode := ""
	for _, s := range states {
		v := 0.0
		if s.Eligible {
			v = 1
			eligible++
		}
		c.metrics.Eligible.WithLabelValues(s.Name).Set(v)
		seen[s.Name] = true
		if s.InstanceID == holder {
			holderNode = s.Name
		}
	}
	for name := range c.eligibleSeen {
		if !seen[name] {
			c.metrics.Eligible.DeleteLabelValues(name)
		}
	}
	c.eligibleSeen = seen
	c.metrics.EligibleNodes.Set(float64(eligible))

	var hl []string
	if holder != "" {
		hl = []string{holderNode, holder}
		c.metrics.Holder.WithLabelValues(hl...).Set(1)
	}
	if c.holderLabels != nil && !slices.Equal(c.holderLabels, hl) {
		c.metrics.Holder.DeleteLabelValues(c.holderLabels...)
	}
	c.holderLabels = hl
}

// move assigns the floating IP to target and waits until the API reports
// it. It reports whether the move succeeded.
func (c *Controller) move(ctx context.Context, holder string, target NodeState) bool {
	msg := fmt.Sprintf("floating IP %s: instance %q -> node %s (instance %s)",
		c.cfg.FloatingIP, holder, target.Name, target.InstanceID)
	if c.cfg.DryRun {
		c.metrics.Reassignments.WithLabelValues("dry_run").Inc()
		c.event(corev1.EventTypeNormal, "DryRunReassign", msg)
		c.notBefore = c.now().Add(c.cfg.MaxBackoff)
		return true
	}

	err := c.assignWithFallback(ctx, target.InstanceID)
	if err == nil {
		err = c.verify(ctx, target.InstanceID)
	}
	if err != nil {
		c.failures++
		c.vipKnown = false
		c.metrics.Reassignments.WithLabelValues("failure").Inc()
		c.event(corev1.EventTypeWarning, "ReassignFailed", msg+": "+err.Error())
		if c.failures >= QuarantineAfter {
			c.quarantine[target.InstanceID] = c.now().Add(c.cfg.Quarantine)
			c.log.Error("reassign failed; quarantining target", "err", err, "node", target.Name,
				"instance", target.InstanceID, "failures", c.failures, "for", c.cfg.Quarantine)
			c.event(corev1.EventTypeWarning, "TargetQuarantined",
				fmt.Sprintf("%s: %d failed moves, not a target for %s", msg, c.failures, c.cfg.Quarantine))
			c.resetMove()
			return false
		}
		backoff := c.backoff(c.failures)
		c.notBefore = c.now().Add(backoff)
		c.log.Error("reassign failed", "err", err, "retryIn", backoff)
		return false
	}
	c.resetMove()
	delete(c.quarantine, target.InstanceID)
	c.metrics.Reassignments.WithLabelValues("success").Inc()
	c.log.Info("reassigned", "node", target.Name, "instance", target.InstanceID, "previous", holder)
	c.event(corev1.EventTypeNormal, "Reassigned", msg)
	return true
}

func (c *Controller) assign(ctx context.Context, instanceID string) error {
	err := c.api.Assign(ctx, c.cfg.FloatingIP, instanceID)
	if err != nil {
		c.metrics.APIErrors.WithLabelValues("assign").Inc()
	}
	return err
}

// assignWithFallback assigns the IP to target. When the API itself refuses
// (see refused), the IP may be assigned elsewhere and the API may not move
// it directly: the holder is read live, never from the cache, and
// unassigned before a second assign. This is the only place that unassigns.
func (c *Controller) assignWithFallback(ctx context.Context, target string) error {
	err := c.assign(ctx, target)
	if err == nil || !refused(err) {
		return err
	}
	live, gerr := c.api.GetVIP(ctx, c.cfg.FloatingIP)
	if gerr != nil {
		c.metrics.APIErrors.WithLabelValues("get").Inc()
		return errors.Join(err, fmt.Errorf("re-read holder: %w", gerr))
	}
	switch lh := live.InstanceID(); {
	case lh == target:
		c.log.Info("assign refused but the API already reports the target", "err", err, "instance", target)
		return nil
	case lh == "":
		// Unassigned, or held by something that is not an instance.
		return err
	case ctx.Err() != nil:
		return errors.Join(err, ctx.Err())
	default:
		c.log.Warn("assign refused, unassigning the live holder first", "err", err, "holder", lh)
		// From here on a cancel must not strand the IP between the calls.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pairTimeout)
		defer cancel()
		if uerr := c.api.Unassign(pctx, c.cfg.FloatingIP, lh); uerr != nil {
			c.metrics.APIErrors.WithLabelValues("unassign").Inc()
			return errors.Join(err, uerr)
		}
		return c.assign(pctx, target)
	}
}

// refused reports whether err is the API rejecting the request itself, a
// 4xx other than 401 (auth), 404 (no such VIP or instance) and 429 (rate
// limit). Transport errors, timeouts, cancellation and 5xx say nothing
// about the holder and never lead to an unassign.
func refused(err error) bool {
	var apiErr *contabo.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch s := apiErr.Status; {
	case s < 400 || s > 499:
		return false
	case s == 401 || s == 404 || s == 429:
		return false
	}
	return true
}

// verify polls the API until it reports instanceID as the holder.
func (c *Controller) verify(ctx context.Context, instanceID string) error {
	deadline := c.now().Add(c.cfg.VerifyTimeout)
	for {
		v, err := c.api.GetVIP(ctx, c.cfg.FloatingIP)
		if err == nil {
			c.setVIP(v)
			if v.InstanceID() == instanceID {
				return nil
			}
		} else {
			c.metrics.APIErrors.WithLabelValues("get").Inc()
		}
		if !c.now().Before(deadline) {
			if err != nil {
				return fmt.Errorf("cannot read floating IP after %s: %w", c.cfg.VerifyTimeout, err)
			}
			return fmt.Errorf("API still reports instance %q after %s", v.InstanceID(), c.cfg.VerifyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.after(c.verifyEvery):
		}
	}
}

// backoff is Interval doubled per failure after the first, capped at
// MaxBackoff.
func (c *Controller) backoff(failures int) time.Duration {
	d := c.cfg.Interval
	for i := 1; i < failures && d < c.cfg.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, c.cfg.MaxBackoff)
}

func (c *Controller) event(eventType, reason, msg string) {
	for _, s := range c.svcObjs {
		c.recorder.Event(s, eventType, reason, msg)
	}
}

func nodeAddress(n *corev1.Node, t corev1.NodeAddressType) string {
	for _, a := range n.Status.Addresses {
		if a.Type == t {
			return a.Address
		}
	}
	return ""
}
