// Package controller keeps a Contabo floating IP on a node where the
// watched Services have ready local endpoints.
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
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
	DryRun        bool
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
	probe       func(ctx context.Context, url string) bool
	probeURL    func(ip string, port int32) string
	verifyEvery time.Duration

	// Loop state, touched only by the reconcile goroutine.
	vip        contabo.VIP
	vipKnown   bool
	vipAt      time.Time
	failures   int
	notBefore  time.Time
	lastReason string
	svcObjs    []*corev1.Service
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
		probe:       prober.Probe,
		probeURL:    defaultProbeURL,
		verifyEvery: time.Second,
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
func (c *Controller) Run(ctx context.Context) {
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

func (c *Controller) reconcile(ctx context.Context) {
	defer c.metrics.LastReconcile.SetToCurrentTime()

	states, err := c.nodeStates(ctx)
	if err != nil {
		c.metrics.ReconcileErrors.Inc()
		c.setReason(slog.LevelError, "cannot evaluate nodes: "+err.Error())
		return
	}

	now := c.now()
	if !c.vipKnown || now.Sub(c.vipAt) >= c.cfg.VIPRefresh {
		if err := c.refreshVIP(ctx); err != nil && !c.vipKnown {
			c.metrics.ReconcileErrors.Inc()
			c.setReason(slog.LevelError, "cannot read floating IP: "+err.Error())
			return
		}
	}
	holder := c.vip.InstanceID()
	c.recordState(holder, states)

	d := Decide(holder, states)
	if !d.Assign {
		level := slog.LevelInfo
		if d.Reason == "no eligible node" {
			level = slog.LevelWarn
		}
		c.setReason(level, d.Reason)
		return
	}
	c.setReason(slog.LevelWarn, d.Reason+"; moving to "+d.Target.Name)
	if now.Before(c.notBefore) {
		return
	}
	c.move(ctx, holder, d.Target)
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

func (c *Controller) refreshVIP(ctx context.Context) error {
	v, err := c.api.GetVIP(ctx, c.cfg.FloatingIP)
	if err != nil {
		c.metrics.APIErrors.WithLabelValues("get").Inc()
		c.log.Warn("get floating IP failed", "err", err)
		return err
	}
	if v.Type != "" && v.Type != "floating" {
		c.log.Warn("VIP is not a floating IP; it may not be movable", "type", v.Type)
	}
	c.vip, c.vipKnown, c.vipAt = v, true, c.now()
	return nil
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
	for _, n := range nodes {
		id := n.Labels[c.cfg.InstanceLabel]
		if id == "" || n.DeletionTimestamp != nil {
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

func (c *Controller) recordState(holder string, states []NodeState) {
	c.metrics.Eligible.Reset()
	c.metrics.Holder.Reset()
	eligible := 0
	holderNode := ""
	for _, s := range states {
		v := 0.0
		if s.Eligible {
			v = 1
			eligible++
		}
		c.metrics.Eligible.WithLabelValues(s.Name).Set(v)
		if s.InstanceID == holder {
			holderNode = s.Name
		}
	}
	c.metrics.EligibleNodes.Set(float64(eligible))
	if holder != "" {
		c.metrics.Holder.WithLabelValues(holderNode, holder).Set(1)
	}
}

// move assigns the floating IP to target and waits until the API reports it.
func (c *Controller) move(ctx context.Context, holder string, target NodeState) {
	msg := fmt.Sprintf("floating IP %s: instance %q -> node %s (instance %s)",
		c.cfg.FloatingIP, holder, target.Name, target.InstanceID)
	if c.cfg.DryRun {
		c.metrics.Reassignments.WithLabelValues("dry_run").Inc()
		c.event(corev1.EventTypeNormal, "DryRunReassign", msg)
		c.notBefore = c.now().Add(c.cfg.MaxBackoff)
		return
	}

	assign := func() error {
		err := c.api.Assign(ctx, c.cfg.FloatingIP, target.InstanceID)
		if err != nil {
			c.metrics.APIErrors.WithLabelValues("assign").Inc()
		}
		return err
	}
	err := assign()
	if err != nil && holder != "" && holder != target.InstanceID {
		// The API may refuse to move an assigned IP directly. Unassign
		// first: the holder is not eligible anyway.
		c.log.Warn("assign failed, unassigning old holder first", "err", err, "holder", holder)
		if uerr := c.api.Unassign(ctx, c.cfg.FloatingIP, holder); uerr != nil {
			c.metrics.APIErrors.WithLabelValues("unassign").Inc()
			err = errors.Join(err, uerr)
		} else {
			err = assign()
		}
	}
	if err == nil {
		err = c.verify(ctx, target.InstanceID)
	}
	if err != nil {
		c.failures++
		backoff := c.backoff()
		c.notBefore = c.now().Add(backoff)
		c.vipKnown = false
		c.metrics.Reassignments.WithLabelValues("failure").Inc()
		c.log.Error("reassign failed", "err", err, "retryIn", backoff)
		c.event(corev1.EventTypeWarning, "ReassignFailed", msg+": "+err.Error())
		return
	}
	c.failures = 0
	c.notBefore = time.Time{}
	c.metrics.Reassignments.WithLabelValues("success").Inc()
	c.log.Info("reassigned", "node", target.Name, "instance", target.InstanceID, "previous", holder)
	c.event(corev1.EventTypeNormal, "Reassigned", msg)
}

// verify polls the API until it reports instanceID as the holder.
func (c *Controller) verify(ctx context.Context, instanceID string) error {
	deadline := c.now().Add(c.cfg.VerifyTimeout)
	for {
		if err := c.refreshVIP(ctx); err == nil && c.vip.InstanceID() == instanceID {
			return nil
		}
		if !c.now().Before(deadline) {
			return fmt.Errorf("API still reports instance %q after %s", c.vip.InstanceID(), c.cfg.VerifyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.verifyEvery):
		}
	}
}

func (c *Controller) backoff() time.Duration {
	d := c.cfg.Interval
	for i := 1; i < c.failures && d < c.cfg.MaxBackoff; i++ {
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
