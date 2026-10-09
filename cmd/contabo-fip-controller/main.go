// Command contabo-fip-controller keeps a Contabo floating IP on a Kubernetes
// node where the watched LoadBalancer Services have ready local endpoints.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/korkin25/contabo-fip-controller/internal/contabo"
	"github.com/korkin25/contabo-fip-controller/internal/controller"
	"github.com/korkin25/contabo-fip-controller/internal/health"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

// version is set at build time.
var version = "dev"

type serviceList []types.NamespacedName

func (s *serviceList) String() string {
	parts := make([]string, len(*s))
	for i, n := range *s {
		parts[i] = n.String()
	}
	return strings.Join(parts, ",")
}

// Set adds Services. A Service named twice is kept once: a duplicate would
// be probed twice per node and halve the effective --fall.
func (s *serviceList) Set(v string) error {
	for item := range strings.SplitSeq(v, ",") {
		ns, name, ok := strings.Cut(strings.TrimSpace(item), "/")
		if !ok || ns == "" || name == "" {
			return fmt.Errorf("want namespace/name, got %q", item)
		}
		n := types.NamespacedName{Namespace: ns, Name: name}
		if !slices.Contains(*s, n) {
			*s = append(*s, n)
		}
	}
	return nil
}

// validate checks the settings that flag parsing cannot.
func validate(cfg controller.Config, probeTimeout time.Duration, rise, fall int) error {
	if net.ParseIP(cfg.FloatingIP).To4() == nil {
		return errors.New("--floating-ip must be an IPv4 address")
	}
	if len(cfg.Services) == 0 {
		return errors.New("at least one --service is required")
	}
	if rise < 1 || fall < 1 {
		return errors.New("--rise and --fall must be at least 1")
	}
	for _, d := range []struct {
		flag string
		v    time.Duration
	}{
		{"--interval", cfg.Interval},
		{"--vip-refresh", cfg.VIPRefresh},
		{"--max-backoff", cfg.MaxBackoff},
		{"--verify-timeout", cfg.VerifyTimeout},
		{"--quarantine", cfg.Quarantine},
		{"--probe-timeout", probeTimeout},
	} {
		if d.v <= 0 {
			return fmt.Errorf("%s must be positive, got %s", d.flag, d.v)
		}
	}
	switch cfg.AddressType {
	case corev1.NodeHostName, corev1.NodeInternalIP, corev1.NodeExternalIP, corev1.NodeInternalDNS, corev1.NodeExternalDNS:
	default:
		return fmt.Errorf("--node-address-type must be one of Hostname, InternalIP, ExternalIP, InternalDNS, ExternalDNS; got %q", cfg.AddressType)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfg            controller.Config
		services       serviceList
		addressType    string
		probeTimeout   time.Duration
		rise, fall     int
		metricsAddr    string
		kubeconfig     string
		leaderElect    bool
		leaseName      string
		leaseNamespace string
		logLevel       string
	)
	flag.StringVar(&cfg.FloatingIP, "floating-ip", "", "Contabo floating IPv4 to manage (required)")
	flag.Var(&services, "service", "namespace/name of a LoadBalancer Service with externalTrafficPolicy Local; repeatable or comma-separated (required)")
	flag.StringVar(&cfg.InstanceLabel, "instance-label", "contabo.com/instance-id", "node label holding the Contabo instance ID")
	flag.StringVar(&addressType, "node-address-type", string(corev1.NodeInternalIP), "node address type to probe")
	flag.DurationVar(&cfg.Interval, "interval", 2*time.Second, "reconcile interval")
	flag.DurationVar(&cfg.VIPRefresh, "vip-refresh", 30*time.Second, "how often to re-read the floating IP holder from the API")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", time.Minute, "maximum delay between failed reassignments")
	flag.DurationVar(&cfg.VerifyTimeout, "verify-timeout", 15*time.Second, "how long to wait for the API to report a new holder")
	flag.DurationVar(&cfg.Quarantine, "quarantine", 5*time.Minute, fmt.Sprintf("how long an instance is skipped as a target after %d failed moves to it", controller.QuarantineAfter))
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "decide and report, but never call the API to reassign")
	flag.DurationVar(&probeTimeout, "probe-timeout", time.Second, "timeout of one health check probe")
	flag.IntVar(&rise, "rise", 2, "consecutive successful probes before a node becomes eligible")
	flag.IntVar(&fall, "fall", 2, "consecutive failed probes before a node stops being eligible")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "address for /metrics and /healthz")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; in-cluster config when empty")
	flag.BoolVar(&leaderElect, "leader-elect", true, "run only while holding the leader lease")
	flag.StringVar(&leaseName, "lease-name", "contabo-fip-controller", "leader election Lease name")
	flag.StringVar(&leaseNamespace, "lease-namespace", os.Getenv("POD_NAMESPACE"), "leader election Lease namespace")
	flag.StringVar(&logLevel, "log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	cfg.Services = services
	cfg.AddressType = corev1.NodeAddressType(addressType)
	if err := validate(cfg, probeTimeout, rise, fall); err != nil {
		return err
	}
	if leaderElect && leaseNamespace == "" {
		return errors.New("--lease-namespace or POD_NAMESPACE is required with --leader-elect")
	}
	creds, err := credentialsFromEnv()
	if err != nil {
		return err
	}

	restCfg, err := kubeConfig(kubeconfig)
	if err != nil {
		return err
	}
	restCfg.UserAgent = "contabo-fip-controller/" + version
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	// sigCtx ends on a signal or a failed HTTP server. The controller and
	// the leader election have their own contexts so that shutdown can run
	// in order: stop the loop, wait for it, then release the lease.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := controller.NewMetrics(reg)

	broadcaster := record.NewBroadcaster(record.WithContext(context.Background()))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: cs.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "contabo-fip-controller"})

	factory := informers.NewSharedInformerFactory(cs, 0)
	ctrl, err := controller.New(cfg, contabo.New(creds), health.NewProber(probeTimeout, rise, fall),
		factory.Core().V1().Nodes(), factory.Core().V1().Services(), recorder, metrics, log)
	if err != nil {
		return err
	}

	// Serve /metrics and /healthz before the caches sync, and fail the
	// process when the server cannot run.
	ln, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		return fmt.Errorf("metrics server: %w", err)
	}
	srv := &http.Server{Handler: httpMux(reg, ctrl.Healthy), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("metrics server: %w", err)
			stop()
		}
	}()
	defer srv.Shutdown(context.Background())
	result := func(err error) error {
		select {
		case serr := <-serveErr:
			return errors.Join(serr, err)
		default:
			return err
		}
	}

	// Followers keep warm caches so a takeover starts deciding at once.
	factory.Start(sigCtx.Done())
	defer factory.Shutdown()
	for typ, ok := range factory.WaitForCacheSync(sigCtx.Done()) {
		if !ok {
			if sigCtx.Err() != nil {
				return result(nil) // shut down before the caches synced
			}
			return result(fmt.Errorf("informer cache for %v did not sync", typ))
		}
	}

	log.Info("starting", "version", version, "floatingIP", cfg.FloatingIP, "services", services.String(), "dryRun", cfg.DryRun)
	gate := newRunGate()
	if !leaderElect {
		metrics.Leader.Set(1)
		gate.run(sigCtx, ctrl.Run)
		return result(nil)
	}
	identity := os.Getenv("POD_NAME")
	if identity == "" {
		if identity, err = os.Hostname(); err != nil {
			return err
		}
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: leaseNamespace},
		Client:     cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	leCtx, cancelLE := context.WithCancel(context.Background())
	defer cancelLE()
	go func() {
		<-sigCtx.Done()
		gate.stopAndWait()
		cancelLE() // only now may ReleaseOnCancel give the lease away
	}()
	lost := false
	leaderelection.RunOrDie(leCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Name:            leaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("became leader", "identity", identity)
				metrics.Leader.Set(1)
				gate.run(ctx, ctrl.Run)
			},
			OnStoppedLeading: func() {
				metrics.Leader.Set(0)
				lost = sigCtx.Err() == nil
			},
			OnNewLeader: func(id string) {
				if id != identity {
					log.Info("leader is " + id)
				}
			},
		},
	})
	// Do not exit under a move that is still in flight.
	gate.stopAndWait()
	if lost {
		// Restart with clean loop state rather than resume a stale view.
		return result(errors.New("lost leader lease"))
	}
	return result(nil)
}

// runGate runs the controller loop under a context that a shutdown can
// cancel, and lets the shutdown wait until the loop has returned.
type runGate struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

func newRunGate() *runGate {
	ctx, cancel := context.WithCancel(context.Background())
	return &runGate{ctx: ctx, cancel: cancel}
}

// run calls fn with a context that ends with parent or with stopAndWait,
// unless stopAndWait has already been called.
func (g *runGate) run(parent context.Context, fn func(context.Context)) {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.wg.Add(1)
	g.mu.Unlock()
	defer g.wg.Done()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer context.AfterFunc(g.ctx, cancel)()
	fn(ctx)
}

// stopAndWait cancels the running loop, prevents new ones and waits until
// the running one has returned.
func (g *runGate) stopAndWait() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
	g.cancel()
	g.wg.Wait()
}

func credentialsFromEnv() (contabo.Credentials, error) {
	var missing []string
	get := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}
	c := contabo.Credentials{
		ClientID:     get("CONTABO_CLIENT_ID"),
		ClientSecret: get("CONTABO_CLIENT_SECRET"),
		Username:     get("CONTABO_API_USER"),
		Password:     get("CONTABO_API_PASSWORD"),
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing environment variables: %s", strings.Join(missing, ", "))
	}
	return c, nil
}

func kubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

// httpMux serves /metrics and /healthz. /healthz answers 500 while healthy
// returns an error, i.e. on a leader whose loop stopped succeeding.
func httpMux(reg *prometheus.Registry, healthy func() error) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := healthy(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
