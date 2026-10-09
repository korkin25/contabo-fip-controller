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
	"strings"
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

func (s *serviceList) Set(v string) error {
	for item := range strings.SplitSeq(v, ",") {
		ns, name, ok := strings.Cut(strings.TrimSpace(item), "/")
		if !ok || ns == "" || name == "" {
			return fmt.Errorf("want namespace/name, got %q", item)
		}
		*s = append(*s, types.NamespacedName{Namespace: ns, Name: name})
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
	if net.ParseIP(cfg.FloatingIP).To4() == nil {
		return errors.New("--floating-ip must be an IPv4 address")
	}
	if len(cfg.Services) == 0 {
		return errors.New("at least one --service is required")
	}
	if rise < 1 || fall < 1 {
		return errors.New("--rise and --fall must be at least 1")
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := controller.NewMetrics(reg)

	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: cs.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "contabo-fip-controller"})

	factory := informers.NewSharedInformerFactory(cs, 0)
	ctrl, err := controller.New(cfg, contabo.New(creds), health.NewProber(probeTimeout, rise, fall),
		factory.Core().V1().Nodes(), factory.Core().V1().Services(), recorder, metrics, log)
	if err != nil {
		return err
	}
	// Followers keep warm caches so a takeover starts deciding at once.
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}

	srv := &http.Server{Addr: metricsAddr, Handler: httpMux(reg), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
			stop()
		}
	}()
	defer srv.Shutdown(context.Background())

	log.Info("starting", "version", version, "floatingIP", cfg.FloatingIP, "services", services.String(), "dryRun", cfg.DryRun)
	if !leaderElect {
		metrics.Leader.Set(1)
		ctrl.Run(ctx)
		return nil
	}
	if leaseNamespace == "" {
		return errors.New("--lease-namespace or POD_NAMESPACE is required with --leader-elect")
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
	lost := false
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
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
				ctrl.Run(ctx)
			},
			OnStoppedLeading: func() {
				metrics.Leader.Set(0)
				lost = ctx.Err() == nil
			},
			OnNewLeader: func(id string) {
				if id != identity {
					log.Info("leader is " + id)
				}
			},
		},
	})
	if lost {
		// Restart with clean loop state rather than resume a stale view.
		return errors.New("lost leader lease")
	}
	return nil
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

func httpMux(reg *prometheus.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
