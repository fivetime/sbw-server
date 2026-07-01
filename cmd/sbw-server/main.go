// Command sbw-server is the SBW control plane's GLOBAL BRAIN (DESIGN-server-coverer-
// split §8): the sole owner of the YugabyteDB/etcd/Redpanda connections, it does
// placement/装箱, the failover DECISION, and the BSS admin API, and serves coverers the
// rpc.ServerCoverer contract (Watch fanned to each covering coverer, Report aggregated
// into the global view, Register relayed). It does NOT run the RIB tap or the agent-
// facing AgentService — those are the coverer's.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/fivetime/sbw-contract/buildinfo"
	"github.com/fivetime/sbw-contract/logx"
	"github.com/fivetime/sbw-contract/metrics"
	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/fivetime/sbw-server/internal/apiresult"
	"github.com/fivetime/sbw-server/internal/deathvote"
	"github.com/fivetime/sbw-server/internal/edgever"
	"github.com/fivetime/sbw-server/internal/server"
	"github.com/fivetime/sbw-server/internal/ybstore"
)

func main() {
	cfgPath := flag.String("config", "", "path to JSON config file (optional; env overrides apply)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}

	cfg, cfgErr := server.LoadConfig(*cfgPath)

	log, err := logx.New(cfg.Log, os.Stderr)
	if err != nil {
		log = logx.Default()
		log.Warn("invalid log config; falling back to defaults", "err", err)
	}
	if cfgErr != nil {
		log.Error("configuration error", "err", cfgErr)
		os.Exit(1)
	}

	log.Info("sbw-server starting",
		"version", buildinfo.Version,
		"component", "server",
		"etcd", cfg.Etcd.Endpoints,
		"server_coverer_listen", cfg.ServerCovererListenAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// etcd client — the shared strong-consistency store for every replica (§1.2).
	cli, err := clientv3.New(clientv3.Config{Endpoints: cfg.Etcd.Endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		log.Error("etcd connect failed", "err", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	edgeAddrs := make(map[model.EdgeID]netip.Addr, len(cfg.EdgeAddrs))
	for edge, addr := range cfg.EdgeAddrs {
		edgeAddrs[model.EdgeID(edge)] = netip.MustParseAddr(addr) // validated in Config.Validate
	}
	edgeAddrs6 := make(map[model.EdgeID]netip.Addr, len(cfg.EdgeAddrs6))
	for edge, addr := range cfg.EdgeAddrs6 {
		edgeAddrs6[model.EdgeID(edge)] = netip.MustParseAddr(addr) // validated in Config.Validate
	}

	// The server is ALWAYS the HA brain: K=2 coverage + corroborated failover quorum.
	sh := cfg.Sharding.WithDefaults()
	selfID := sh.ResolveReplicaID(cfg.ReplicaID)

	met := metrics.New()
	cp := server.NewControlPlane(cli, server.CPOptions{
		Prefix:               cfg.Etcd.Prefix,
		ReservationTTL:       time.Duration(cfg.Etcd.ReservationTTL),
		Replicas:             cfg.Replicas,
		EdgeAddrs:            edgeAddrs,
		EdgeAddrs6:           edgeAddrs6,
		LivenessQuorum:       sh.FailoverQuorum,
		LivenessHardDebounce: time.Duration(sh.HardDebounce),
		CoverageK:            sh.K,
		SelfID:               selfID,
		HomeMarker:           homeMarkerFn(cfg.HomeMarker),
		Metrics:              met,
		Logger:               log,
	})

	// YugabyteDB is MANDATORY: pools/members live in YSQL and there is NO retreat to an
	// all-etcd path. BOTH an empty DSN and a failed Connect are FATAL.
	if cfg.Yugabyte.DSN == "" {
		log.Error("yugabyte is mandatory: no DSN configured (set YB_DSN / yugabyte.dsn)")
		os.Exit(1)
	}
	ybPool, err := ybstore.Connect(ctx, cfg.Yugabyte.DSN)
	if err != nil {
		log.Error("yugabyte is mandatory: connect failed", "err", err)
		os.Exit(1)
	}
	defer ybPool.Close()
	yb := ybstore.New(ybPool).WithLogger(log)
	capCache := ybstore.NewCapacityCache(yb, time.Duration(cfg.Yugabyte.CapRefresh)).WithLogger(log)
	cp.SetYBStore(yb, capCache)
	go capCache.Run(ctx)
	log.Info("yugabyte bulk store wired (pools+members in YSQL; etcd = coordination only)",
		"cap_refresh", time.Duration(cfg.Yugabyte.CapRefresh))

	// ASYNC API-result event stream (BSS correlation). EMPTY brokers ⇒ Noop emitter ⇒
	// NO events + NO behaviour change. A producer-build failure is non-fatal.
	if len(cfg.RedpandaBrokers) > 0 {
		prod, perr := apiresult.NewProducer(cfg.RedpandaBrokers, cfg.APIResultsTopic,
			apiresult.WithErrorLog(func(e error) { log.Warn("api-result produce failed", "err", e) }))
		if perr != nil {
			log.Warn("api-result emitter disabled (producer build failed; falling back to Noop)", "err", perr)
		} else {
			cp.SetAPIResultEmitter(prod)
			defer prod.Close()
			log.Info("api-result event stream wired (BSS async convergence correlation)",
				"brokers", cfg.RedpandaBrokers, "topic", cfg.APIResultsTopic)
		}
	} else {
		log.Info("api-result event stream disabled (no redpanda_brokers configured)")
	}

	// HA wiring (always on for the server). Per-edge desired/applied versions (L-07)
	// decouple the failover DECISION from cross-replica DELIVERY (the converge loop);
	// the death-vote bridge corroborates K coverers' votes across server replicas.
	ev := edgever.New(cli, cfg.Etcd.Prefix)
	cp.SetEdgeVer(ev)
	if !cp.HasEdgeVer() {
		log.Error("edgever store not wired (async failover would be silently dead)")
		os.Exit(1)
	}
	voter := deathvote.New(cli, cfg.Etcd.Prefix, selfID, time.Duration(sh.LeaseTTL))
	cp.SetVoter(voter)
	go func() {
		if err := cp.RunDeathVotes(ctx); err != nil && ctx.Err() == nil {
			log.Error("death-vote watcher stopped (fatal)", "err", err)
			os.Exit(1)
		}
	}()

	// COVERER ASSIGNMENT is wired in the ControlPlane constructor (covererFunc =
	// fan.assignmentFor): the agent's coverer set is the SAME connected-coverer HRW the
	// desired-state routing uses, so the agent homes to the exact coverer the server routes
	// its EDGE_DIRECTIVE through — no store, no ctrlreg join, no tap. The step-11
	// ctrlreg/etcd-backed assigner placeholder is gone (membership = connected Watch streams).

	// Prometheus /metrics.
	if cfg.MetricsListenAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", met.Handler())
		msrv := &http.Server{Addr: cfg.MetricsListenAddr, Handler: mux}
		go func() {
			if err := msrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("metrics server stopped", "err", err)
			}
		}()
		defer func() { _ = msrv.Close() }()
		go cp.RunMetricsRefresh(ctx, 15*time.Second)
		log.Info("metrics serving", "addr", cfg.MetricsListenAddr, "path", "/metrics")
	}

	// Management / BSS ingestion API.
	if cfg.AdminListenAddr != "" {
		asrv := &http.Server{Addr: cfg.AdminListenAddr, Handler: cp.AdminHandler()}
		go func() {
			if err := asrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("admin server stopped", "err", err)
			}
		}()
		defer func() { _ = asrv.Close() }()
		log.Info("admin API serving", "addr", cfg.AdminListenAddr)
	}

	// Reconcilers (§4.3/§5.9/§6.5/B-02/L-04/L-07/T-706).
	go cp.RunReclaim(ctx, 10*time.Second)
	go cp.RunLiveness(ctx, 5*time.Second)
	go cp.RunDriftSweep(ctx, time.Duration(cfg.DriftSweepInterval))
	go cp.RunReconcileAccounts(ctx, 30*time.Second)
	go cp.RunReconcileProgram(ctx, 30*time.Second)
	go cp.RunReconcileAnchors(ctx, 30*time.Second) // inert until MEMBER_EDGE feeds a physical view
	go cp.RunActionExpiry(ctx, 30*time.Second)
	go cp.RunResultTimeoutSweep(ctx, 5*time.Second, 60*time.Second)
	go func() {
		if err := cp.RunConverge(ctx); err != nil && ctx.Err() == nil {
			log.Error("converge loop stopped (fatal)", "err", err)
			os.Exit(1)
		}
	}()
	go cp.RunPoolReconcile(ctx, 5*time.Second)
	// COVERAGE recompute loop: re-emit each connected coverer's HRW covered-edge set
	// on connect/evict/first-register (debounced ~250ms to coalesce a fleet reconnect).
	go cp.RunCoverageRecompute(ctx, 250*time.Millisecond)

	// Serve the rpc.ServerCoverer gRPC contract — ONLY this service (NOT AgentService:
	// agents dial the coverer, not the server).
	//
	// Keepalive is load-bearing for total-restart recovery: the coverer's Watch is a long
	// server-stream that idles between pushes; after a mass-restart a coverer's ClientConn can
	// end up half-open (the Watch stream created but never truly connected — the server never
	// received it), and without keepalive the coverer blocks in stream.Recv() FOREVER (never
	// erroring, never retrying) so it stays wedged until a manual pod restart. Server pings
	// idle conns (KeepaliveParams) so a dead coverer is reaped; the EnforcementPolicy MUST
	// admit the coverer's client pings (MinTime <= client ping interval, PermitWithoutStream)
	// or the server GOAWAYs them.
	gs := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	rpc.RegisterServerCovererServer(gs, server.NewCovererServer(cp))
	cp.SetGRPCServer(gs)
	lis, err := net.Listen("tcp", cfg.ServerCovererListenAddr)
	if err != nil {
		log.Error("server-coverer listen failed", "addr", cfg.ServerCovererListenAddr, "err", err)
		os.Exit(1)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()

	log.Info("sbw-server running; coverers may connect. Send SIGTERM/SIGINT to stop.",
		"replica_id", selfID, "k", sh.K, "failover_quorum", sh.FailoverQuorum)
	select {
	case <-ctx.Done():
		log.Info("sbw-server received shutdown signal; stopping")
	case err := <-serveErr:
		if err != nil {
			log.Error("server-coverer gRPC server stopped", "err", err)
			os.Exit(1)
		}
	}
	cp.Stop() // evict coverer streams + bounded graceful stop of gs
}

// homeMarkerFn builds the per-edge home-marker large community from config (§4.7 /
// T-703 / C-04 backstop): a home edge's anchors carry `GlobalAdmin:LocalData1:<edge>`
// so MX204/RR can raise local-pref for the current home. Returns nil when disabled.
func homeMarkerFn(hm server.HomeMarkerConfig) func(model.EdgeID) (model.LargeCommunity, bool) {
	if !hm.Enabled {
		return nil
	}
	ga := hm.GlobalAdmin
	if ga == 0 {
		ga = 65010
	}
	ld1 := hm.LocalData1
	if ld1 == 0 {
		ld1 = 101
	}
	return func(e model.EdgeID) (model.LargeCommunity, bool) {
		return model.LargeCommunity{GlobalAdmin: ga, LocalData1: ld1, LocalData2: edgeNum(e)}, true
	}
}

// edgeNum maps an edge id to a stable small integer for the marker's LocalData2: the
// trailing decimal digits if present, else a 32-bit hash. Observability label only.
func edgeNum(e model.EdgeID) uint32 {
	s := string(e)
	i := len(s)
	for i > 0 && s[i-1] >= '0' && s[i-1] <= '9' {
		i--
	}
	if i < len(s) {
		if n, err := strconv.ParseUint(s[i:], 10, 32); err == nil {
			return uint32(n)
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
