package server

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/fivetime/sbw-contract/config"
	"github.com/fivetime/sbw-contract/logx"
)

// ServerConfig is the sbw-server (GLOBAL BRAIN) runtime configuration. It is the
// SERVER HALF of the monolith controller.Config: it keeps the YugabyteDB/etcd/
// Redpanda backends, placement inputs (EdgeAddrs/Replicas/HomeMarker), the failover
// quorum knobs (Sharding), and the gRPC listen address coverers dial
// (ServerCovererListenAddr). It DROPS the whole BGPConfig (asn/router_id/peers/canary/
// bfd — the RIB tap is the COVERER's), GRPCListenAddr (that served agents — also the
// coverer's), and Sharding.Enabled/GRPCEndpoint/ReconcileInterval (the server is
// ALWAYS the HA brain; coverer membership/routing lives on the coverers).
type ServerConfig struct {
	Log  logx.Config `json:"log"`
	Etcd EtcdConfig  `json:"etcd"`

	// Yugabyte holds the YSQL DSN for the MANDATORY bulk pool/member store. An empty
	// DSN is FATAL at startup (there is no all-etcd retreat).
	Yugabyte YugabyteConfig `json:"yugabyte"`

	// ServerCovererListenAddr is where the server serves the rpc.ServerCoverer gRPC
	// service — the address the sbw-coverer processes dial. Replaces the monolith's
	// GRPCListenAddr (which served agents; that is the coverer's job now).
	ServerCovererListenAddr string `json:"server_coverer_listen_addr"`

	// MetricsListenAddr serves Prometheus /metrics. Empty disables it.
	MetricsListenAddr string `json:"metrics_listen_addr"`
	// AdminListenAddr serves the management/ingestion HTTP API (pool CRUD, agent
	// decommission). Empty disables it.
	AdminListenAddr string `json:"admin_listen_addr"`

	// Replicas is the number of homes per pool (§4.1): 2 = primary + backup. 0 → 2.
	Replicas int `json:"replicas"`

	// EdgeAddrs / EdgeAddrs6 map each edge id to its v4 / v6 redirect next-hop (the
	// egress FlowSpec render input). Values must be IP literals.
	EdgeAddrs  map[string]string `json:"edge_addrs"`
	EdgeAddrs6 map[string]string `json:"edge_addrs6"`

	// HomeMarker is the home-anchor marker large community (§4.7/T-703). The render
	// runs on the server so the marker config stays here (the monolith sourced its
	// default GlobalAdmin from the BGP ASN, which the server no longer has — so a
	// default is supplied directly).
	HomeMarker HomeMarkerConfig `json:"home_marker"`

	// DriftSweepInterval is how often the report-hash drift backstop recomputes each
	// reporting edge's expected pool-set. 0 → 30s.
	DriftSweepInterval config.Duration `json:"drift_sweep_interval"`

	// ReplicaID is this server replica's stable id (the deathvote/liveness self id and
	// the etcd membership/coverage hash key the server uses to read coverer routing).
	// Empty → hostname-derived by the cmd is acceptable; here it is taken verbatim.
	ReplicaID string `json:"replica_id"`

	// Sharding carries the K=2 HA brain knobs the server always runs with (the server
	// is ALWAYS the HA brain — there is no Enabled toggle).
	Sharding ShardingConfig `json:"sharding"`

	// RedpandaBrokers are the bootstrap brokers for the ASYNC API-result event stream.
	// EMPTY ⇒ the feature is DISABLED (a Noop emitter is wired).
	RedpandaBrokers []string `json:"redpanda_brokers"`
	// APIResultsTopic is the topic the API-result events are produced to. Empty →
	// "sbw.api.results".
	APIResultsTopic string `json:"api_results_topic"`
}

const (
	defaultAPIResultsTopic         = "sbw.api.results"
	defaultServerCovererListenAddr = ":1792"
)

// EtcdConfig points at the etcd cluster holding COORDINATION state (registry, token
// ledger, sharding/coverage, liveness, edgever). The bulk pool/member DATA lives in
// YugabyteDB.
type EtcdConfig struct {
	Endpoints      []string        `json:"endpoints"`
	Prefix         string          `json:"prefix"`
	ReservationTTL config.Duration `json:"reservation_ttl"`
}

// YugabyteConfig points at the YugabyteDB YSQL endpoint holding the bulk pool +
// member DATA. Yugabyte is MANDATORY — no all-etcd retreat.
type YugabyteConfig struct {
	DSN        string          `json:"dsn"`
	CapRefresh config.Duration `json:"cap_refresh"`
}

// HomeMarkerConfig parameterizes the home-anchor marker large community
// (§4.7/T-703). Enabled stamps `GlobalAdmin:LocalData1:<edge>` on a home's anchors.
type HomeMarkerConfig struct {
	Enabled     bool   `json:"enabled"`
	GlobalAdmin uint32 `json:"global_admin"` // 0 → defaultHomeMarkerGlobalAdmin
	LocalData1  uint32 `json:"local_data1"`  // 0 → 101
}

// defaultHomeMarkerGlobalAdmin is the marker's GlobalAdmin when none is configured
// (the server has no BGP ASN to derive it from). Matches the monolith ASN default.
const defaultHomeMarkerGlobalAdmin = 65010

// ShardingConfig carries the K=2 HA brain knobs (DESIGN-liveness §8, L-03/L-05). The
// server is ALWAYS the HA brain, so unlike the monolith there is no Enabled toggle.
type ShardingConfig struct {
	// K is the number of coverers covering each edge (redundancy). 0 → 2.
	K int `json:"k"`
	// FailoverQuorum is how many of an edge's K coverers must observe its session down
	// before HARD-death failover fires (L-03). 0 → K. Clamped to [1, K].
	FailoverQuorum int `json:"failover_quorum"`
	// LeaseTTL is the etcd lease for the deathvote/coverer-membership reads. 0 → 10s.
	LeaseTTL config.Duration `json:"lease_ttl"`
	// HardDebounce is a hold-down the HARD-death quorum must persist before failover,
	// damping a recovering edge whose tap flaps. 0 → 3s.
	HardDebounce config.Duration `json:"hard_debounce"`
}

// WithDefaults returns the sharding config with zero fields filled in.
func (s ShardingConfig) WithDefaults() ShardingConfig {
	if s.K == 0 {
		s.K = 2
	}
	if s.FailoverQuorum == 0 {
		s.FailoverQuorum = s.K // unanimous by default
	}
	if s.FailoverQuorum < 1 {
		s.FailoverQuorum = 1
	}
	if s.FailoverQuorum > s.K {
		s.FailoverQuorum = s.K
	}
	if s.LeaseTTL == 0 {
		s.LeaseTTL = config.Duration(10 * time.Second)
	}
	if s.HardDebounce == 0 {
		s.HardDebounce = config.Duration(3 * time.Second)
	}
	return s
}

// ResolveReplicaID returns the server replica's id: the explicit replicaID, else a
// fixed local key (fine for a single-replica deployment).
func (s ShardingConfig) ResolveReplicaID(replicaID string) string {
	if replicaID != "" {
		return replicaID
	}
	return "server"
}

// DefaultConfig returns the sbw-server defaults.
func DefaultConfig() ServerConfig {
	return ServerConfig{
		Log: logx.Config{Level: "info", Format: logx.FormatJSON},
		Etcd: EtcdConfig{
			Endpoints:      []string{"127.0.0.1:2379"},
			Prefix:         "sbw/",
			ReservationTTL: config.Duration(30 * time.Second),
		},
		Yugabyte: YugabyteConfig{
			DSN:        "", // mandatory; FATAL if empty (no hardcoded-lab-DB retreat)
			CapRefresh: config.Duration(5 * time.Second),
		},
		ServerCovererListenAddr: defaultServerCovererListenAddr,
		MetricsListenAddr:       ":9101",
		AdminListenAddr:         ":8080",
		DriftSweepInterval:      config.Duration(30 * time.Second),
		APIResultsTopic:         defaultAPIResultsTopic,
	}
}

// LoadConfig builds the server config: defaults → optional JSON file → env overrides
// → validation. It always returns a defaults-populated config (so the caller can
// still build a logger) alongside any error.
func LoadConfig(path string) (ServerConfig, error) {
	cfg := DefaultConfig()
	if err := config.LoadFile(path, &cfg); err != nil {
		return cfg, err
	}
	if err := cfg.applyEnv(); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *ServerConfig) applyEnv() error {
	var err error
	c.Log.Level = config.String("LOG_LEVEL", c.Log.Level)
	c.Log.Format = logx.Format(config.String("LOG_FORMAT", string(c.Log.Format)))

	c.Etcd.Endpoints = config.Strings("ETCD_ENDPOINTS", c.Etcd.Endpoints)
	c.Etcd.Prefix = config.String("ETCD_PREFIX", c.Etcd.Prefix)
	if c.Etcd.ReservationTTL, err = config.DurationEnv("ETCD_RESERVATION_TTL", c.Etcd.ReservationTTL); err != nil {
		return err
	}

	c.Yugabyte.DSN = config.String("YB_DSN", c.Yugabyte.DSN)
	if c.Yugabyte.CapRefresh, err = config.DurationEnv("YB_CAP_REFRESH", c.Yugabyte.CapRefresh); err != nil {
		return err
	}
	if c.Yugabyte.CapRefresh == 0 {
		c.Yugabyte.CapRefresh = config.Duration(5 * time.Second)
	}

	c.ServerCovererListenAddr = config.String("SERVER_COVERER_LISTEN_ADDR", c.ServerCovererListenAddr)
	c.MetricsListenAddr = config.String("METRICS_LISTEN_ADDR", c.MetricsListenAddr)
	c.AdminListenAddr = config.String("ADMIN_LISTEN_ADDR", c.AdminListenAddr)

	if c.Replicas, err = config.Int("REPLICAS", c.Replicas); err != nil {
		return err
	}
	c.ReplicaID = config.String("REPLICA_ID", c.ReplicaID)

	if c.HomeMarker.Enabled, err = config.Bool("HOME_MARKER_ENABLED", c.HomeMarker.Enabled); err != nil {
		return err
	}
	if c.HomeMarker.GlobalAdmin, err = config.Uint32("HOME_MARKER_GLOBAL_ADMIN", c.HomeMarker.GlobalAdmin); err != nil {
		return err
	}
	if c.HomeMarker.GlobalAdmin == 0 {
		c.HomeMarker.GlobalAdmin = defaultHomeMarkerGlobalAdmin // 0 ⇒ the default marker GlobalAdmin
	}
	if c.HomeMarker.LocalData1, err = config.Uint32("HOME_MARKER_LOCAL_DATA1", c.HomeMarker.LocalData1); err != nil {
		return err
	}

	if c.Sharding.K, err = config.Int("SHARDING_K", c.Sharding.K); err != nil {
		return err
	}
	if c.Sharding.FailoverQuorum, err = config.Int("SHARDING_FAILOVER_QUORUM", c.Sharding.FailoverQuorum); err != nil {
		return err
	}
	if c.Sharding.LeaseTTL, err = config.DurationEnv("SHARDING_LEASE_TTL", c.Sharding.LeaseTTL); err != nil {
		return err
	}
	if c.Sharding.HardDebounce, err = config.DurationEnv("SHARDING_HARD_DEBOUNCE", c.Sharding.HardDebounce); err != nil {
		return err
	}

	c.RedpandaBrokers = config.Strings("REDPANDA_BROKERS", c.RedpandaBrokers)
	c.APIResultsTopic = config.String("API_RESULTS_TOPIC", c.APIResultsTopic)
	if c.APIResultsTopic == "" {
		c.APIResultsTopic = defaultAPIResultsTopic
	}
	if c.DriftSweepInterval, err = config.DurationEnv("DRIFT_SWEEP_INTERVAL", c.DriftSweepInterval); err != nil {
		return err
	}
	if c.DriftSweepInterval == 0 {
		c.DriftSweepInterval = config.Duration(30 * time.Second)
	}
	return nil
}

// Validate checks the server config for startup-blocking errors. The DSN-empty case
// is left FATAL in main (Yugabyte mandatory) so the logger is already built when it
// is reported; here we validate everything else.
func (c ServerConfig) Validate() error {
	if len(c.Etcd.Endpoints) == 0 {
		return fmt.Errorf("server config: etcd.endpoints must be set")
	}
	if c.ServerCovererListenAddr == "" {
		return fmt.Errorf("server config: server_coverer_listen_addr must be set")
	}
	for edge, addr := range c.EdgeAddrs {
		if a, err := netip.ParseAddr(addr); err != nil || !a.Is4() {
			return fmt.Errorf("server config: edge_addrs[%s] = %q must be a valid IPv4 address", edge, addr)
		}
	}
	for edge, addr := range c.EdgeAddrs6 {
		if a, err := netip.ParseAddr(addr); err != nil || !a.Is6() {
			return fmt.Errorf("server config: edge_addrs6[%s] = %q must be a valid IPv6 address", edge, addr)
		}
	}
	if c.Sharding.K < 0 {
		return fmt.Errorf("server config: sharding.k must be >= 0, got %d", c.Sharding.K)
	}
	return nil
}
