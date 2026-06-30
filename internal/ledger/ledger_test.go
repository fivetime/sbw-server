package ledger

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// A local etcd is started once for the whole package (TestMain). If the etcd
// binary is absent (e.g. a CI without it), every test skips — the ledger is
// infra-dependent, like the VPP/BIRD integration suites.
var testEtcdAddr string

func TestMain(m *testing.M) {
	bin, err := exec.LookPath("etcd")
	if err != nil {
		os.Exit(m.Run()) // no etcd → setup() skips
	}
	dir, err := os.MkdirTemp("", "etcd-ledger-*")
	if err != nil {
		panic(err)
	}
	addr := "127.0.0.1:23790"
	peer := "http://127.0.0.1:23791"
	cmd := exec.Command(bin,
		"--data-dir", dir,
		"--listen-client-urls", "http://"+addr,
		"--advertise-client-urls", "http://"+addr,
		"--listen-peer-urls", peer,
		"--initial-advertise-peer-urls", peer,
		"--initial-cluster", "default="+peer,
		"--log-level", "error",
	)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		os.Exit(m.Run())
	}
	// Wait until it answers.
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 2 * time.Second})
	ready := false
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := cli.Get(ctx, "ping")
		cancel()
		if err == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cli.Close()
	if ready {
		testEtcdAddr = addr
	}
	code := m.Run()
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func setup(t *testing.T) (*Ledger, *clock, context.Context) {
	t.Helper()
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	prefix := "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/"
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	return New(cli, prefix, 30*time.Second, WithClock(clk.now)), clk, ctx
}

func remaining(t *testing.T, l *Ledger, ctx context.Context, agent string) int64 {
	t.Helper()
	v, err := l.Remaining(ctx, agent)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	return v
}

// putResv seeds a reservation record directly (the reserve path now lives in the
// Yugabyte store; the ledger only owns the balance accounting + hung-token
// Reclaim sweep, so tests stage reservations via etcd rather than a Reserve call).
// It debits the agent's shard `shard` so the balance reflects the outstanding
// reservation, mirroring what a real reserve would have done.
func putResv(t *testing.T, l *Ledger, ctx context.Context, id string, r Reservation) {
	t.Helper()
	bal, err := l.Remaining(ctx, r.Agent)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	if bal < r.Amount {
		t.Fatalf("seed reservation %s: balance %d < amount %d", id, bal, r.Amount)
	}
	shardKey := l.tokShard(r.Agent, r.Shard)
	resvKey := l.resvPrefix() + id
	val, _ := json.Marshal(r)
	gr, err := l.kv.Get(ctx, shardKey)
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	var cur int64
	if len(gr.Kvs) > 0 {
		cur, err = strconv.ParseInt(string(gr.Kvs[0].Value), 10, 64)
		if err != nil {
			t.Fatalf("parse shard balance: %v", err)
		}
	}
	if cur < r.Amount {
		t.Fatalf("seed reservation %s: shard %d balance %d < amount %d", id, r.Shard, cur, r.Amount)
	}
	if _, err := l.kv.Txn(ctx).Then(
		clientv3.OpPut(shardKey, strconv.FormatInt(cur-r.Amount, 10)),
		clientv3.OpPut(resvKey, string(val)),
	).Commit(); err != nil {
		t.Fatalf("seed reservation %s: %v", id, err)
	}
}

func TestInitAgentOnceAndRemaining(t *testing.T) {
	l, _, ctx := setup(t)
	ok, err := l.InitAgent(ctx, "edge-2", 1000)
	if err != nil || !ok {
		t.Fatalf("first init should succeed: ok=%v err=%v", ok, err)
	}
	if got := remaining(t, l, ctx, "edge-2"); got != 1000 {
		t.Errorf("remaining = %d, want 1000", got)
	}
	// Re-register must NOT reset a balance that may have allocations.
	ok, _ = l.InitAgent(ctx, "edge-2", 500)
	if ok {
		t.Error("re-init should be a no-op, not reset the balance")
	}
	if got := remaining(t, l, ctx, "edge-2"); got != 1000 {
		t.Errorf("remaining after re-init = %d, want 1000 (not reset)", got)
	}
}

func TestReclaimHungTokensOnly(t *testing.T) {
	l, clk, ctx := setup(t)
	_, _ = l.InitAgent(ctx, "edge-2", 1000)
	now := clk.now()
	// One hung (reserved, will expire) and one committed reservation. Amounts are
	// kept within a single shard's slice (1000/64 → shard 0 = 55, others = 15).
	putResv(t, l, ctx, "hung", Reservation{
		Agent: "edge-2", Amount: 10, State: StateReserved, Pool: "p1",
		CreatedAt: now.UnixMilli(), ExpireAt: now.Add(30 * time.Second).UnixMilli(), Shard: 0,
	})
	putResv(t, l, ctx, "live", Reservation{
		Agent: "edge-2", Amount: 7, State: State("committed"), Pool: "p2",
		CreatedAt: now.UnixMilli(), ExpireAt: now.Add(30 * time.Second).UnixMilli(), Shard: 1,
	})

	if n, _ := l.Reclaim(ctx); n != 0 {
		t.Errorf("nothing reclaimable before expiry, got %d", n)
	}
	clk.advance(time.Minute)
	if n, _ := l.Reclaim(ctx); n != 1 {
		t.Errorf("only the hung reservation should reclaim, got %d", n)
	}
	if got := remaining(t, l, ctx, "edge-2"); got != 993 {
		t.Errorf("remaining = %d, want 993 (hung 10 returned, live 7 spent)", got)
	}
	if n, _ := l.Reclaim(ctx); n != 0 {
		t.Errorf("second reclaim should find nothing, got %d", n)
	}
}

func TestReclaimRefundsOriginalShard(t *testing.T) {
	l, clk, ctx := setup(t)
	_, _ = l.InitAgent(ctx, "edge-7", 1000)
	now := clk.now()
	putResv(t, l, ctx, "r1", Reservation{
		Agent: "edge-7", Amount: 5, State: StateReserved, Pool: "p1",
		CreatedAt: now.UnixMilli(), ExpireAt: now.Add(30 * time.Second).UnixMilli(), Shard: 3,
	})
	if got := remaining(t, l, ctx, "edge-7"); got != 995 {
		t.Fatalf("after seeding reservation, remaining = %d, want 995", got)
	}
	clk.advance(time.Minute)
	if n, _ := l.Reclaim(ctx); n != 1 {
		t.Fatalf("expired reservation should reclaim, got %d", n)
	}
	if got := remaining(t, l, ctx, "edge-7"); got != 1000 {
		t.Errorf("reclaim should restore full balance, remaining = %d, want 1000", got)
	}
}
