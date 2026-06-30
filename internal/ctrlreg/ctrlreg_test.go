package ctrlreg

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var testEtcdAddr string

func TestMain(m *testing.M) {
	bin, err := exec.LookPath("etcd")
	if err != nil {
		os.Exit(m.Run())
	}
	dir, _ := os.MkdirTemp("", "etcd-ctrlreg-*")
	addr := "127.0.0.1:23798"
	peer := "http://127.0.0.1:23799"
	cmd := exec.Command(bin, "--data-dir", dir,
		"--listen-client-urls", "http://"+addr, "--advertise-client-urls", "http://"+addr,
		"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
		"--initial-cluster", "default="+peer, "--log-level", "error")
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		os.Exit(m.Run())
	}
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 2 * time.Second})
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, e := cli.Get(ctx, "ping")
		cancel()
		if e == nil {
			testEtcdAddr = addr
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cli.Close()
	code := m.Run()
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func newClient(t *testing.T) *clientv3.Client {
	t.Helper()
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func setup(t *testing.T, ttl time.Duration) (*Registry, *clientv3.Client, string, context.Context) {
	cli := newClient(t)
	prefix := "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/"
	ctx := context.Background()
	t.Cleanup(func() { _, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix()) })
	return New(cli, prefix, ttl), cli, prefix, ctx
}

func ctrl(id string) Controller { return Controller{ID: id, GRPCEndpoint: id + ":1791"} }

func TestJoinListIDs(t *testing.T) {
	r, _, _, ctx := setup(t, 30*time.Second)
	m1, err := r.Join(ctx, ctrl("ctrl-b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m1.Close(ctx) })
	m2, err := r.Join(ctx, ctrl("ctrl-a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close(ctx) })

	ids, err := r.IDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "ctrl-a" || ids[1] != "ctrl-b" {
		t.Fatalf("IDs = %v, want sorted [ctrl-a ctrl-b]", ids)
	}
	cs, _ := r.List(ctx)
	if cs[0].GRPCEndpoint != "ctrl-a:1791" || cs[0].RegisteredAt == 0 {
		t.Errorf("List did not carry endpoint/timestamp: %+v", cs[0])
	}
}

func TestCloseLeaves(t *testing.T) {
	r, _, _, ctx := setup(t, 30*time.Second)
	m1, _ := r.Join(ctx, ctrl("ctrl-1"))
	m2, _ := r.Join(ctx, ctrl("ctrl-2"))
	t.Cleanup(func() { _ = m2.Close(ctx) })

	if ids, _ := r.IDs(ctx); len(ids) != 2 {
		t.Fatalf("want 2 before close, got %v", ids)
	}
	if err := m1.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if ids, _ := r.IDs(ctx); len(ids) != 1 || ids[0] != "ctrl-2" {
		t.Errorf("after graceful close want [ctrl-2], got %v", ids)
	}
}

// TestCrashExpiryRemoves: a controller that dies (no graceful close, keep-alive
// stops because its client is gone) drops out after the lease TTL — the HA
// property the shard reshuffle relies on.
func TestCrashExpiryRemoves(t *testing.T) {
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	r, _, _, ctx := setup(t, 2*time.Second) // short TTL

	// A separate client = the "crashing" replica; closing it kills keep-alive.
	dyingCli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	dyingReg := New(dyingCli, r.prefix, 2*time.Second)
	if _, err := dyingReg.Join(ctx, ctrl("ctrl-dying")); err != nil {
		t.Fatal(err)
	}
	if ids, _ := r.IDs(ctx); len(ids) != 1 {
		t.Fatalf("want 1 after join, got %v", ids)
	}

	dyingCli.Close() // simulate crash: keep-alive stops, lease no longer renewed
	// Wait past the TTL for etcd to expire the lease.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if ids, _ := r.IDs(ctx); len(ids) == 0 {
			return // expired — good
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("crashed controller did not expire from membership within 8s")
}

func TestWatchStreamsChanges(t *testing.T) {
	r, _, _, ctx := setup(t, 30*time.Second)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch, err := r.Watch(wctx)
	if err != nil {
		t.Fatal(err)
	}
	// Initial snapshot (empty).
	if snap := <-ch; len(snap) != 0 {
		t.Fatalf("initial snapshot want empty, got %v", snap)
	}
	m, _ := r.Join(ctx, ctrl("ctrl-x"))
	t.Cleanup(func() { _ = m.Close(ctx) })

	// Expect a change snapshot containing ctrl-x.
	select {
	case snap := <-ch:
		if len(snap) != 1 || snap[0].ID != "ctrl-x" {
			t.Errorf("watch change want [ctrl-x], got %v", snap)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not deliver the join within 3s")
	}
}
