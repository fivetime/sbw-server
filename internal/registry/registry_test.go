package registry

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Local etcd for the package (distinct port from other etcd test suites so
// `go test ./...` can run them in parallel). Skips if etcd is absent.
var testEtcdAddr string

func TestMain(m *testing.M) {
	bin, err := exec.LookPath("etcd")
	if err != nil {
		os.Exit(m.Run())
	}
	dir, _ := os.MkdirTemp("", "etcd-registry-*")
	addr := "127.0.0.1:23792"
	peer := "http://127.0.0.1:23793"
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

type fclock struct{ t time.Time }

func (c *fclock) now() time.Time { return c.t }

func setup(t *testing.T) (*Registry, *fclock, context.Context) {
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
	clk := &fclock{t: time.Unix(1_700_000_000, 0)}
	return New(cli, prefix, WithClock(clk.now)), clk, ctx
}

func TestRegisterIdempotentPreservesFirstSeen(t *testing.T) {
	r, clk, ctx := setup(t)
	if err := r.Register(ctx, "edge-2", 100_000_000_000); err != nil {
		t.Fatal(err)
	}
	first, ok, _ := r.Get(ctx, "edge-2")
	if !ok || first.CapacityBps != 100_000_000_000 {
		t.Fatalf("get after register: %+v ok=%v", first, ok)
	}
	firstSeen := first.RegisteredAt

	// Re-register later with a new capacity → RegisteredAt preserved, capacity refreshed.
	clk.t = clk.t.Add(time.Hour)
	if err := r.Register(ctx, "edge-2", 40_000_000_000); err != nil {
		t.Fatal(err)
	}
	again, _, _ := r.Get(ctx, "edge-2")
	if again.RegisteredAt != firstSeen {
		t.Errorf("re-register changed RegisteredAt %d → %d", firstSeen, again.RegisteredAt)
	}
	if again.CapacityBps != 40_000_000_000 {
		t.Errorf("capacity not refreshed: %d", again.CapacityBps)
	}
}

func TestListSortedAndDeregister(t *testing.T) {
	r, _, ctx := setup(t)
	for _, e := range []model.EdgeID{"edge-5", "edge-2", "edge-9"} {
		if err := r.Register(ctx, e, 10_000_000_000); err != nil {
			t.Fatal(err)
		}
	}
	list, err := r.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].EdgeID != "edge-2" || list[1].EdgeID != "edge-5" || list[2].EdgeID != "edge-9" {
		t.Errorf("list not sorted: %v", list)
	}

	if err := r.Deregister(ctx, "edge-5"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := r.Get(ctx, "edge-5"); ok {
		t.Error("deregistered agent should be gone")
	}
	list, _ = r.List(ctx)
	if len(list) != 2 {
		t.Errorf("after deregister want 2, got %d", len(list))
	}
}
