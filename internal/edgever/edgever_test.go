package edgever

import (
	"context"
	"os"
	"os/exec"
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
	dir, _ := os.MkdirTemp("", "etcd-edgever-*")
	addr := "127.0.0.1:23802"
	peer := "http://127.0.0.1:23803"
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

func setup(t *testing.T) (*Store, *clientv3.Client, context.Context) {
	t.Helper()
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	prefix := "test/" + t.Name() + "/"
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
	return New(cli, prefix), cli, ctx
}

// Bump is strictly monotonic per edge; Desired reads back the latest; distinct
// edges are independent.
func TestBumpMonotonicAndIndependent(t *testing.T) {
	s, _, ctx := setup(t)
	for want := uint64(1); want <= 3; want++ {
		got, err := s.Bump(ctx, "l1")
		if err != nil || got != want {
			t.Fatalf("Bump l1 = %d (%v), want %d", got, err, want)
		}
	}
	if d, _ := s.Desired(ctx, "l1"); d != 3 {
		t.Fatalf("Desired l1 = %d, want 3", d)
	}
	if d, _ := s.Desired(ctx, "l2"); d != 0 {
		t.Fatalf("Desired l2 = %d, want 0 (never bumped)", d)
	}
	if v, _ := s.Bump(ctx, "l2"); v != 1 {
		t.Fatalf("Bump l2 = %d, want 1 (independent counter)", v)
	}
}

// Advance raises applied only upward; a lower/equal value is a no-op.
func TestAdvanceMonotonic(t *testing.T) {
	s, _, ctx := setup(t)
	if err := s.Advance(ctx, "l1", 5); err != nil {
		t.Fatal(err)
	}
	if a, _ := s.Applied(ctx, "l1"); a != 5 {
		t.Fatalf("Applied = %d, want 5", a)
	}
	if err := s.Advance(ctx, "l1", 3); err != nil { // stale/duplicate report
		t.Fatal(err)
	}
	if a, _ := s.Applied(ctx, "l1"); a != 5 {
		t.Fatalf("Applied regressed to %d, want 5 (Advance is monotonic)", a)
	}
	if err := s.Advance(ctx, "l1", 9); err != nil {
		t.Fatal(err)
	}
	if a, _ := s.Applied(ctx, "l1"); a != 9 {
		t.Fatalf("Applied = %d, want 9", a)
	}
}

// WatchDesired delivers the existing snapshot first, then live bumps.
func TestWatchDesiredSnapshotThenLive(t *testing.T) {
	s, _, ctx := setup(t)
	if _, err := s.Bump(ctx, "l1"); err != nil { // pre-existing → must appear in snapshot
		t.Fatal(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := s.WatchDesired(wctx)
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot event for l1.
	select {
	case ev := <-ch:
		if ev.Edge != "l1" || ev.Version != 1 {
			t.Fatalf("snapshot event = %+v, want l1/1", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no snapshot event")
	}
	// Live bump for l2.
	if _, err := s.Bump(ctx, "l2"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Edge != "l2" || ev.Version != 1 {
			t.Fatalf("live event = %+v, want l2/1", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no live event after bump")
	}
}
