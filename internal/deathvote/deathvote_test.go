package deathvote

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

var testEtcdAddr string

func TestMain(m *testing.M) {
	bin, err := exec.LookPath("etcd")
	if err != nil {
		os.Exit(m.Run())
	}
	dir, _ := os.MkdirTemp("", "etcd-deathvote-*")
	addr := "127.0.0.1:23806"
	peer := "http://127.0.0.1:23807"
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

func prefix(t *testing.T) string {
	return "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/"
}

// One voter's Down/Up is seen by another's Watch as Down then cleared.
func TestVotePropagates(t *testing.T) {
	cli := newClient(t)
	p := prefix(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { _, _ = cli.Delete(context.Background(), p, clientv3.WithPrefix()) })

	watcher := New(cli, p, "ctrl-watch", 10*time.Second)
	ch, err := watcher.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	voter := New(cli, p, "ctrl-a", 10*time.Second)
	if err := voter.Down(ctx, "edge-1"); err != nil {
		t.Fatal(err)
	}
	ev := recv(t, ch)
	if ev.Edge != "edge-1" || ev.Coverer != "ctrl-a" || !ev.Down {
		t.Fatalf("down event = %+v, want edge-1/ctrl-a/down", ev)
	}

	if err := voter.Up(ctx, "edge-1"); err != nil {
		t.Fatal(err)
	}
	ev = recv(t, ch)
	if ev.Edge != "edge-1" || ev.Coverer != "ctrl-a" || ev.Down {
		t.Fatalf("up event = %+v, want edge-1/ctrl-a/cleared", ev)
	}
}

// A vote already present before Watch starts arrives in the initial snapshot.
func TestVoteSnapshot(t *testing.T) {
	cli := newClient(t)
	p := prefix(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { _, _ = cli.Delete(context.Background(), p, clientv3.WithPrefix()) })

	voter := New(cli, p, "ctrl-b", 10*time.Second)
	if err := voter.Down(ctx, "edge-9"); err != nil {
		t.Fatal(err)
	}

	watcher := New(cli, p, "ctrl-watch", 10*time.Second)
	ch, err := watcher.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ev := recv(t, ch)
	if ev.Edge != "edge-9" || ev.Coverer != "ctrl-b" || !ev.Down {
		t.Fatalf("snapshot event = %+v, want edge-9/ctrl-b/down", ev)
	}
}

// A crashed voter's lease expiry clears its votes (a dead coverer stops voting).
func TestVoteExpiresOnCrash(t *testing.T) {
	if testEtcdAddr == "" {
		t.Skip("etcd binary not available")
	}
	cli := newClient(t)
	p := prefix(t)
	ctx := context.Background()
	t.Cleanup(func() { _, _ = cli.Delete(ctx, p, clientv3.WithPrefix()) })

	// Separate client = the "crashing" voter; closing it stops keep-alive.
	dyingCli, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	dying := New(dyingCli, p, "ctrl-dying", 2*time.Second) // short TTL
	if err := dying.Down(ctx, "edge-5"); err != nil {
		t.Fatal(err)
	}
	if !votePresent(t, cli, p, "edge-5", "ctrl-dying") {
		t.Fatal("vote should be present after Down")
	}

	dyingCli.Close() // crash: keep-alive stops, lease no longer renewed
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if !votePresent(t, cli, p, "edge-5", "ctrl-dying") {
			return // expired — good
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("crashed voter's vote did not expire within 8s")
}

func votePresent(t *testing.T, cli *clientv3.Client, p string, edge model.EdgeID, coverer string) bool {
	t.Helper()
	resp, err := cli.Get(context.Background(), p+"deathvotes/"+string(edge)+"/"+coverer)
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs) > 0
}

func recv(t *testing.T, ch <-chan VoteEvent) VoteEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a vote event")
		return VoteEvent{}
	}
}
