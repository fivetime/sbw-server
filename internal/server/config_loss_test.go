package server

import (
	"testing"
	"time"

	"github.com/fivetime/sbw-contract/config"
)

func TestLossConfigDefaults(t *testing.T) {
	l := LossConfig{}.WithDefaults()
	if l.AlertPct != 15 || l.MigratePct != 30 {
		t.Fatalf("loss pct defaults = %d/%d, want 15/30", l.AlertPct, l.MigratePct)
	}
	if time.Duration(l.MigrateSustain) != 5*time.Minute {
		t.Fatalf("migrate sustain default = %v, want 5m", time.Duration(l.MigrateSustain))
	}
	// Explicit values survive.
	l2 := LossConfig{AlertPct: 10, MigratePct: 25, MigrateSustain: config.Duration(time.Minute)}.WithDefaults()
	if l2.AlertPct != 10 || l2.MigratePct != 25 || time.Duration(l2.MigrateSustain) != time.Minute {
		t.Fatalf("explicit loss config overwritten: %+v", l2)
	}
}

func baseValidConfig() ServerConfig {
	return ServerConfig{
		Etcd:                    EtcdConfig{Endpoints: []string{"http://127.0.0.1:2379"}},
		ServerCovererListenAddr: ":1792",
	}
}

func TestLossConfigValidate(t *testing.T) {
	ok := baseValidConfig()
	ok.Loss = LossConfig{AlertPct: 15, MigratePct: 30}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid loss config rejected: %v", err)
	}
	// Zero = "use default" — must pass (WithDefaults fills it later).
	if err := baseValidConfig().Validate(); err != nil {
		t.Fatalf("zero loss config rejected: %v", err)
	}
	for name, lc := range map[string]LossConfig{
		"alert>100":      {AlertPct: 101},
		"migrate>100":    {MigratePct: 101},
		"alert negative": {AlertPct: -1},
		"migrate<alert":  {AlertPct: 40, MigratePct: 20},
	} {
		c := baseValidConfig()
		c.Loss = lc
		if err := c.Validate(); err == nil {
			t.Fatalf("%s: want validation error", name)
		}
	}
}
