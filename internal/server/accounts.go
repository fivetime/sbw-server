package server

import (
	"context"
	"sync"
	"time"

	"github.com/fivetime/sbw-contract/model"
)

// Account reconciliation (controller §4.3 账面对账): the controller's token
// ledger says how much bandwidth it has SOLD on each agent (capacity×90% −
// remaining); each agent reports how much it has actually PROVISIONED
// (CapacityReport.SoldBandwidthBps, Σ of its home pools' rates). In steady state
// the two agree. Drift means a push was lost, a controller replica crashed
// mid-transaction, or a reservation hung — a quietly-oversold or under-counted
// agent. Reconciliation surfaces the drift (alarm) so it can self-heal or be
// corrected, rather than silently exhausting/wasting an agent's quota.
//
// It is read-only: it never auto-corrects the ledger (a wrong SetTokens could
// strand committed allocations); V1 alarms and leaves correction to an operator
// or a future guarded auto-repair.

// AccountDrift is one agent's ledger-vs-reported mismatch (all in bits/s).
type AccountDrift struct {
	Edge       model.EdgeID
	LedgerSold int64 // capacity×90% − remaining tokens (what the controller sold)
	AgentSold  int64 // the agent's reported SoldBandwidthBps (what it provisioned)
	Delta      int64 // LedgerSold − AgentSold (signed: + oversold by ledger, − under)
}

// reportCache holds the latest EdgeReport per edge for reconciliation and (later)
// soft-death fusion. Concurrent: written from gRPC Report handlers, read from the
// reconcile loop.
type reportCache struct {
	mu   sync.RWMutex
	last map[model.EdgeID]model.EdgeReport
}

func newReportCache() *reportCache {
	return &reportCache{last: map[model.EdgeID]model.EdgeReport{}}
}

func (c *reportCache) put(r model.EdgeReport) {
	c.mu.Lock()
	c.last[r.EdgeID] = r
	c.mu.Unlock()
}

// delete drops an edge's cached report on deregister/decommission so the cache does
// not retain entries for edges no longer in the fleet (per-edge growth over lifetime).
func (c *reportCache) delete(edge model.EdgeID) {
	c.mu.Lock()
	delete(c.last, edge)
	c.mu.Unlock()
}

func (c *reportCache) get(edge model.EdgeID) (model.EdgeReport, bool) {
	c.mu.RLock()
	r, ok := c.last[edge]
	c.mu.RUnlock()
	return r, ok
}

// ReconcileAccounts compares the ledger's sold bandwidth against each registered
// agent's last reported provisioned bandwidth and returns the drifts that exceed
// the tolerance, firing the drift alarm for each. Agents that have not reported
// yet are skipped (nothing to compare). Read-only.
func (cp *ControlPlane) ReconcileAccounts(ctx context.Context) ([]AccountDrift, error) {
	agents, err := cp.Registry.List(ctx)
	if err != nil {
		return nil, err
	}
	var drifts []AccountDrift
	for _, a := range agents {
		report, ok := cp.reports.get(a.EdgeID)
		if !ok {
			continue // no report yet
		}
		remaining, err := cp.Ledger.Remaining(ctx, string(a.EdgeID))
		if err != nil {
			return drifts, err
		}
		sellable := int64(a.CapacityBps) * sellableFracPercent / 100
		ledgerSold := sellable - remaining
		agentSold := int64(report.Capacity.SoldBandwidthBps)
		delta := ledgerSold - agentSold
		if abs64(delta) <= int64(cp.acctTolerance) {
			continue
		}
		d := AccountDrift{Edge: a.EdgeID, LedgerSold: ledgerSold, AgentSold: agentSold, Delta: delta}
		drifts = append(drifts, d)
		cp.metrics.AccountDrift(d.Edge, d.Delta)
		cp.log.Warn("account drift", "edge", d.Edge, "ledger_sold", d.LedgerSold, "agent_sold", d.AgentSold, "delta", d.Delta)
		if cp.onAcctDrift != nil {
			cp.onAcctDrift(d)
		}
	}
	return drifts, nil
}

// RunReconcileAccounts runs ReconcileAccounts every interval until ctx is
// cancelled. Blocks; run in a goroutine.
func (cp *ControlPlane) RunReconcileAccounts(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) {
		if _, err := cp.ReconcileAccounts(ctx); err != nil {
			cp.log.Warn("account reconciliation failed", "err", err)
		}
	})
}

// RefreshMetrics updates the inventory gauges (registered agents, stored pools,
// dead edges) once. RunMetricsRefresh calls it on a timer.
func (cp *ControlPlane) RefreshMetrics(ctx context.Context) {
	if cp.metrics == nil {
		return
	}
	agents, err := cp.Registry.List(ctx)
	if err != nil {
		cp.log.Warn("metrics refresh: list agents", "err", err)
		return
	}
	// Pools live in the MANDATORY Yugabyte bulk store (the create writes ZERO etcd,
	// so the etcd poolstore is empty in production). cp.YB is always wired.
	pools, err := cp.YB.List(ctx)
	if err != nil {
		cp.log.Warn("metrics refresh: list pools", "err", err)
		return
	}
	dead := 0
	for _, a := range agents {
		if cp.Liveness.IsDead(a.EdgeID) {
			dead++
		}
	}
	cp.metrics.SetInventory(len(agents), len(pools), dead)
}

// RunMetricsRefresh refreshes the inventory gauges every interval until ctx is
// cancelled. Blocks; run in a goroutine.
func (cp *ControlPlane) RunMetricsRefresh(ctx context.Context, interval time.Duration) {
	cp.runLoop(ctx, interval, func(ctx context.Context) { cp.RefreshMetrics(ctx) })
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
