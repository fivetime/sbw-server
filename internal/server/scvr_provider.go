package server

import (
	"context"
	"encoding/json"

	"github.com/fivetime/sbw-contract/model"
	"github.com/fivetime/sbw-contract/rpc"
)

// scvrProvider is the SERVER-HALF of the ServerCoverer contract: the Report and
// Register logic the gRPC handlers (servercoverer.go) delegate to. It is the
// post-split form of the monolith's in-process scvr.Provider — the Watch downlink
// is now the desiredFan + the gRPC Watch pump (connectCoverer / initialCovererSync),
// not an in-process adopted stream, so only Report + Register live here.
type scvrProvider struct{ cp *ControlPlane }

// Report ingests one coverer uplink event into the server half. The gRPC Report
// handler calls this per CovererReport on the stream; a bad report is logged, not
// fatal to the stream.
func (p *scvrProvider) Report(ctx context.Context, r *rpc.CovererReport) error {
	switch r.Kind {
	case rpc.CovererReport_DEATH_VOTE:
		edge := model.EdgeID(r.EdgeId)
		if r.Soft {
			// SOFT canary signal (DESIGN-liveness §4.7): the canary route withdrawn/restored.
			// It only fails over IN CONJUNCTION with an agent data-plane-death report, so it
			// drives the Monitor's canary path, NOT the hard FailoverQuorum.
			if r.Down {
				p.cp.Liveness.CanaryDown(edge)
			} else {
				p.cp.Liveness.CanaryUp(edge)
			}
		} else {
			// HARD session vote: each coverer's PeerDown/Up is a PER-COVERER vote keyed by
			// r.CovererId so FailoverQuorum corroborates ACROSS the K coverers (cp.HardDown
			// would collapse them into one server-local vote). Cross-server-replica
			// corroboration via the etcd deathvote bridge is the step-11 decision.
			if r.CovererId == "" {
				p.cp.log.Warn("hard death-vote report missing coverer_id (quorum cannot corroborate)", "edge", r.EdgeId)
			}
			p.cp.Liveness.Vote(edge, r.CovererId, r.Down)
		}
	case rpc.CovererReport_AGENT_REPORT:
		// the agent's EdgeReport (health/capacity/metering-echo) — process it on the
		// server half (heartbeat, soft-death, convergence resolve, applied-version
		// advance, drift backstop).
		var er model.EdgeReport
		if err := json.Unmarshal(r.Payload, &er); err != nil {
			return err
		}
		return p.cp.onReport(ctx, er)
	case rpc.CovererReport_MEMBER_EDGE:
		// TODO(§8): feed the server's global member→edge map (placement-locality-gap →
		// locality-aware placement; also the source the server needs to re-implement the
		// render-time anchor suppression the coverer-side guard used to drive). No source
		// yet (members not advertised in the lab) — stub ok.
	case rpc.CovererReport_AGENT_REGISTER:
		// SUPERSEDED: registration rides the request-response Register below (it returns a
		// reply — accepted + coverers — that the one-way Report cannot carry). Kept only so
		// a stray AGENT_REGISTER report is a harmless no-op.
	}
	return nil
}

// Register is the REQUEST-RESPONSE uplink: the coverer relays the agent's
// RegisterRequest and the server-half does the authoritative registration
// (onRegister: ledger init / edge inventory) and computes the agent's coverer set
// (covererFunc, sharding) into the reply. A coverer-lookup failure must NOT fail
// registration (the agent stays where it reached and is re-homed by the next REHOME
// push).
func (p *scvrProvider) Register(ctx context.Context, req *rpc.RegisterRequest) (*rpc.RegisterResponse, error) {
	if req.SchemaVersion != 0 && int(req.SchemaVersion) != model.SchemaVersion {
		return &rpc.RegisterResponse{SchemaVersion: model.SchemaVersion, Accepted: false}, nil
	}
	edge := model.EdgeID(req.EdgeId)
	if p.cp.onRegister != nil {
		if err := p.cp.onRegister(ctx, edge, req.CapacityBps); err != nil {
			return nil, err
		}
	}
	resp := &rpc.RegisterResponse{SchemaVersion: model.SchemaVersion, Accepted: true}
	if p.cp.covererFunc != nil {
		if a, ok, err := p.cp.covererFunc(ctx, edge); err != nil {
			p.cp.log.Warn("seam register: coverer assignment failed", "edge", edge, "err", err)
		} else if ok {
			if b, mErr := json.Marshal(a); mErr == nil {
				resp.Coverers = b
			}
		}
	}
	return resp, nil
}
