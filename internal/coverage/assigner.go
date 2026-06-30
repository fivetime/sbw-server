package coverage

import (
	"context"

	"github.com/fivetime/sbw-contract/model"
)

// EndpointMap resolves live controller ids to their agent-facing gRPC endpoints
// (ctrlreg.Registry.Endpoints satisfies it). Kept separate from Members so the
// assigner depends only on what it needs.
type EndpointMap interface {
	Endpoints(ctx context.Context) (map[string]string, error)
}

// Assigner turns "who covers edge X" into the concrete CovererAssignment the
// controller hands an agent (DESIGN-liveness §10, L-05→L-06): the edge's HRW
// coverers, resolved to dial endpoints, with the highest-ranked one marked
// primary. The agent reports to the primary and keeps the rest as fallback.
type Assigner struct {
	rec *Reconciler
	ep  EndpointMap
}

// NewAssigner builds an assigner over a Reconciler (for the coverer ranking) and
// an endpoint map (for the dial targets).
func NewAssigner(rec *Reconciler, ep EndpointMap) *Assigner {
	return &Assigner{rec: rec, ep: ep}
}

// Assign computes edge's coverer assignment: primary first (index 0 of the HRW
// ranking), then fallbacks, each resolved to its gRPC endpoint. A coverer whose
// endpoint is unknown (raced membership) is still listed with an empty endpoint
// so the agent at least learns the id; it simply can't dial it yet.
func (a *Assigner) Assign(ctx context.Context, edge model.EdgeID) (model.CovererAssignment, error) {
	ids, err := a.rec.CoverersOf(ctx, edge)
	if err != nil {
		return model.CovererAssignment{}, err
	}
	eps, err := a.ep.Endpoints(ctx)
	if err != nil {
		return model.CovererAssignment{}, err
	}
	coverers := make([]model.Coverer, 0, len(ids))
	for i, id := range ids {
		coverers = append(coverers, model.Coverer{
			ControllerID: id,
			GRPCEndpoint: eps[id],
			Primary:      i == 0,
		})
	}
	return model.CovererAssignment{EdgeID: edge, Coverers: coverers}, nil
}
