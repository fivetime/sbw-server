// Package apiresult emits ASYNC API-result events to a Redpanda/Kafka topic so the
// BSS can correlate a synchronous admin-API response (a 201/200/202 carrying a
// request_id) with the eventual DATA-PLANE realization of that operation.
//
// The control plane accepts/rejects a create/update/destroy SYNCHRONOUSLY (the
// HTTP status already carries it: 400/409/500/503 reject, 2xx accept). But the
// data-plane apply — the agent installing the member's VPP policer, announcing the
// /32 anchor + FlowSpec via bird — is fired off the request path AFTER the 2xx, so
// the BSS otherwise has no way to learn WHEN/IF it converged. This package closes
// that gap: the control plane registers a "pending" against the request_id and the
// pushed delta's per-edge Generation, and when the home edge's report echoes an
// applied Generation >= that pending's, a "converged" event is emitted; a timeout
// sweep emits "failed"(reason=timeout) for pendings that never converge.
//
// The emit is OPTIONAL and best-effort: with no brokers configured the control
// plane wires the Noop emitter and behaves EXACTLY as today (no new failure mode,
// no behaviour change). The franz-go Produce is async (callback-delivered), so the
// emit never blocks the report hot path or the create path.
package apiresult

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Outcome is the terminal state of an API operation's data-plane realization.
type Outcome string

const (
	// OutcomeConverged: the home edge's report echoed an applied generation at or
	// past the operation's pushed generation — the data plane reflects the op.
	OutcomeConverged Outcome = "converged"
	// OutcomeFailed: the operation did not converge within the timeout (reason set).
	OutcomeFailed Outcome = "failed"
)

// Event is the JSON envelope shipped to the API-results topic, keyed by RequestID.
// ONE schema carries TWO shapes the BSS discriminates by Op (or by RequestID==""):
//
//   - API-RESULT (request-correlated): the data-plane outcome of an admin op the BSS
//     issued — Op is "create"|"update"|"destroy"|"migrate"|"decommission", RequestID
//     is the handle the synchronous response carried, and Outcome is "converged" (the
//     home edge's report echoed an applied generation >= Generation) or "failed"
//     (the timeout sweep fired, Reason="timeout"). Edge is the home edge whose apply
//     resolves the op. FromEdge/ToEdge/Source are empty.
//
//   - FAILOVER (unsolicited, controller-initiated): a node-failure auto-promote the
//     BSS issued NO request for, yet its pool→edge home moved and it must learn. Op
//     is "failover", Source is "controller", RequestID is "" (no request to
//     correlate), FromEdge is the dead old primary, ToEdge is the promoted backup,
//     Reason is "node-failure", and Generation is the new primary's render generation.
//     It is a NOTIFICATION (no converged/timeout handshake) emitted at the
//     auto-promote decision point — Outcome/Edge are left empty.
//
//   - MEMBER-DOWN / MEMBER-UP (unsolicited, controller-initiated): a pool MEMBER's
//     /32 (or /128) host route left / re-entered its home edge's physical RIB — the
//     authoritative member-liveness signal (DESIGN-liveness: a member's host-route
//     presence in the home edge's RIB via BGP IS its liveness; route-withdrawal is
//     the veto). Op is "member-down" / "member-up", Source is "controller", RequestID
//     is "" (unsolicited), MemberPrefix carries the host /32 (/128), Edge is the home
//     edge, and Reason names the death method ("route-withdrawal" — the controller's
//     trustworthy-absence verdict via its RIB tap). Like FAILOVER it is a NOTIFICATION
//     (no converged/timeout handshake) — Outcome/FromEdge/ToEdge are left empty.
type Event struct {
	RequestID  string  `json:"request_id"`
	Op         string  `json:"op"` // open set: "create"|"update"|"destroy"|"migrate"|"decommission"|"failover"|"member-down"|"member-up"|"delivery-loss"|"program-drift"|"anchor-unprovisioned"|"anchor-rogue"|"edge-dataplane-down"|"edge-dataplane-up"|"metering-stale"|"metering-resumed"|"pool-double-death"|"pool-expired"|"member-evicted"|"edge-down"|"edge-up"|"edge-registered"|"edge-deregistered"|"edge-capacity-changed"|"redundancy-lost"|"redundancy-regained"|"backup-changed"|"capacity-exhausted"|"rehome"|"edge-forwarding-degraded"|"edge-forwarding-recovered"|…
	PoolID     uint64  `json:"pool_id"`
	Edge       string  `json:"edge,omitempty"`    // the primary/home edge whose apply resolves an API-result op (member-down/up: the member's home edge)
	Outcome    Outcome `json:"outcome,omitempty"` // "converged"|"failed" (API-result only; empty for failover / member-down/up)
	Reason     string  `json:"reason,omitempty"`  // "timeout" (failed) | "node-failure" (failover) | "route-withdrawal"|"bfd-down" (member-down)
	Generation uint64  `json:"generation"`        // the per-edge desired-state generation the op pushed
	TSUnixMs   int64   `json:"ts_unix_ms"`        // when the event was emitted
	// Failover-only fields (omitted for API-result events).
	Source   string `json:"source,omitempty"`    // "controller" for an unsolicited failover / member-down/up event
	FromEdge string `json:"from_edge,omitempty"` // failover: the dead old primary the pool moved OFF
	ToEdge   string `json:"to_edge,omitempty"`   // failover: the promoted backup the pool moved ONTO
	// Member-down/up-only field (omitted for the others).
	MemberPrefix string `json:"member_prefix,omitempty"` // the member host /32 (/128) whose RIB presence flipped

	// --- TIER 1 + TIER 2 unsolicited observability fields (appended additively; do
	// NOT renumber/reorder — later tiers append further). All omitempty so a request-
	// correlated API-result and the existing failover/member events serialize unchanged.

	// Gap is the count magnitude that tripped a data-plane drift alarm (delivery-loss
	// / program-drift): Σ|expected−reported| of policers+sessions for the drift Kind.
	Gap int `json:"gap,omitempty"`
	// DisplacedByPool is the pool that DISPLACED a member out of PoolID's pool on a
	// cross-pool CIDR-overlap replace (member-evicted): PoolID is the displaced (losing)
	// pool, DisplacedByPool is the displacing (winning) one.
	DisplacedByPool uint64 `json:"displaced_by_pool,omitempty"`
	// Action is the action kind of an auto-destroyed pool (pool-expired): e.g.
	// "blackhole"/"scrub" — the suppression action whose TTL elapsed.
	Action string `json:"action,omitempty"`

	// --- TIER 3 (edge/fleet availability) + TIER 4 (enrichment) fields (appended
	// additively; do NOT renumber/reorder). All omitempty so every pre-TIER-3 event
	// (request-correlated API-results, failover, member-*, the TIER-1/2 events)
	// serializes byte-for-byte unchanged.

	// CapacityBps is an edge's sellable NIC line rate (bits/s) carried by the
	// edge-registered / edge-deregistered / edge-capacity-changed fleet events — the
	// billing/oversell input. Only set on those edge-inventory events.
	CapacityBps int64 `json:"capacity_bps,omitempty"`

	// --- TIER 4 enrichment of the request-correlated "converged" create/update event:
	// the GRANTED rate basis so an event-only BSS can rate the pool without a second
	// lookup. Set ONLY on a converged create/update (zero/omitted elsewhere).

	// CIRKbps is the pool's granted committed rate in kbps (egress, the scarce
	// uplink that drives the token cost). 0 with Unlimited=true ⇒ the 95th-percentile
	// "无限带宽" pool (rendered as a 100Gbps count-only policer — neither tokens nor the
	// metered rate reveal the rating basis, so Unlimited is the explicit flag for it).
	CIRKbps uint64 `json:"cir_kbps,omitempty"`
	// IngressCIRKbps is the pool's granted ingress (downlink) committed rate in kbps,
	// for a per-direction-rated pool. 0 ⇒ symmetric / not separately rated.
	IngressCIRKbps uint64 `json:"ingress_cir_kbps,omitempty"`
	// Tokens is the per-home quota cost debited for the pool (bits/s for a rate-limit
	// pool, a nominal 1 for a control/blackhole pool) — the ledger cost basis.
	Tokens int64 `json:"tokens,omitempty"`
	// BillingMode is the rating basis: "cir" (a committed-rate pool) or "95th-pct" (an
	// unlimited CIR==0 pool billed on the 95th percentile). Set on converged
	// create/update only.
	BillingMode string `json:"billing_mode,omitempty"`
	// Unlimited flags the CIR==0 / 95th-percentile pool (the sharp case ~0 tokens +
	// 100G placeholder policer). Set on converged create/update only.
	Unlimited bool `json:"unlimited,omitempty"`

	// --- TIER 5 (per-member forwarding-loss, §4.2.5) fields (appended additively; do
	// NOT renumber/reorder). All omitempty so every pre-TIER-5 event serializes unchanged.

	// LossBps is a member's forwarding-loss in basis points (0..10000) carried by the
	// edge-forwarding-degraded / edge-forwarding-recovered alert events and the
	// loss-triggered "migrate" event (Reason="forwarding-loss"). The member is in
	// MemberPrefix, the direction ("ingress"/"egress") + top drop reason in Reason.
	LossBps uint16 `json:"loss_bps,omitempty"`
}

// Emitter ships an API-result Event. Emit MUST NOT block the caller (the report /
// create hot paths): the franz-go impl produces asynchronously with a callback.
type Emitter interface {
	Emit(ctx context.Context, ev Event)
}

// Noop is the disabled emitter: it drops every event. Wired when no brokers are
// configured, so the control plane runs exactly as it did before this feature.
type Noop struct{}

// Emit discards the event.
func (Noop) Emit(context.Context, Event) {}

var _ Emitter = Noop{}

// Producer is a franz-go-backed Emitter. It produces one JSON record per event to
// the configured topic, keyed by request_id so all events for one request land in
// one partition (ordered per request). Produce is ASYNCHRONOUS: it enqueues into
// franz-go's internal buffer (which batches + retries) and returns immediately, so
// the report/create hot paths never block on the broker.
type Producer struct {
	client *kgo.Client
	topic  string
	log    func(error)
}

var _ Emitter = (*Producer)(nil)

// Option configures a Producer.
type Option func(*Producer)

// WithErrorLog sets a callback for async produce errors (default: discard). Wire
// it to the controller logger so a persistently-failing broker is observable.
func WithErrorLog(f func(error)) Option {
	return func(p *Producer) { p.log = f }
}

// NewProducer builds a franz-go producer over the given brokers + topic. Returns
// an error only on client construction (bad broker syntax); it does NOT dial here
// — franz-go connects lazily on the first Produce, so a temporarily-unreachable
// broker never blocks startup. Caller closes it via Close.
func NewProducer(brokers []string, topic string, opts ...Option) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("apiresult: at least one broker required")
	}
	if topic == "" {
		return nil, fmt.Errorf("apiresult: topic required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("apiresult: new client: %w", err)
	}
	p := &Producer{client: cl, topic: topic, log: func(error) {}}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

// Emit marshals the event and produces it ASYNCHRONOUSLY: kgo.Client.Produce
// enqueues into the internal buffer and returns at once, delivering any error to
// the callback. It NEVER blocks the caller (the onReport / timeout-sweep paths),
// preserving the per-million-pool create/report throughput. A marshal failure is
// dropped (it cannot succeed on retry); a produce error is logged — the event is
// best-effort (the next report re-resolves a converged pending; the timeout sweep
// re-fires a failed one).
func (p *Producer) Emit(ctx context.Context, ev Event) {
	b, err := json.Marshal(ev)
	if err != nil {
		p.log(fmt.Errorf("apiresult: marshal event %q: %w", ev.RequestID, err))
		return
	}
	// Key by request_id so all events for one request land in one partition (ordered
	// per request). An unsolicited event carries no request_id, so key it so its
	// stream stays ordered and a single hot partition is avoided: a member-down/up
	// event keys by member_prefix (all events for one member ordered), every other
	// unsolicited event (failover) keys by pool_id.
	key := []byte(ev.RequestID)
	if len(key) == 0 {
		if ev.MemberPrefix != "" {
			key = []byte(ev.MemberPrefix)
		} else {
			key = []byte(strconv.FormatUint(ev.PoolID, 10))
		}
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: b,
	}
	p.client.Produce(ctx, rec, func(_ *kgo.Record, perr error) {
		if perr != nil {
			p.log(fmt.Errorf("apiresult: produce request %q: %w", ev.RequestID, perr))
		}
	})
}

// Close flushes buffered events and shuts the producer down. Best-effort flush so
// a clean shutdown delivers in-flight events; call from the cmd's shutdown path.
func (p *Producer) Close() {
	if p.client == nil {
		return
	}
	_ = p.client.Flush(context.Background())
	p.client.Close()
}
