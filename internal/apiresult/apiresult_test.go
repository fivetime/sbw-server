package apiresult

import (
	"context"
	"encoding/json"
	"testing"
)

// TestEventJSONSchema pins the wire schema the BSS correlates against: the exact
// JSON field names + the outcome enum values. A change here is a contract break.
func TestEventJSONSchema(t *testing.T) {
	ev := Event{
		RequestID:  "req-1",
		Op:         "create",
		PoolID:     42,
		Edge:       "edge-a",
		Outcome:    OutcomeConverged,
		Reason:     "",
		Generation: 7,
		TSUnixMs:   1_700_000_000_000,
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// reason is omitempty: absent when empty.
	for _, k := range []string{"request_id", "op", "pool_id", "edge", "outcome", "generation", "ts_unix_ms"} {
		if _, ok := m[k]; !ok {
			t.Errorf("event JSON missing field %q (got %v)", k, m)
		}
	}
	if _, ok := m["reason"]; ok {
		t.Errorf("empty reason must be omitted, got %v", m["reason"])
	}
	if m["outcome"] != "converged" {
		t.Errorf("outcome = %v, want converged", m["outcome"])
	}

	// A failed event carries the reason.
	b2, _ := json.Marshal(Event{Outcome: OutcomeFailed, Reason: "timeout"})
	var m2 map[string]any
	_ = json.Unmarshal(b2, &m2)
	if m2["outcome"] != "failed" || m2["reason"] != "timeout" {
		t.Errorf("failed event = %v, want outcome=failed reason=timeout", m2)
	}
}

// TestNoopEmitterDropsSilently proves the disabled emitter never panics / blocks.
func TestNoopEmitterDropsSilently(t *testing.T) {
	var e Emitter = Noop{}
	e.Emit(context.Background(), Event{RequestID: "x"}) // must be inert
}

// TestNewProducerValidation rejects empty brokers/topic (the cmd only builds a
// Producer when brokers are configured; this guards the constructor contract).
func TestNewProducerValidation(t *testing.T) {
	if _, err := NewProducer(nil, "t"); err == nil {
		t.Error("expected error for no brokers")
	}
	if _, err := NewProducer([]string{"localhost:9092"}, ""); err == nil {
		t.Error("expected error for empty topic")
	}
	// A valid construction succeeds WITHOUT dialing (franz-go connects lazily).
	p, err := NewProducer([]string{"localhost:9092"}, "sbw.api.results")
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	p.Close()
}
