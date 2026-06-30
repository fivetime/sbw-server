package orchestrator

import "github.com/fivetime/sbw-server/internal/scheduler"

// Placement tie-break among equally-free edges is RANDOM in production (spread create
// bursts evenly — see scheduler.SelectHomes). These tests assert a specific primary/backup
// for equal-capacity edges, so make the tie-break deterministic (edge-id) for the package.
func init() { scheduler.DisableRandomTieBreak() }
