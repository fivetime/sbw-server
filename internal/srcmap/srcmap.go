// Package srcmap holds the controller's src_ip → home-agent record DTO
// (controller §6.4). The authoritative src→home store is now Yugabyte-backed
// (see internal/ybstore); the former etcd-backed Store has been removed. What
// remains is the shared record shape the orchestrator passes around.
package srcmap

import (
	"net/netip"

	"github.com/fivetime/sbw-contract/model"
)

// Record is one source's home assignment.
type Record struct {
	Src       netip.Prefix `json:"src"`
	Home      model.EdgeID `json:"home"`
	PoolID    model.PoolID `json:"pool_id"`
	UpdatedAt int64        `json:"updated_at_ms"`
}
