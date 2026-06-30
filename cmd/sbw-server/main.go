// Command sbw-server is the SBW control plane's GLOBAL BRAIN + cached api-server
// (DESIGN-server-coverer-split): the sole owner of the YugabyteDB/etcd connections,
// it does placement/装箱, the failover DECISION, and the BSS API, and serves coverers a
// CACHED rpc.ServerCoverer (Watch fanned from cache, Report into the global view) so
// store connections stay O(server-replicas), not O(edges).
//
// SCAFFOLD (§8 step 2): module + skeleton only. The server-half packages migrate here in
// §8 step 3 — admin / scheduler / orchestrator / ybstore / poolstore / render / srcmap /
// ledger / registry / apiresult — out of sbw-controller, which then retires.
package main

import (
	"flag"
	"fmt"

	"github.com/fivetime/sbw-contract/buildinfo"
	"github.com/fivetime/sbw-contract/logx"
	"github.com/fivetime/sbw-contract/rpc"
)

// the server IMPLEMENTS the ServerCoverer contract (coverers are its clients).
var _ rpc.ServerCovererServer

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.String())
		return
	}
	log := logx.Default()
	log.Info("sbw-server scaffold — not yet wired (DESIGN-server-coverer-split §8)",
		"version", buildinfo.Version, "component", "server")
	// TODO(§8 step3): own YugabyteDB/etcd/Redpanda + serve rpc.ServerCoverer
	//   (Watch from the watch cache; Report aggregated into the global view).
}
