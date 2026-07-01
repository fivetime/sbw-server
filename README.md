# sbw-server — SBW 控制面全局脑 + 缓存型 api-server

共享带宽池(SBW)控制面拆分后的**全局**部件(见 `sbw-contract/docs/DESIGN-server-coverer-split.md`)。

- **唯一**持有 YugabyteDB / etcd 连接 + 内存 watch cache;放置装箱、failover **决策**、BSS API。
- 对 coverer 提供 `rpc.ServerCoverer`:`Watch`(从缓存扇出覆盖分配 + 每边 desired-state)、`Report`(聚合判死票 / member→edge / agent 上报成全局视图)。
- **承重点**:coverer 只 watch server、绝不碰存储 → 存储连接数 = O(server 副本),与边数无关。

## 状态
**已上线(§8 拆分完成)**:server 侧包(`admin`/`scheduler`/`orchestrator`/`ybstore`/`render`/`srcmap`/`ledger`/`registry`/`apiresult`)已从单体迁入,CI 全绿,并在 k3s lab 端到端验证(基本流 + onSubscribe 修复 + K=2 coverer 分配/homing + coverer failover + 恢复再平衡)。单体 `sbw-controller` 已退役(仓库归档只读)。共享契约/模型在 `sbw-contract`(`rpc`/`model`);分片传感/执行半在 `sbw-coverer`。

```bash
go build ./...        # 编译
go run ./cmd/sbw-server --version
```
