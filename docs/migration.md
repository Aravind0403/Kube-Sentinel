# Interface migration after v0.1.0

This cleanup is a breaking interface rename. Rebuild and deploy the scheduler and sidecar together. Existing images are not compatible with the renamed RPC service.

| Interface | v0.1.0 | Cleaned interface |
|---|---|---|
| Framework plugin and gRPC service | `ACOPredictiveScheduler` | `SentinelScheduler` |
| Scheduler name | `aco-sentinel-scheduler` | `kube-sentinel-scheduler` |
| Go module | `aco-sentinel-plugin` | `sentinel-plugin` |
| Workload and observation prefix | `aco-sentinel.io/` | `kube-sentinel.io/` |
| Placement diagnostics | `/pheromones` | `/placements` |
| Diagnostic counters | `total_deposits`, `phantom_deposits` | `committed_count`, `rollback_count` |

Update Pod templates and any node annotation writer together with the scheduler. An old workload key can lose its protected-workload classification; do not mix old clients and new binaries. Existing pending Pods with the old schedulerName must be recreated or handled by the old scheduler. Kubernetes does not rename existing objects when a manifest changes its name; remove the old test deployment separately after verifying the new deployment. Use a fresh disposable cluster for this release acceptance.

The `sentinel.cost/*`, `sentinel.gpu/*`, `sentinel.node/*` and provider metadata keys retain their meanings. Message field numbers are unchanged, but the RPC service path changes. Old state-file diagnostic counters are not migrated; use a fresh local state file for acceptance. Historical artifacts remain byte-for-byte unchanged.

To regenerate bindings, install the repository Python requirements, protoc and the Go plugins `protoc-gen-go` and `protoc-gen-go-grpc`, then run from the repository root:

```sh
protoc -I v2/proto --go_out=v2/go_plugin/pb --go_opt=paths=source_relative --go-grpc_out=v2/go_plugin/pb --go-grpc_opt=paths=source_relative v2/proto/sentinel.proto
python -m grpc_tools.protoc -I v2/proto --python_out=v2/proto --pyi_out=v2/proto --grpc_python_out=v2/proto v2/proto/sentinel.proto
```
