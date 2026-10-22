# Release validation status

Baseline: v0.1.0, commit `4e769b391b8aced9780aa6b5525d8cbc2198ca0b`.

The baseline native Go race suite, 39 observation matrix cases and real Python daemon failover passed an independent rerun. Archived kind evidence supports the L1–L7 fixture outcomes. This cleanup changes public identifiers and generated RPC bindings, so it requires its own native test run and fresh local cluster evidence.

## Remaining local acceptance gate

Fix only L8 fault injection and evidence:

1. Stop the sidecar daemon while the scheduler keeps running. Assert scoring RPC unavailability and hold the outage through the protected Pod binding. Do not ignore failed fault injection.
2. Record a protected Pod UID, binding to on-demand capacity and correlated fallback evidence. Generate enough attempts if claiming the configured breaker opened.
3. Restart the daemon and await a successful RPC. Submit a new protected Pod and record its successful native scoring call through the scheduler. Rollout status or a commit callback does not prove scoring recovery.
4. Fail the scenario on missing evidence. Preserve timestamped logs.
5. Run all L1–L8 on a fresh cluster and repeat once, recording source commit, dirty state and image identifiers. The original handoff already required the repeat.

The archived run's fallback was at L2 startup (11:24:19 UTC), while L8's outage Pod bound at 11:24:46 and appears in a successful sidecar COMMIT. That does not establish a daemon outage during L8. The separate native integration test does establish process failover outside Kubernetes.

No algorithm, HA, dashboard, GPU hardware or research expansion is required to close this local gate.

## Cleanup branch verification (2026-10-22)

The renamed native plugin passed `go test -race -count=1 -v ./...` with the real Python daemon test enabled, including all 39 observation cases. Python confidence verification, offline replay (50 jobs), both renamed socket benchmark clients, shell syntax and local Markdown links were checked.

## GKE Acceptance Milestone (2026-10-22)

The bounded live GKE acceptance run (`gke-20261022-235720`, commit `bbec67e`) executed on a real GKE Standard cluster in `us-central1-a` across three nodes (two On-Demand and one native Spot). All 8 scenarios (L1–L8) passed:

1. **L1–L7 Scenarios**: Verified Spot routing, protected Spot avoidance, node architecture filtering, hardware accelerator filtering, cost-aware ranking, stale telemetry filtering, and mandatory cost-ceiling enforcement.
2. **L8 Outage & Recovery**: Proved daemon unavailability and fallback scoring in scheduler logs during protected Pod binding (preventing Spot scheduling), followed by clean daemon restart, port recovery, and resumed native sidecar scoring for subsequent workloads.
3. **Sequential Latency Benchmark**: 10-pod sequential scheduling achieved min 3.233s, median 3.302s, P95 3.409s, P99 3.448s, max 3.458s.
   *Qualification*: The median 3.302s measures client submission through observing the binding (including network/API calls and polling). It is not scheduler-only latency. Ten samples do not establish reliable tail latency.
4. **Teardown & Ongoing Spend**: Verified zero remaining clusters, VMs, disks, IP addresses, and forwarding rules. Unused registry images in Artifact Registry were purged to establish true zero ongoing spend.
5. **Endpoint Hardening**: Restricted the daemon's `/shutdown` HTTP endpoint to loopback origin (`127.0.0.1` / `::1`) and gated it behind `SENTINEL_ENABLE_TEST_SHUTDOWN="true"`, returning 403 Forbidden by default.

