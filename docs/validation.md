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

The renamed native plugin passed `go test -race -count=1 -v ./...` with the real Python daemon test enabled, including all 39 observation cases. Python confidence verification, offline replay (50 jobs), both renamed socket benchmark clients, shell syntax and local Markdown links were checked. Existing archived acceptance files have no diff. Container images and the fresh kind L1–L8 matrix have not been rerun for this branch; neither cloud readiness nor L8 completion is claimed.
