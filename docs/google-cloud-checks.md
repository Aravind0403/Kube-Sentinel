# Next: bounded Google Cloud acceptance

Status: preparation only. Complete the local L8 gate and rerun the renamed release first. No cloud resources are created by these instructions.

## Logistics

Use a dedicated test project (or an explicitly isolated test cluster), billing access, a selected region/zone, a spend budget and time limit. The operator needs permission to manage the test GKE cluster, push/pull Artifact Registry images, and install the scheduler's Kubernetes RBAC. Use short-lived authenticated tooling; do not commit keys. Budget alerts are notifications, not a hard spending cap.

Choose a small GKE Standard CPU-only test cluster with on-demand capacity and one Spot worker. Select machine sizes and estimate current cluster, node, registry, network and logging costs for the chosen location before creation. Set an explicit teardown time. GPU quota and GPUs are unnecessary for this gate.

Build for the actual node architecture, publish both images using immutable identifiers, and create a dedicated cloud overlay using pullable registry images. The local profile uses imagePullPolicy Never and is not a cloud deployment manifest. Keep the scheduler itself on on-demand capacity and use a separate schedulerName so only opted-in test Pods use it. Check the compiled Kubernetes framework dependency against the target GKE version rather than assuming compatibility from kind.

## Check sequence

1. Record project, cluster version, node architecture, commit and image digests.
2. Install the renamed plugin and sidecar with scoped RBAC. Verify scheduler startup and a successful scoring RPC before submitting test work.
3. Record provider Spot labels; GKE supplies `cloud.google.com/gke-spot=true`. Add explicit price metadata and a controlled observation writer. Record prices as fixtures unless separately verified; fabricated utilization is never real host telemetry.
4. Repeat the same L1–L8 outcomes using actual Pod bindings, pending reasons and timestamped scoring/fallback evidence. Include unknown-price rejection under a ceiling and confirm the default scheduler remains usable for other Pods.
5. Preserve raw evidence and measured recovery times without extrapolating an SLA or cloud savings. Keep CPU placement evidence separate from GPU validation.
6. Delete the dedicated cluster and unneeded registry images, then verify resource cleanup. Record the observed bill separately when available.

The first cloud run answers whether the tested plugin deploys and preserves its policies on GKE. Real GPU execution, representative workload comparisons and research experiments are separate follow-ups.

Sources checked for this plan:

- [GKE modes of operation](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/choose-cluster-mode)
- [GKE Spot VMs and provider labels](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/spot-vms)
