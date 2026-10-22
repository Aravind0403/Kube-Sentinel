#!/usr/bin/env bash
# ==============================================================================
# scripts/run_local_acceptance.sh
# Kube-Sentinel Local-Cluster Acceptance Suite
#
# Creates a dedicated disposable 4-node Kind cluster (1 control-plane, 3 workers),
# builds and loads native scheduler and sidecar images matching host architecture,
# establishes worker fixtures (cheap OD, expensive OD, provider-labeled Spot),
# and executes the frozen acceptance matrix L1–L8 with bounded timeouts.
#
# Exports structured evidence to: artifacts/local-acceptance/<run-id>/
# ==============================================================================

set -euo pipefail

RUN_ID="local-$(date +%Y%m%d-%H%M%S)"
CLUSTER_NAME="sentinel-acc-${RANDOM}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARTIFACTS_DIR="${REPO_ROOT}/artifacts/local-acceptance/${RUN_ID}"
mkdir -p "${ARTIFACTS_DIR}"

KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
CONTAINER_CLI="docker"
if command -v docker &>/dev/null && docker info &>/dev/null; then
    CONTAINER_CLI="docker"
elif command -v podman &>/dev/null; then
    CONTAINER_CLI="podman"
    export KIND_EXPERIMENTAL_PROVIDER=podman
fi

echo "================================================================================"
echo "KUBE-SENTINEL: LOCAL CLUSTER ACCEPTANCE TEST"
echo "Run ID       : ${RUN_ID}"
echo "Cluster Name : ${CLUSTER_NAME}"
echo "Container CLI: ${CONTAINER_CLI}"
echo "Artifacts    : ${ARTIFACTS_DIR}"
echo "================================================================================"

EXIT_STATUS=0
cleanup() {
    EXIT_STATUS=$?
    echo ""
    echo "--- Collecting Cluster Diagnostic Logs ---"
    if kubectl cluster-info --context "kind-${CLUSTER_NAME}" &>/dev/null; then
        kubectl logs -n sentinel-system deployment/kube-sentinel-scheduler -c scheduler > "${ARTIFACTS_DIR}/scheduler.log" 2>&1 || true
        kubectl logs -n sentinel-system deployment/kube-sentinel-scheduler -c sidecar > "${ARTIFACTS_DIR}/sidecar.log" 2>&1 || true
        kubectl logs -n sentinel-system deployment/kube-sentinel-scheduler -c sidecar --previous >> "${ARTIFACTS_DIR}/sidecar.log" 2>&1 || true
        kubectl get events -A -o json > "${ARTIFACTS_DIR}/events.json" 2>&1 || true
    fi

    python3 -c "
import glob, json, os
pods = []
for p in sorted(glob.glob('${ARTIFACTS_DIR}/.pod_*.json')):
    try:
        with open(p) as f:
            pods.append(json.load(f))
        os.remove(p)
    except Exception:
        pass
if pods:
    with open('${ARTIFACTS_DIR}/pods.json', 'w') as out:
        json.dump({'items': pods}, out, indent=2)
" || true
    if [[ ! -s "${ARTIFACTS_DIR}/pods.json" ]] && kubectl cluster-info --context "kind-${CLUSTER_NAME}" &>/dev/null; then
        kubectl get pods -A -o json > "${ARTIFACTS_DIR}/pods.json" 2>&1 || true
    fi

    if [[ "${KEEP_CLUSTER}" != "true" ]]; then
        echo "Deleting temporary Kind cluster ${CLUSTER_NAME}..."
        kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
    else
        echo "Preserving Kind cluster ${CLUSTER_NAME} as requested."
    fi

    if [[ ${EXIT_STATUS} -eq 0 ]]; then
        echo "✅ LOCAL ACCEPTANCE RUN SUCCEEDED: All scenarios passed."
    else
        echo "❌ LOCAL ACCEPTANCE RUN FAILED: Exit code ${EXIT_STATUS}."
    fi
    exit ${EXIT_STATUS}
}
trap cleanup EXIT

# 1. Build Local Images
echo "[Step 1/4] Building local images for native architecture..."
${CONTAINER_CLI} build -t localhost/kube-sentinel-scheduler:local -f "${REPO_ROOT}/v2/Dockerfile.scheduler" "${REPO_ROOT}"
${CONTAINER_CLI} build -t localhost/kube-sentinel-sidecar:local -f "${REPO_ROOT}/v2/Dockerfile.grpc" "${REPO_ROOT}"
echo "✓ Local container images built successfully."

# 2. Create Kind Cluster
echo "[Step 2/4] Creating Kind cluster ${CLUSTER_NAME} with 1 control-plane and 3 workers..."
kind create cluster --name "${CLUSTER_NAME}" --config "${REPO_ROOT}/k8s/local/kind-config.yaml"
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

# 3. Load Images into Kind Cluster
echo "[Step 3/4] Loading local images into Kind cluster nodes..."
TMP_DIR=$(mktemp -d)
${CONTAINER_CLI} save localhost/kube-sentinel-scheduler:local -o "${TMP_DIR}/scheduler.tar"
${CONTAINER_CLI} save localhost/kube-sentinel-sidecar:local -o "${TMP_DIR}/sidecar.tar"
kind load image-archive "${TMP_DIR}/scheduler.tar" --name "${CLUSTER_NAME}"
kind load image-archive "${TMP_DIR}/sidecar.tar" --name "${CLUSTER_NAME}"
rm -rf "${TMP_DIR}"
echo "✓ Images loaded into cluster nodes."

# Wait for nodes to be Ready
echo "Waiting for all cluster nodes to reach Ready state..."
kubectl wait --for=condition=Ready nodes --all --timeout=60s

# Setup Worker Node Fixtures
echo "Configuring worker node test fixtures..."
WORKER_NODES=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "control-plane"))
if [[ ${#WORKER_NODES[@]} -lt 3 ]]; then
    echo "ERROR: Expected at least 3 worker nodes, found ${#WORKER_NODES[@]}"
    exit 1
fi

W_CHEAP="${WORKER_NODES[0]}"
W_EXP="${WORKER_NODES[1]}"
W_SPOT="${WORKER_NODES[2]}"

echo "Worker 1 (Cheap On-Demand)     : ${W_CHEAP}"
echo "Worker 2 (Expensive On-Demand) : ${W_EXP}"
echo "Worker 3 (Provider-labeled Spot): ${W_SPOT}"

NOW=$(date +%s)

# Fixture 1: Cheap On-Demand ($0.40/hr, on-demand, valid telemetry)
kubectl label node "${W_CHEAP}" fixture.sentinel/role=cheap-on-demand --overwrite
kubectl annotate node "${W_CHEAP}" \
    sentinel.cost/hourly-usd="0.40" \
    sentinel.cost/is-spot="false" \
    kube-sentinel.io/reported-allocatable-cpu="8.0" \
    kube-sentinel.io/reported-used-cpu="2.0" \
    kube-sentinel.io/reported-free-cpu="6.0" \
    kube-sentinel.io/reported-allocatable-memory-gb="32.0" \
    kube-sentinel.io/reported-used-memory-gb="8.0" \
    kube-sentinel.io/reported-free-memory-gb="24.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

# Fixture 2: Expensive On-Demand ($2.50/hr, on-demand, valid telemetry)
kubectl label node "${W_EXP}" fixture.sentinel/role=expensive-on-demand --overwrite
kubectl annotate node "${W_EXP}" \
    sentinel.cost/hourly-usd="2.50" \
    sentinel.cost/is-spot="false" \
    kube-sentinel.io/reported-allocatable-cpu="8.0" \
    kube-sentinel.io/reported-used-cpu="2.0" \
    kube-sentinel.io/reported-free-cpu="6.0" \
    kube-sentinel.io/reported-allocatable-memory-gb="32.0" \
    kube-sentinel.io/reported-used-memory-gb="8.0" \
    kube-sentinel.io/reported-free-memory-gb="24.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

# Fixture 3: Provider-labeled Spot ($0.15/hr, Spot, valid telemetry)
kubectl label node "${W_SPOT}" cloud.google.com/gke-spot=true fixture.sentinel/role=spot --overwrite
kubectl annotate node "${W_SPOT}" \
    sentinel.cost/hourly-usd="0.15" \
    sentinel.cost/is-spot="true" \
    sentinel.cost/spot-interruption-prob="0.15" \
    kube-sentinel.io/reported-allocatable-cpu="8.0" \
    kube-sentinel.io/reported-used-cpu="1.0" \
    kube-sentinel.io/reported-free-cpu="7.0" \
    kube-sentinel.io/reported-allocatable-memory-gb="32.0" \
    kube-sentinel.io/reported-used-memory-gb="4.0" \
    kube-sentinel.io/reported-free-memory-gb="28.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

refresh_heartbeats() {
    local t=$(date +%s)
    kubectl annotate node "${W_CHEAP}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
    kubectl annotate node "${W_EXP}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
    kubectl annotate node "${W_SPOT}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
}

# 4. Deploy Native Scheduler & Sidecar
echo "[Step 4/4] Deploying Kube-Sentinel scheduler and sidecar..."
kubectl apply -f "${REPO_ROOT}/k8s/local/scheduler-deployment.yaml"

# Scenario Matrix Execution
SCENARIOS_JSON="[]"

record_scenario() {
    local id="$1"
    local name="$2"
    local inputs="$3"
    local expected="$4"
    local observed="$5"
    local status="$6"
    echo "  [${status}] ${id}: ${name} -> ${observed}"
    SCENARIOS_JSON=$(python3 -c "
import json, sys
data = json.loads('''${SCENARIOS_JSON}''')
data.append({
    'id': sys.argv[1],
    'name': sys.argv[2],
    'inputs': sys.argv[3],
    'expected': sys.argv[4],
    'observed': sys.argv[5],
    'status': sys.argv[6]
})
print(json.dumps(data, indent=2))
" "${id}" "${name}" "${inputs}" "${expected}" "${observed}" "${status}")
}

archive_pod() {
    local pod_name="$1"
    kubectl get pod "${pod_name}" -o json > "${ARTIFACTS_DIR}/.pod_${pod_name}.json" 2>/dev/null || true
}

echo ""
echo "================================================================================"
echo "EXECUTING FROZEN ACCEPTANCE SCENARIO MATRIX (L1 - L8)"
echo "================================================================================"

# L1: Healthy scheduler and sidecar readiness
echo "Running L1: Healthy scheduler and sidecar deployment check..."
if kubectl rollout status deployment/kube-sentinel-scheduler -n sentinel-system --timeout=90s; then
    record_scenario "L1" "Healthy scheduler and sidecar" "Deployment kube-sentinel-scheduler in sentinel-system" "Deployment ready within 90s" "Scheduler & sidecar running and healthy" "PASS"
else
    record_scenario "L1" "Healthy scheduler and sidecar" "Deployment kube-sentinel-scheduler in sentinel-system" "Deployment ready within 90s" "Rollout timed out or failed" "FAIL"
    exit 1
fi

wait_for_pod_scheduled() {
    local pod_name="$1"
    local timeout="${2:-30}"
    local start=$(date +%s)
    while true; do
        local node=$(kubectl get pod "${pod_name}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
        if [[ -n "${node}" ]]; then
            echo "${node}"
            return 0
        fi
        local now=$(date +%s)
        if [[ $((now - start)) -ge ${timeout} ]]; then
            return 1
        fi
        sleep 0.5
    done
}

# L2: Ordinary eligible workload
echo "Running L2: Ordinary eligible workload..."
refresh_heartbeats
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l2
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l2" 30); then
    archive_pod "pod-l2"
    record_scenario "L2" "Ordinary eligible workload" "Pod pod-l2 (cpu: 50m, mem: 64Mi, schedulerName: kube-sentinel-scheduler)" "Binds to any healthy worker" "Bound to ${BOUND_NODE}" "PASS"
else
    archive_pod "pod-l2"
    record_scenario "L2" "Ordinary eligible workload" "Pod pod-l2 (cpu: 50m, mem: 64Mi, schedulerName: kube-sentinel-scheduler)" "Binds to any healthy worker" "Failed to schedule within 30s" "FAIL"
    exit 1
fi
kubectl delete pod pod-l2 --now >/dev/null 2>&1

# L3: Protected latency-critical workload (Disallowed on Spot)
echo "Running L3: Protected latency-critical workload..."
refresh_heartbeats
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l3
  labels:
    kube-sentinel.io/workload-type: "latency-critical"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l3" 30); then
    archive_pod "pod-l3"
    if [[ "${BOUND_NODE}" == "${W_SPOT}" ]]; then
        record_scenario "L3" "Protected latency-critical workload" "Pod pod-l3 (latency-critical)" "Must NOT bind to Spot node (${W_SPOT})" "VIOLATION: Bound to Spot node ${BOUND_NODE}" "FAIL"
        exit 1
    else
        record_scenario "L3" "Protected latency-critical workload" "Pod pod-l3 (latency-critical)" "Must bind to on-demand node" "Correctly bound to on-demand node ${BOUND_NODE}" "PASS"
    fi
else
    archive_pod "pod-l3"
    record_scenario "L3" "Protected latency-critical workload" "Pod pod-l3 (latency-critical)" "Must bind to on-demand node" "Failed to schedule" "FAIL"
    exit 1
fi
kubectl delete pod pod-l3 --now >/dev/null 2>&1

# L4: Explicit cost ceiling ($1.00 ceiling excludes $W_EXP at $2.50)
echo "Running L4: Explicit cost ceiling enforcement..."
refresh_heartbeats
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l4
  labels:
    kube-sentinel.io/workload-type: "latency-critical"
  annotations:
    sentinel.cost/ceiling-usd: "1.00"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l4" 30); then
    archive_pod "pod-l4"
    if [[ "${BOUND_NODE}" == "${W_CHEAP}" ]]; then
        record_scenario "L4" "Explicit cost ceiling (\$1.00)" "Pod pod-l4 (latency-critical, cost ceiling \$1.00)" "Must bind to ${W_CHEAP} (\$0.40)" "Bound to ${BOUND_NODE}" "PASS"
    else
        record_scenario "L4" "Explicit cost ceiling (\$1.00)" "Pod pod-l4 (latency-critical, cost ceiling \$1.00)" "Must bind to ${W_CHEAP} (\$0.40)" "Bound to unexpected node ${BOUND_NODE}" "FAIL"
        exit 1
    fi
else
    archive_pod "pod-l4"
    record_scenario "L4" "Explicit cost ceiling (\$1.00)" "Pod pod-l4 (latency-critical, cost ceiling \$1.00)" "Must bind to ${W_CHEAP} (\$0.40)" "Failed to schedule" "FAIL"
    exit 1
fi
kubectl delete pod pod-l4 --now >/dev/null 2>&1

# L5: One malformed-telemetry worker plus a healthy worker
echo "Running L5: Malformed telemetry worker exclusion..."
refresh_heartbeats
# Inject corrupted free memory exceeding allocatable into expensive worker (1000GB > 32GB)
kubectl annotate node "${W_EXP}" kube-sentinel.io/reported-free-memory-gb="1000.0" --overwrite

kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l5
  labels:
    kube-sentinel.io/workload-type: "latency-critical"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l5" 30); then
    archive_pod "pod-l5"
    if [[ "${BOUND_NODE}" == "${W_CHEAP}" ]]; then
        record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} free_mem=1000GB > allocatable=32GB" "Must avoid ${W_EXP} and bind to ${W_CHEAP}" "Bound to healthy ${BOUND_NODE}" "PASS"
    else
        record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} free_mem=1000GB > allocatable=32GB" "Must avoid ${W_EXP}" "Unexpected binding to ${BOUND_NODE}" "FAIL"
        exit 1
    fi
else
    archive_pod "pod-l5"
    record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} corrupted telemetry" "Must bind to healthy node" "Failed to schedule" "FAIL"
    exit 1
fi
# Restore valid telemetry
kubectl annotate node "${W_EXP}" kube-sentinel.io/reported-free-memory-gb="24.0" --overwrite
kubectl delete pod pod-l5 --now >/dev/null 2>&1

# L6: One stale-heartbeat worker plus a healthy worker
echo "Running L6: Stale heartbeat worker exclusion..."
refresh_heartbeats
# Inject stale heartbeat (2 hours ago) into expensive worker
STALE_TIME=$(( $(date +%s) - 7200 ))
kubectl annotate node "${W_EXP}" kube-sentinel.io/last-heartbeat-timestamp="${STALE_TIME}" --overwrite

kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l6
  labels:
    kube-sentinel.io/workload-type: "latency-critical"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l6" 30); then
    archive_pod "pod-l6"
    if [[ "${BOUND_NODE}" == "${W_CHEAP}" ]]; then
        record_scenario "L6" "Stale heartbeat worker exclusion" "Node ${W_EXP} heartbeat=${STALE_TIME} (stale >30s)" "Must avoid ${W_EXP} and bind to ${W_CHEAP}" "Bound to healthy ${BOUND_NODE}" "PASS"
    else
        record_scenario "L6" "Stale heartbeat worker exclusion" "Node ${W_EXP} stale heartbeat" "Must avoid ${W_EXP}" "Unexpected binding to ${BOUND_NODE}" "FAIL"
        exit 1
    fi
else
    archive_pod "pod-l6"
    record_scenario "L6" "Stale heartbeat worker exclusion" "Node ${W_EXP} stale heartbeat" "Must bind to healthy node" "Failed to schedule" "FAIL"
    exit 1
fi
# Restore fresh heartbeat
kubectl annotate node "${W_EXP}" kube-sentinel.io/last-heartbeat-timestamp="$(date +%s)" --overwrite
kubectl delete pod pod-l6 --now >/dev/null 2>&1

# L7: All workers violate a mandatory requirement (Must remain Pending)
echo "Running L7: Impossible constraint all-node rejection..."
refresh_heartbeats
# Cost ceiling $0.05 is below all nodes ($0.15, $0.40, $2.50)
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l7
  annotations:
    sentinel.cost/ceiling-usd: "0.05"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

sleep 8
archive_pod "pod-l7"
POD_NODE=$(kubectl get pod pod-l7 -o jsonpath='{.spec.nodeName}')
POD_STATUS=$(kubectl get pod pod-l7 -o jsonpath='{.status.phase}')

if [[ -z "${POD_NODE}" && "${POD_STATUS}" == "Pending" ]]; then
    record_scenario "L7" "Mandatory constraint all-worker rejection" "Cost ceiling \$0.05 (all nodes >= \$0.15)" "Remains unbound (Pending)" "Pending without binding as expected" "PASS"
else
    record_scenario "L7" "Mandatory constraint all-worker rejection" "Cost ceiling \$0.05 (all nodes >= \$0.15)" "Remains unbound" "VIOLATION: Scheduled to ${POD_NODE}" "FAIL"
    exit 1
fi
kubectl delete pod pod-l7 --now >/dev/null 2>&1

# L8: Sidecar failure and recovery across new submissions
echo "Running L8: Daemon failure and recovery across workload submissions..."
refresh_heartbeats

SCHED_POD=$(kubectl get pods -n sentinel-system -l app=kube-sentinel-scheduler -o jsonpath='{.items[0].metadata.name}')
LOG_START_OUTAGE=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | wc -l | tr -d ' ')
RESTART_COUNT_BEFORE=$(kubectl get pod "${SCHED_POD}" -n sentinel-system -o jsonpath='{.status.containerStatuses[?(@.name=="sidecar")].restartCount}')

echo "Injecting fault: sending SIGTERM to sidecar daemon (PID 1) in ${SCHED_POD}..."
kubectl exec -n sentinel-system "${SCHED_POD}" -c sidecar -- kill -TERM 1 || true
sleep 1

# Submit protected workload during outage
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l8-outage
  labels:
    kube-sentinel.io/workload-type: "latency-critical"
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l8-outage" 30); then
    archive_pod "pod-l8-outage"
    if [[ "${BOUND_NODE}" == "${W_SPOT}" ]]; then
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar kill; workload: latency-critical" "Preserve on-demand placement during outage" "VIOLATION: Bound to Spot node during outage" "FAIL"
        exit 1
    fi
    echo "Protected pod safely bound during daemon outage to on-demand node ${BOUND_NODE}"
    
    # PROVE DAEMON UNAVAILABILITY: Assert fallback scoring / open breaker was logged during this binding
    OUTAGE_LOGS=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | tail -n +"$((LOG_START_OUTAGE + 1))")
    if ! echo "${OUTAGE_LOGS}" | grep -q -E "(Fallback scoring active|circuit breaker open)"; then
        echo "ERROR: Scheduler logs did not record fallback scoring during pod-l8-outage binding!"
        echo "Logs during outage window:"
        echo "${OUTAGE_LOGS}"
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar kill; workload: latency-critical" "Prove daemon unavailability (fallback active) during binding" "FAIL: Fallback scoring not recorded in scheduler logs" "FAIL"
        exit 1
    fi
    echo "  [PROVEN] Scheduler entered fallback scoring during daemon outage."
else
    archive_pod "pod-l8-outage"
    record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar kill; workload: latency-critical" "Preserve on-demand placement during outage" "Failed to bind during outage fallback" "FAIL"
    exit 1
fi
kubectl delete pod pod-l8-outage --now >/dev/null 2>&1

# Wait for sidecar process to respawn / container restart
echo "Waiting for sidecar container restart and readiness..."
kubectl wait --for=condition=Ready pod/"${SCHED_POD}" -n sentinel-system --timeout=60s

# Verify restartCount has advanced (proving container was actually terminated and recreated)
RESTART_COUNT_AFTER=$(kubectl get pod "${SCHED_POD}" -n sentinel-system -o jsonpath='{.status.containerStatuses[?(@.name=="sidecar")].restartCount}')
if [[ "${RESTART_COUNT_AFTER}" -le "${RESTART_COUNT_BEFORE}" ]]; then
    echo "WARNING: Sidecar restartCount did not advance (before=${RESTART_COUNT_BEFORE}, after=${RESTART_COUNT_AFTER})"
fi

# Allow circuit breaker backoff duration (2s configured) to expire so it can transition HalfOpen -> Closed
sleep 4

LOG_START_RECOVERY=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | wc -l | tr -d ' ')
SIDECAR_START_RECOVERY=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c sidecar 2>/dev/null | wc -l | tr -d ' ')

# Submit post-recovery workload
refresh_heartbeats
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-l8-recovered
spec:
  schedulerName: kube-sentinel-scheduler
  restartPolicy: Never
  containers:
    - name: worker
      image: alpine:3.19
      command: ["sleep", "300"]
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
EOF

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l8-recovered" 30); then
    archive_pod "pod-l8-recovered"
    
    # PROVE SUCCESSFUL NATIVE SCORING AFTER RESTART
    RECOVERY_SCHED_LOGS=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | tail -n +"$((LOG_START_RECOVERY + 1))")
    RECOVERY_SIDECAR_LOGS=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c sidecar 2>/dev/null | tail -n +"$((SIDECAR_START_RECOVERY + 1))")
    
    if ! echo "${RECOVERY_SCHED_LOGS}" | grep -q "Native scoring active"; then
        echo "ERROR: Scheduler did not perform native scoring for pod-l8-recovered after restart!"
        echo "Scheduler logs:"
        echo "${RECOVERY_SCHED_LOGS}"
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar killed & recovered; post-recovery pod" "Prove native scoring active after restart" "FAIL: Native scoring not recorded after restart" "FAIL"
        exit 1
    fi
    
    if echo "${RECOVERY_SCHED_LOGS}" | grep -q "Fallback scoring active"; then
        echo "ERROR: Fallback scoring was still active for pod-l8-recovered!"
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar killed & recovered; post-recovery pod" "Prove native scoring active after restart" "FAIL: Fallback scoring still active after recovery" "FAIL"
        exit 1
    fi
    
    if ! echo "${RECOVERY_SIDECAR_LOGS}" | grep -q "pod-l8-recovered"; then
        echo "ERROR: Sidecar daemon logs do not record evaluation of pod-l8-recovered!"
        echo "Sidecar logs:"
        echo "${RECOVERY_SIDECAR_LOGS}"
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar killed & recovered; post-recovery pod" "Prove sidecar daemon evaluated pod-l8-recovered" "FAIL: Sidecar logs lack pod-l8-recovered evaluation" "FAIL"
        exit 1
    fi
    
    echo "  [PROVEN] Native gRPC scoring successfully resumed after daemon restart."
    record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar killed & recovered; post-recovery pod" "Prove fallback during outage; prove native scoring after restart" "Outage preserved on-demand with proven fallback; restart resumed proven native scoring on ${BOUND_NODE}" "PASS"
else
    archive_pod "pod-l8-recovered"
    record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar killed & recovered; post-recovery pod" "Normal scheduling resumes" "Failed to schedule after recovery" "FAIL"
    exit 1
fi
kubectl delete pod pod-l8-recovered --now >/dev/null 2>&1

# Write final scenarios.json and run.json
echo "${SCENARIOS_JSON}" | python3 -m json.tool > "${ARTIFACTS_DIR}/scenarios.json"

cat <<EOF > "${ARTIFACTS_DIR}/run.json"
{
  "run_id": "${RUN_ID}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "git_commit": "$(git rev-parse HEAD)",
  "images": {
    "scheduler": "localhost/kube-sentinel-scheduler:local",
    "sidecar": "localhost/kube-sentinel-sidecar:local"
  },
  "versions": {
    "kubernetes": "$(kubectl version -o json | python3 -c 'import sys, json; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')",
    "go": "$(go version | awk '{print $3}')",
    "python": "$(python3 --version | awk '{print $2}')"
  },
  "config": {
    "cluster_name": "${CLUSTER_NAME}",
    "nodes": {
      "control_plane": 1,
      "workers": 3,
      "roles": {
        "cheap_on_demand": "${W_CHEAP} (\$0.40/hr)",
        "expensive_on_demand": "${W_EXP} (\$2.50/hr)",
        "spot": "${W_SPOT} (\$0.15/hr, cloud.google.com/gke-spot=true)"
      }
    },
    "circuit_breaker": {
      "failure_threshold": 2,
      "timeout_seconds": 2
    }
  },
  "total_scenarios": 8,
  "passed_scenarios": 8,
  "overall_outcome": "PASS"
}
EOF

echo ""
echo "================================================================================"
echo "ACCEPTANCE SUMMARY: 8/8 SCENARIOS PASSED"
echo "Artifacts written to: ${ARTIFACTS_DIR}"
echo "================================================================================"
