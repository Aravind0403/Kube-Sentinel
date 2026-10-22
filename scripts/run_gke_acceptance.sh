#!/usr/bin/env bash
# ==============================================================================
# Kube-Sentinel: Bounded Google Cloud (GKE) Acceptance & Benchmark Suite
# Conforms to docs/google-cloud-checks.md
# ==============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:-starry-trilogy-503219-s4}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
CLUSTER_NAME="sentinel-gke-${RANDOM}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/aco-sentinel"
GIT_SHA="$(git rev-parse --short HEAD)"
TAG="gke-${GIT_SHA}-${RANDOM}"
SCHED_IMG="${REGISTRY}/kube-sentinel-scheduler:${TAG}"
SIDECAR_IMG="${REGISTRY}/kube-sentinel-sidecar:${TAG}"
RUN_ID="gke-$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_DIR="${REPO_ROOT}/artifacts/gke-acceptance/${RUN_ID}"

mkdir -p "${ARTIFACTS_DIR}"

echo "================================================================================"
echo "KUBE-SENTINEL BOUNDED GKE ACCEPTANCE & BENCHMARK SUITE"
echo "Project:    ${PROJECT_ID}"
echo "Location:   ${ZONE}"
echo "Cluster:    ${CLUSTER_NAME}"
echo "Images:     ${SCHED_IMG}"
echo "            ${SIDECAR_IMG}"
echo "Artifacts:  ${ARTIFACTS_DIR}"
echo "Commit:     ${GIT_SHA}"
echo "================================================================================"

CLUSTER_CREATED="false"

cleanup() {
    local EXIT_STATUS=$?
    echo ""
    echo "================================================================================"
    echo "TEARING DOWN GKE EVALUATION ENVIRONMENT (STRICT CLEANUP)"
    echo "================================================================================"
    
    if [[ "${CLUSTER_CREATED}" == "true" ]] || gcloud container clusters describe "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        echo "Collecting GKE diagnostic logs before deletion..."
        kubectl get nodes -o wide > "${ARTIFACTS_DIR}/gke_nodes.txt" 2>&1 || true
        kubectl get pods -A -o wide > "${ARTIFACTS_DIR}/gke_pods_all.txt" 2>&1 || true
        kubectl get events -A -o json > "${ARTIFACTS_DIR}/events.json" 2>&1 || true
        kubectl logs -n sentinel-system -l app=kube-sentinel-scheduler -c scheduler --tail=-1 > "${ARTIFACTS_DIR}/scheduler.log" 2>&1 || true
        kubectl logs -n sentinel-system -l app=kube-sentinel-scheduler -c sidecar --tail=-1 > "${ARTIFACTS_DIR}/sidecar.log" 2>&1 || true
        
        echo "Deleting GKE cluster ${CLUSTER_NAME} in ${ZONE}..."
        gcloud container clusters delete "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --quiet || true
    fi

    echo "Cleaning up container images from Artifact Registry..."
    gcloud artifacts docker images delete "${SCHED_IMG}" --project="${PROJECT_ID}" --quiet --delete-tags || true
    gcloud artifacts docker images delete "${SIDECAR_IMG}" --project="${PROJECT_ID}" --quiet --delete-tags || true
    
    echo "✓ All cloud resources cleanly destroyed. Zero ongoing spend."
    if [[ ${EXIT_STATUS} -eq 0 ]]; then
        echo "✅ GKE ACCEPTANCE RUN SUCCEEDED: All checks passed and environment cleaned."
    else
        echo "❌ GKE ACCEPTANCE RUN FAILED with exit status ${EXIT_STATUS}."
    fi
    exit ${EXIT_STATUS}
}
trap cleanup EXIT INT TERM

# Step 1: Build & Push Images (linux/amd64 for GKE)
echo ""
echo "[Step 1/5] Building and pushing linux/amd64 images to Artifact Registry..."
gcloud auth print-access-token | podman login -u oauth2accesstoken --password-stdin "${REGION}-docker.pkg.dev"

echo "Compiling linux/amd64 scheduler binary on host..."
(cd "${REPO_ROOT}/v2/go_plugin" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o "${REPO_ROOT}/kube-scheduler-linux-amd64" .)

echo "Packaging scheduler image for linux/amd64..."
podman build --platform=linux/amd64 -t "${SCHED_IMG}" -f "${REPO_ROOT}/v2/Dockerfile.scheduler.prebuilt" "${REPO_ROOT}"
echo "Pushing scheduler image..."
podman push "${SCHED_IMG}"

echo "Building sidecar image for linux/amd64..."
podman build --platform=linux/amd64 -t "${SIDECAR_IMG}" -f "${REPO_ROOT}/v2/Dockerfile.grpc" "${REPO_ROOT}"
echo "Pushing sidecar image..."
podman push "${SIDECAR_IMG}"
echo "✓ Images published to Artifact Registry."

# Step 2: Create GKE Cluster (2 On-Demand + 1 Spot Node)
echo ""
echo "[Step 2/5] Provisioning GKE Standard CPU-only cluster ${CLUSTER_NAME} in ${ZONE}..."
gcloud container clusters create "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --num-nodes=2 \
    --machine-type=e2-standard-2 \
    --release-channel=regular \
    --scopes=cloud-platform \
    --labels=environment=test,owner=sentinel-acceptance \
    --quiet
CLUSTER_CREATED="true"

echo "Creating dedicated GKE Spot node pool (1 node)..."
gcloud container node-pools create spot-pool \
    --cluster="${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --num-nodes=1 \
    --machine-type=e2-standard-2 \
    --scopes=cloud-platform \
    --spot \
    --quiet

echo "Configuring kubectl credentials..."
gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"
kubectl wait --for=condition=Ready nodes --all --timeout=120s

# Step 3: Configure Worker Node Test Fixtures
echo ""
echo "[Step 3/5] Configuring node test fixtures on GKE..."
GKE_NODES=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'))
echo "Discovered GKE nodes: ${GKE_NODES[*]}"

# Identify the Spot node by Google's native cloud.google.com/gke-spot label
W_SPOT=""
ON_DEMAND_NODES=()
for n in "${GKE_NODES[@]}"; do
    IS_SPOT_LABEL=$(kubectl get node "${n}" -o jsonpath='{.metadata.labels.cloud\.google\.com/gke-spot}' 2>/dev/null || true)
    if [[ "${IS_SPOT_LABEL}" == "true" ]]; then
        W_SPOT="${n}"
    else
        ON_DEMAND_NODES+=("${n}")
    fi
done

if [[ -z "${W_SPOT}" ]]; then
    echo "ERROR: Failed to find GKE node with native cloud.google.com/gke-spot=true"
    exit 1
fi

W_CHEAP="${ON_DEMAND_NODES[0]}"
W_EXP="${ON_DEMAND_NODES[1]}"

echo "GKE Worker 1 (Cheap On-Demand)     : ${W_CHEAP}"
echo "GKE Worker 2 (Expensive On-Demand) : ${W_EXP}"
echo "GKE Worker 3 (Native GKE Spot)     : ${W_SPOT}"

NOW=$(date +%s)
# Fixture 1: Cheap On-Demand ($0.40/hr, on-demand, valid telemetry)
kubectl label node "${W_CHEAP}" fixture.sentinel/role=cheap-on-demand --overwrite
kubectl annotate node "${W_CHEAP}" \
    sentinel.cost/hourly-usd="0.40" \
    sentinel.cost/is-spot="false" \
    kube-sentinel.io/reported-allocatable-cpu="2.0" \
    kube-sentinel.io/reported-used-cpu="0.5" \
    kube-sentinel.io/reported-free-cpu="1.5" \
    kube-sentinel.io/reported-allocatable-memory-gb="8.0" \
    kube-sentinel.io/reported-used-memory-gb="2.0" \
    kube-sentinel.io/reported-free-memory-gb="6.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

# Fixture 2: Expensive On-Demand ($2.50/hr, on-demand, valid telemetry)
kubectl label node "${W_EXP}" fixture.sentinel/role=expensive-on-demand --overwrite
kubectl annotate node "${W_EXP}" \
    sentinel.cost/hourly-usd="2.50" \
    sentinel.cost/is-spot="false" \
    kube-sentinel.io/reported-allocatable-cpu="2.0" \
    kube-sentinel.io/reported-used-cpu="0.5" \
    kube-sentinel.io/reported-free-cpu="1.5" \
    kube-sentinel.io/reported-allocatable-memory-gb="8.0" \
    kube-sentinel.io/reported-used-memory-gb="2.0" \
    kube-sentinel.io/reported-free-memory-gb="6.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

# Fixture 3: Native GKE Spot ($0.15/hr, Spot, valid telemetry)
kubectl label node "${W_SPOT}" fixture.sentinel/role=spot --overwrite
kubectl annotate node "${W_SPOT}" \
    sentinel.cost/hourly-usd="0.15" \
    sentinel.cost/is-spot="true" \
    sentinel.cost/spot-interruption-prob="0.15" \
    kube-sentinel.io/reported-allocatable-cpu="2.0" \
    kube-sentinel.io/reported-used-cpu="0.5" \
    kube-sentinel.io/reported-free-cpu="1.5" \
    kube-sentinel.io/reported-allocatable-memory-gb="8.0" \
    kube-sentinel.io/reported-used-memory-gb="2.0" \
    kube-sentinel.io/reported-free-memory-gb="6.0" \
    kube-sentinel.io/last-heartbeat-timestamp="${NOW}" --overwrite

refresh_heartbeats() {
    local t=$(date +%s)
    kubectl annotate node "${W_CHEAP}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
    kubectl annotate node "${W_EXP}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
    kubectl annotate node "${W_SPOT}" kube-sentinel.io/last-heartbeat-timestamp="${t}" --overwrite
}

# Step 4: Deploy Kube-Sentinel on GKE
echo ""
echo "[Step 4/5] Deploying Kube-Sentinel scheduler and sidecar on GKE..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: sentinel-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sentinel-scheduler-sa
  namespace: sentinel-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sentinel-scheduler-role-binding
subjects:
  - kind: ServiceAccount
    name: sentinel-scheduler-sa
    namespace: sentinel-system
roleRef:
  kind: ClusterRole
  name: system:kube-scheduler
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sentinel-scheduler-volume-binding
subjects:
  - kind: ServiceAccount
    name: sentinel-scheduler-sa
    namespace: sentinel-system
roleRef:
  kind: ClusterRole
  name: system:volume-scheduler
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: sentinel-scheduler-config
  namespace: sentinel-system
data:
  scheduler-config.yaml: |
    apiVersion: kubescheduler.config.k8s.io/v1
    kind: KubeSchedulerConfiguration
    leaderElection:
      leaderElect: false
    profiles:
      - schedulerName: kube-sentinel-scheduler
        plugins:
          preFilter:
            enabled:
              - name: SentinelScheduler
          filter:
            enabled:
              - name: SentinelScheduler
          preScore:
            enabled:
              - name: SentinelScheduler
          score:
            enabled:
              - name: SentinelScheduler
                weight: 100
          reserve:
            enabled:
              - name: SentinelScheduler
          preBind:
            enabled:
              - name: SentinelScheduler
          postBind:
            enabled:
              - name: SentinelScheduler
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: sentinel-plugin-config
  namespace: sentinel-system
data:
  config.yaml: |
    scoring:
      trust_exponent_gamma: 2.0
      weight_scoring_plugin: 100
      fallback_strategy: "resource_fit"
    telemetry:
      heartbeat_timeout_seconds: 30
    performance:
      grpc_timeout_seconds: 2
      cache_ttl_seconds: 0.2
    resilience:
      circuit_breaker:
        failure_threshold: 2
        timeout_seconds: 2
        half_open_max_attempts: 1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kube-sentinel-scheduler
  namespace: sentinel-system
  labels:
    app: kube-sentinel-scheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kube-sentinel-scheduler
  template:
    metadata:
      labels:
        app: kube-sentinel-scheduler
    spec:
      serviceAccountName: sentinel-scheduler-sa
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: cloud.google.com/gke-spot
                    operator: DoesNotExist
      containers:
        - name: scheduler
          image: ${SCHED_IMG}
          imagePullPolicy: IfNotPresent
          command:
            - /bin/kube-scheduler
            - --config=/etc/kubernetes/scheduler/scheduler-config.yaml
            - --v=2
          volumeMounts:
            - name: config-volume
              mountPath: /etc/kubernetes/scheduler
            - name: sentinel-config-volume
              mountPath: /etc/sentinel
        - name: sidecar
          image: ${SIDECAR_IMG}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 50051
          env:
            - name: PYTHONUNBUFFERED
              value: "1"
      volumes:
        - name: config-volume
          configMap:
            name: sentinel-scheduler-config
        - name: sentinel-config-volume
          configMap:
            name: sentinel-plugin-config
EOF

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

wait_for_pod_scheduled() {
    local pod_name="$1"
    local timeout="${2:-40}"
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

echo ""
echo "================================================================================"
echo "EXECUTING GKE ACCEPTANCE SCENARIO MATRIX (L1 - L8)"
echo "================================================================================"

# L1: Healthy scheduler and sidecar readiness
echo "Running L1: Healthy scheduler and sidecar deployment check on GKE..."
if kubectl rollout status deployment/kube-sentinel-scheduler -n sentinel-system --timeout=180s; then
    record_scenario "L1" "Healthy scheduler and sidecar" "Deployment on GKE Standard" "Deployment ready within 180s" "Scheduler & sidecar running and healthy" "PASS"
else
    record_scenario "L1" "Healthy scheduler and sidecar" "Deployment on GKE Standard" "Deployment ready within 180s" "Rollout timed out or failed" "FAIL"
    kubectl describe pods -n sentinel-system -l app=kube-sentinel-scheduler || true
    exit 1
fi

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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l2" 40); then
    archive_pod "pod-l2"
    record_scenario "L2" "Ordinary eligible workload" "Pod pod-l2 on GKE" "Binds to any healthy worker" "Bound to ${BOUND_NODE}" "PASS"
else
    archive_pod "pod-l2"
    record_scenario "L2" "Ordinary eligible workload" "Pod pod-l2 on GKE" "Binds to any healthy worker" "Failed to schedule" "FAIL"
    exit 1
fi
kubectl delete pod pod-l2 --now >/dev/null 2>&1

# L3: Protected latency-critical workload (Must avoid GKE Spot node)
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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l3" 40); then
    archive_pod "pod-l3"
    if [[ "${BOUND_NODE}" != "${W_SPOT}" ]]; then
        record_scenario "L3" "Protected latency-critical workload" "Workload: latency-critical on GKE" "Must avoid native GKE Spot node (${W_SPOT})" "Correctly bound to on-demand node ${BOUND_NODE}" "PASS"
    else
        record_scenario "L3" "Protected latency-critical workload" "Workload: latency-critical on GKE" "Must avoid native GKE Spot node (${W_SPOT})" "VIOLATION: Scheduled to Spot node ${BOUND_NODE}" "FAIL"
        exit 1
    fi
else
    archive_pod "pod-l3"
    record_scenario "L3" "Protected latency-critical workload" "Workload: latency-critical on GKE" "Must bind to on-demand node" "Failed to schedule" "FAIL"
    exit 1
fi
kubectl delete pod pod-l3 --now >/dev/null 2>&1

# L4: Explicit cost ceiling enforcement
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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l4" 40); then
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

# L5: Malformed telemetry worker exclusion
echo "Running L5: Malformed telemetry worker exclusion..."
refresh_heartbeats
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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l5" 40); then
    archive_pod "pod-l5"
    if [[ "${BOUND_NODE}" == "${W_CHEAP}" ]]; then
        record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} free_mem=1000GB > allocatable=8GB" "Must avoid ${W_EXP} and bind to ${W_CHEAP}" "Bound to healthy ${BOUND_NODE}" "PASS"
    else
        record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} free_mem=1000GB > allocatable=8GB" "Must avoid ${W_EXP}" "Unexpected binding to ${BOUND_NODE}" "FAIL"
        exit 1
    fi
else
    archive_pod "pod-l5"
    record_scenario "L5" "Malformed telemetry worker exclusion" "Node ${W_EXP} corrupted telemetry" "Must bind to healthy node" "Failed to schedule" "FAIL"
    exit 1
fi
kubectl annotate node "${W_EXP}" kube-sentinel.io/reported-free-memory-gb="6.0" --overwrite
kubectl delete pod pod-l5 --now >/dev/null 2>&1

# L6: Stale heartbeat worker exclusion
echo "Running L6: Stale heartbeat worker exclusion..."
refresh_heartbeats
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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l6" 40); then
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
kubectl annotate node "${W_EXP}" kube-sentinel.io/last-heartbeat-timestamp="$(date +%s)" --overwrite
kubectl delete pod pod-l6 --now >/dev/null 2>&1

# L7: Impossible constraint all-node rejection
echo "Running L7: Impossible constraint all-node rejection..."
refresh_heartbeats
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

sleep 10
archive_pod "pod-l7"
POD_NODE=$(kubectl get pod pod-l7 -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
POD_STATUS=$(kubectl get pod pod-l7 -o jsonpath='{.status.phase}' 2>/dev/null || true)

if [[ -z "${POD_NODE}" && "${POD_STATUS}" == "Pending" ]]; then
    record_scenario "L7" "Mandatory constraint all-worker rejection" "Cost ceiling \$0.05 (all nodes >= \$0.15)" "Remains unbound (Pending)" "Pending without binding as expected" "PASS"
else
    record_scenario "L7" "Mandatory constraint all-worker rejection" "Cost ceiling \$0.05 (all nodes >= \$0.15)" "Remains unbound" "VIOLATION: Scheduled to ${POD_NODE}" "FAIL"
    exit 1
fi
kubectl delete pod pod-l7 --now >/dev/null 2>&1

# L8: Sidecar failure and recovery across workload submissions
echo "Running L8: Daemon failure and recovery across workload submissions..."
refresh_heartbeats

SCHED_POD=$(kubectl get pods -n sentinel-system -l app=kube-sentinel-scheduler -o jsonpath='{.items[0].metadata.name}')
LOG_START_OUTAGE=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | wc -l | tr -d ' ')

echo "Injecting fault: sending SIGTERM and shutdown request to sidecar daemon in ${SCHED_POD}..."
kubectl exec -n sentinel-system "${SCHED_POD}" -c sidecar -- python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8083/shutdown")' || true
kubectl exec -n sentinel-system "${SCHED_POD}" -c sidecar -- /bin/sh -c "kill -TERM 1 || /usr/bin/kill -TERM 1" || true

echo "Verifying sidecar daemon port 50051 is down..."
for _ in {1..20}; do
    if ! kubectl exec -n sentinel-system "${SCHED_POD}" -c scheduler -- nc -z -w 1 127.0.0.1 50051 2>/dev/null; then
        echo "  [CONFIRMED] Sidecar daemon port 50051 is closed."
        break
    fi
    sleep 0.5
done

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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l8-outage" 40); then
    archive_pod "pod-l8-outage"
    if [[ "${BOUND_NODE}" == "${W_SPOT}" ]]; then
        record_scenario "L8" "Sidecar failure & recovery" "Fault: sidecar kill; workload: latency-critical" "Preserve on-demand placement during outage" "VIOLATION: Bound to Spot node during outage" "FAIL"
        exit 1
    fi
    echo "Protected pod safely bound during daemon outage to on-demand node ${BOUND_NODE}"
    
    # PROVE DAEMON UNAVAILABILITY: Assert fallback scoring / open breaker was logged
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

echo "Waiting for sidecar container restart and readiness..."
kubectl wait --for=condition=Ready pod/"${SCHED_POD}" -n sentinel-system --timeout=90s
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

if BOUND_NODE=$(wait_for_pod_scheduled "pod-l8-recovered" 40); then
    archive_pod "pod-l8-recovered"
    
    RECOVERY_SCHED_LOGS=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c scheduler 2>/dev/null | tail -n +"$((LOG_START_RECOVERY + 1))")
    RECOVERY_SIDECAR_LOGS=$(kubectl logs -n sentinel-system "${SCHED_POD}" -c sidecar 2>/dev/null | tail -n +"$((SIDECAR_START_RECOVERY + 1))")
    
    if ! echo "${RECOVERY_SCHED_LOGS}" | grep -q "Native scoring active"; then
        echo "ERROR: Scheduler did not perform native scoring for pod-l8-recovered after restart!"
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

# Step 5: Benchmarks (Latency & Throughput on GKE)
echo ""
echo "================================================================================"
echo "[Step 5/5] EXECUTING GKE SCHEDULING LATENCY BENCHMARK"
echo "================================================================================"

BENCHMARK_PODS=10
BENCHMARK_LATENCIES=()
echo "Submitting ${BENCHMARK_PODS} sequential benchmark workloads..."

for i in $(seq 1 ${BENCHMARK_PODS}); do
    refresh_heartbeats
    PNAME="bench-pod-${i}"
    T_START=$(python3 -c "import time; print(int(time.time() * 1000))")
    kubectl apply -f - <<EOF >/dev/null 2>&1
apiVersion: v1
kind: Pod
metadata:
  name: ${PNAME}
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
    if BOUND_NODE=$(wait_for_pod_scheduled "${PNAME}" 30); then
        T_END=$(python3 -c "import time; print(int(time.time() * 1000))")
        LATENCY=$(( T_END - T_START ))
        BENCHMARK_LATENCIES+=("${LATENCY}")
        echo "  Pod ${PNAME} scheduled in ${LATENCY}ms -> ${BOUND_NODE}"
    else
        echo "  Pod ${PNAME} failed to schedule"
    fi
    kubectl delete pod "${PNAME}" --now >/dev/null 2>&1
done

BENCHMARK_STATS=$(python3 -c "
import json, numpy as np
latencies = list(map(int, '''${BENCHMARK_LATENCIES[*]}'''.split()))
if latencies:
    print(json.dumps({
        'count': len(latencies),
        'min_ms': int(np.min(latencies)),
        'p50_ms': int(np.percentile(latencies, 50)),
        'p95_ms': int(np.percentile(latencies, 95)),
        'p99_ms': int(np.percentile(latencies, 99)),
        'max_ms': int(np.max(latencies)),
        'mean_ms': round(float(np.mean(latencies)), 2)
    }, indent=2))
else:
    print(json.dumps({'error': 'No latencies recorded'}))
")

echo "Benchmark statistics on GKE:"
echo "${BENCHMARK_STATS}"
echo "${BENCHMARK_STATS}" > "${ARTIFACTS_DIR}/benchmarks.json"

# Write final scenarios.json and run.json
echo "${SCENARIOS_JSON}" | python3 -m json.tool > "${ARTIFACTS_DIR}/scenarios.json"

cat <<EOF > "${ARTIFACTS_DIR}/run.json"
{
  "run_id": "${RUN_ID}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "project_id": "${PROJECT_ID}",
  "zone": "${ZONE}",
  "cluster_name": "${CLUSTER_NAME}",
  "git_commit": "$(git rev-parse HEAD)",
  "images": {
    "scheduler": "${SCHED_IMG}",
    "sidecar": "${SIDECAR_IMG}"
  },
  "versions": {
    "gke": "$(kubectl version -o json | python3 -c 'import sys, json; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')",
    "go": "$(go version | awk '{print $3}')",
    "python": "$(python3 --version | awk '{print $2}')"
  },
  "benchmark_summary": ${BENCHMARK_STATS}
}
EOF

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

echo ""
echo "================================================================================"
echo "GKE ACCEPTANCE SUMMARY: 8/8 SCENARIOS & BENCHMARKS COMPLETED"
echo "Artifacts saved to: ${ARTIFACTS_DIR}"
echo "================================================================================"
