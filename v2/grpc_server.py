"""
v2/grpc_server.py
─────────────────
Phase 3: Python gRPC Server for ACO-Sentinel.

Serves the cost engine, workload predictor, and confidence tracker logic
over gRPC to the Go scheduler plugin.
"""

from __future__ import annotations

import os
import sys
import time
import json
import yaml
import signal
import threading
import http.server
import math
from concurrent import futures
from typing import Dict

import grpc

# Add project root and libraries to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "core")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "proto")))

import sentinel_pb2
import sentinel_pb2_grpc
from confidence import (
    NodeConfidenceTracker,
    AdaptiveThresholdManager,
    calculate_freshness,
    calculate_heartbeat_consistency,
    calculate_internal_consistency,
    calculate_cross_consistency,
)
from orchestrator.control_plane.cost_engine import CostEngine
from orchestrator.shared.models import (
    ComputeNode,
    InstanceType,
    JobRequest,
    NodeArch,
    NodeCostProfile,
    NodeTelemetry,
    ResourceRequest,
    WorkloadType,
)


def normalize_arch(arch_str: Optional[str]) -> Optional[str]:
    """Canonical normalization for CPU architecture identifiers."""
    if not arch_str:
        return None
    s = str(arch_str).lower().strip()
    if s in ("amd64", "x86_64", "x86-64", "x86", "x64"):
        return "amd64"
    if s in ("arm64", "aarch64", "arm"):
        return "arm64"
    return s


class ConfigWatcher:
    """
    Background file-watcher reloading yaml configuration dynamically.
    """
    def __init__(self, path: str = "v2/sentinel-config.yaml") -> None:
        self.path = path
        # Try local path or root path
        if not os.path.exists(self.path):
            self.path = "sentinel-config.yaml"
        self.config: dict = {}
        self.load_config()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def load_config(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self.config = yaml.safe_load(f) or {}
                print(f"[Sentinel-ConfigWatcher] Configuration loaded from {self.path}")
            except Exception as e:
                print(f"[Sentinel-ConfigWatcher] Error loading config: {e}")

    def _watch_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(30)
            self.load_config()

    def stop(self) -> None:
        self._stop_event.set()


class StatePersistence:
    """
    Periodically flushes and recovers committed placement state to disk atomically.
    """
    def __init__(self, server: ACOPredictiveSchedulerServicer, path: str = "v2/sentinel_state.json") -> None:
        self.server = server
        self.path = path
        if not os.path.exists(os.path.dirname(self.path)) and os.path.dirname(self.path):
            self.path = "sentinel_state.json"
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._save_loop, daemon=True)
        self._thread.start()

    def _save_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(300)  # Flush every 5 minutes
            self.save()

    def save(self) -> None:
        data = {
            "committed_placements": self.server.committed_placements,
            "total_deposits": self.server.total_deposits,
            "phantom_deposits": self.server.phantom_deposits,
            "timestamp": time.time()
        }
        try:
            tmp_path = self.path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.rename(tmp_path, self.path)
            print(f"[Sentinel-Persistence] State flushed atomically to {self.path}")
        except Exception as e:
            print(f"[Sentinel-Persistence] Error writing state: {e}")

    def load(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    data = json.load(f)
                self.server.committed_placements = data.get("committed_placements", {})
                self.server.total_deposits = data.get("total_deposits", 0)
                self.server.phantom_deposits = data.get("phantom_deposits", 0)
                print(f"[Sentinel-Persistence] State loaded successfully from {self.path}")
            except Exception as e:
                print(f"[Sentinel-Persistence] Error loading state: {e}")

    def stop(self) -> None:
        self._stop_event.set()


class FederationHTTPServer:
    """
    Exposes a lightweight, read-only HTTP server for pulling cluster placements.
    """
    def __init__(self, server: ACOPredictiveSchedulerServicer, port: int = 8083) -> None:
        self.server = server
        self.port = port

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(h) -> None:
                if h.path == "/pheromones":
                    h.send_response(200)
                    h.send_header("Content-Type", "application/json")
                    h.end_headers()
                    response = {
                        "placements": self.server.committed_placements,
                        "total_deposits": self.server.total_deposits,
                        "phantom_deposits": self.server.phantom_deposits
                    }
                    h.wfile.write(json.dumps(response).encode())
                else:
                    h.send_response(404)
                    h.end_headers()

            def log_message(h, format, *args) -> None:
                pass  # Suppress request logging to keep server console clean

        try:
            self.httpd = http.server.HTTPServer(("0.0.0.0", self.port), Handler)
            self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self._thread.start()
            print(f"[Sentinel-FederationServer] Serving metrics on port {self.port}...")
        except Exception as e:
            print(f"[Sentinel-FederationServer] WARNING: Failed to start HTTP server: {e}")

    def stop(self) -> None:
        self.httpd.shutdown()


class ACOPredictiveSchedulerServicer(sentinel_pb2_grpc.ACOPredictiveSchedulerServicer):
    """
    gRPC service coordinating custom scoring and placement checks.
    """

    def __init__(self) -> None:
        self.cost_engine = CostEngine()
        self.confidence_trackers: Dict[str, NodeConfidenceTracker] = {}
        self.committed_placements: Dict[str, str] = {}
        self.phantom_deposits = 0
        self.total_deposits = 0
        self.threshold_manager = AdaptiveThresholdManager()
        self.config_watcher = ConfigWatcher()
        self.persistence = StatePersistence(self)
        self.persistence.load()
        self.fed_server = FederationHTTPServer(self)

    def ScoreNodes(
        self,
        request: sentinel_pb2.ScoreRequest,
        context: grpc.ServicerContext,
    ) -> sentinel_pb2.ScoreResponse:
        """
        Score a list of node candidates for a given pod spec.
        """
        if request.pod.uid == "healthz":
            return sentinel_pb2.ScoreResponse()
        if request.pod.uid == "reset":
            self.confidence_trackers.clear()
            self.committed_placements.clear()
            self.phantom_deposits = 0
            self.total_deposits = 0
            return sentinel_pb2.ScoreResponse()

        # Collect intervals and calculate CVs for active threshold management
        cv_values = []
        for n in request.nodes:
            if n.recent_heartbeat_intervals:
                intervals = list(n.recent_heartbeat_intervals)
                if len(intervals) >= 2:
                    mean = sum(intervals) / len(intervals)
                    if mean > 1e-6:
                        variance = sum((x - mean) ** 2 for x in intervals) / len(intervals)
                        std_dev = math.sqrt(variance)
                        cv_values.append(std_dev / mean)

        if cv_values:
            avg_cv = sum(cv_values) / len(cv_values)
            self.threshold_manager.evaluate_state(avg_cv)

        # Get dynamic thresholds
        thresholds = self.threshold_manager.get_thresholds()
        cv_max = thresholds["cv_max"]
        t_max = thresholds["t_max"]

        # Map workload type string to enum (case and separator insensitive)
        w_raw = request.pod.workload_type.upper().replace("-", "_")
        if "LATENCY" in w_raw or "CRITICAL" in w_raw:
            w_type = WorkloadType.LATENCY_CRITICAL
        elif "STREAM" in w_raw:
            w_type = WorkloadType.STREAM
        else:
            w_type = WorkloadType.BATCH

        # Parse architecture requirement with normalization (V04 fix)
        arch_req = None
        norm_req_arch = normalize_arch(request.pod.required_arch)
        if norm_req_arch == "arm64":
            arch_req = NodeArch.ARM64
        elif norm_req_arch == "amd64":
            arch_req = NodeArch.X86_64

        # Re-construct JobRequest object for CostEngine with full typed constraints (F03, F07 fixes)
        has_gpu = request.pod.gpu_requested > 0
        job = JobRequest(
            job_id=request.pod.uid,
            workload_type=w_type,
            resources=ResourceRequest(
                cpu_cores_min=request.pod.cpu_cores_requested,
                memory_gb_min=request.pod.memory_gb_requested,
                gpu_required=has_gpu,
                gpu_count=int(request.pod.gpu_requested) if has_gpu else 1,
                gpu_memory_gb=request.pod.min_gpu_memory_gb if request.pod.min_gpu_memory_gb > 0 else None,
            ),
            gpu_model_preferred=request.pod.gpu_type_required if request.pod.gpu_type_required else None,
            arch_required=arch_req,
            cost_ceiling_usd=request.pod.cost_ceiling_usd if request.pod.cost_ceiling_usd > 0 else None,
        )

        # Use true wall-clock time (F08 fix)
        current_time = time.time()

        scores = []
        for node_cand in request.nodes:
            node_id = node_cand.node_id

            # Initialize tracker if not exists
            if node_id not in self.confidence_trackers:
                self.confidence_trackers[node_id] = NodeConfidenceTracker(
                    node_id=node_id,
                    alpha=0.5,
                    initial_confidence=0.7,
                    t_max=t_max,
                    cv_max=cv_max,
                )
            tracker = self.confidence_trackers[node_id]
            tracker.cv_max = cv_max
            tracker.t_max = t_max

            # 1. Internal consistency on both CPU and Memory (F14 fix)
            k_int_cpu = calculate_internal_consistency(
                node_cand.reported_allocatable_cpu,
                node_cand.reported_used_cpu,
                node_cand.reported_free_cpu,
            )
            if node_cand.reported_allocatable_memory_gb > 0:
                k_int_mem = calculate_internal_consistency(
                    node_cand.reported_allocatable_memory_gb,
                    node_cand.reported_used_memory_gb,
                    node_cand.reported_free_memory_gb,
                )
            else:
                k_int_mem = 1.0
            k_internal = k_int_cpu * k_int_mem

            # 2. Freshness check against real server time (F08, V04 fixes)
            if node_cand.last_heartbeat_timestamp > 0:
                raw_delta = current_time - node_cand.last_heartbeat_timestamp
                # If heartbeat is more than 1.0s in the future, reject as invalid clock anomaly
                if raw_delta < -1.0:
                    k_fresh = 0.0
                else:
                    delta_t = max(0.0, raw_delta)
                    k_fresh = calculate_freshness(delta_t, tracker.t_max)
            else:
                k_fresh = 0.0

            # 3. Heartbeat arrival jitter / cadence check
            if node_cand.recent_heartbeat_intervals:
                k_heartbeat = calculate_heartbeat_consistency(
                    list(node_cand.recent_heartbeat_intervals), tracker.cv_max
                )
            else:
                k_heartbeat = 1.0

            # 4. Cross-scheduler consistency on both CPU and Memory (F14 fix)
            k_cross_cpu = calculate_cross_consistency(
                node_cand.allocatable_cpu,
                node_cand.scheduler_expected_free_cpu,
                node_cand.reported_free_cpu,
            )
            if node_cand.allocatable_memory_gb > 0:
                k_cross_mem = calculate_cross_consistency(
                    node_cand.allocatable_memory_gb,
                    node_cand.scheduler_expected_free_memory_gb,
                    node_cand.reported_free_memory_gb,
                )
            else:
                k_cross_mem = 1.0
            k_cross = k_cross_cpu * k_cross_mem

            # Composite raw confidence product
            raw_conf = k_internal * k_fresh * k_heartbeat * k_cross

            # Update tracker only if this is a newly observed heartbeat (F12 fix)
            if node_cand.last_heartbeat_timestamp > 0 and node_cand.last_heartbeat_timestamp > tracker.last_heartbeat_timestamp:
                tracker.record_heartbeat(node_cand.last_heartbeat_timestamp)
                kappa_i = tracker.ema.update(raw_conf)
            else:
                # Same observation; do not artificially advance EMA on every query
                kappa_i = tracker.ema.value * k_fresh

            # Re-construct NodeTelemetry
            node_telemetry = NodeTelemetry(
                node_id=node_id,
                cpu_util_pct=(
                    (node_cand.reported_used_cpu / node_cand.reported_allocatable_cpu * 100.0)
                    if node_cand.reported_allocatable_cpu > 0
                    else 0.0
                ),
                memory_util_pct=(
                    (
                        node_cand.reported_used_memory_gb
                        / node_cand.reported_allocatable_memory_gb
                        * 100.0
                    )
                    if node_cand.reported_allocatable_memory_gb > 0
                    else 0.0
                ),
            )

            # Re-construct ComputeNode with real cloud pricing & GPU metadata (F07 fix)
            inst_type = InstanceType.SPOT if node_cand.is_spot else InstanceType.ON_DEMAND
            cost_hr = node_cand.hourly_cost_usd if node_cand.hourly_cost_usd > 0 else (0.60 if not node_cand.is_spot else 0.18)
            spot_prob = node_cand.spot_interruption_prob if node_cand.spot_interruption_prob > 0 else (0.20 if node_cand.is_spot else 0.0)
            node_cost_profile = NodeCostProfile(
                instance_type=inst_type,
                cost_per_hour_usd=cost_hr,
                interruption_prob=spot_prob,
                region=node_cand.zone if node_cand.zone else "us-central1",
            )

            # Do not allow GPU capability to overwrite CPU architecture (V04 fix)
            norm_cand_arch = normalize_arch(node_cand.arch)
            if norm_cand_arch == "arm64":
                node_arch = NodeArch.ARM64
            else:
                node_arch = NodeArch.X86_64

            gpu_inv = {node_cand.gpu_type: int(node_cand.gpu_count)} if node_cand.gpu_count > 0 and node_cand.gpu_type else {}
            gpu_vram = {node_cand.gpu_type: float(node_cand.gpu_memory_gb)} if node_cand.gpu_count > 0 and node_cand.gpu_type else {}

            compute_node = ComputeNode(
                node_id=node_id,
                total_cpu_cores=node_cand.allocatable_cpu,
                total_memory_gb=node_cand.allocatable_memory_gb,
                allocated_cpu_cores=max(0.0, node_cand.allocatable_cpu - node_cand.scheduler_expected_free_cpu),
                allocated_memory_gb=max(0.0, node_cand.allocatable_memory_gb - node_cand.scheduler_expected_free_memory_gb),
                arch=node_arch,
                gpu_inventory=gpu_inv,
                gpu_vram_gb=gpu_vram,
                cost_profile=node_cost_profile,
                latest_telemetry=node_telemetry,
            )

            # Hard eligibility filtering - INDEPENDENT checks (V04 fix)
            is_eligible = True
            reasons = []

            # 1. Architecture Check
            if norm_req_arch and norm_cand_arch and norm_req_arch != norm_cand_arch:
                is_eligible = False
                reasons.append(f"ARCH_MISMATCH: requires {norm_req_arch}, node has {norm_cand_arch}")

            # 2. GPU Count and Model Check
            if job.resources.gpu_required:
                total_gpus = sum(compute_node.gpu_inventory.values())
                if total_gpus < job.resources.gpu_count:
                    is_eligible = False
                    reasons.append(f"INSUFFICIENT_GPUS: requires {job.resources.gpu_count}, node has {total_gpus}")
                if job.gpu_model_preferred and job.gpu_model_preferred not in compute_node.gpu_inventory:
                    is_eligible = False
                    reasons.append(f"GPU_MODEL_UNAVAILABLE: requires {job.gpu_model_preferred}")

            # 3. GPU VRAM Check (V04 fix)
            if job.resources.gpu_memory_gb and job.resources.gpu_memory_gb > 0:
                node_vram = max(compute_node.gpu_vram_gb.values(), default=0.0) if compute_node.gpu_vram_gb else float(node_cand.gpu_memory_gb)
                if node_vram < job.resources.gpu_memory_gb:
                    is_eligible = False
                    reasons.append(f"INSUFFICIENT_VRAM: requires {job.resources.gpu_memory_gb}GB, node has {node_vram}GB")

            # 4. Cost Ceiling Check - INDEPENDENT of GPU (V04 fix)
            if job.cost_ceiling_usd and compute_node.cost_profile.cost_per_hour_usd > job.cost_ceiling_usd:
                is_eligible = False
                reasons.append(f"COST_EXCEEDS_CEILING: ${compute_node.cost_profile.cost_per_hour_usd:.2f} > ${job.cost_ceiling_usd:.2f}")

            # 5. Workload Intent / QoS Spot Rule - INDEPENDENT (V04 fix)
            if job.workload_type == WorkloadType.LATENCY_CRITICAL:
                if compute_node.cost_profile.instance_type == InstanceType.SPOT or compute_node.cost_profile.interruption_prob > 0.05:
                    is_eligible = False
                    reasons.append("SPOT_DISALLOWED_FOR_LATENCY_CRITICAL")

            # 6. Telemetry Zero-Trust Threshold Check - INDEPENDENT (V04 fix)
            if kappa_i < 0.05:
                is_eligible = False
                reasons.append(f"UNTRUSTED_TELEMETRY: confidence {kappa_i:.4f} < 0.05")

            filter_reason = "; ".join(reasons) if not is_eligible else "PASSED"

            if is_eligible:
                # Compute base cost score
                eta = self.cost_engine.score_node(job, compute_node, prediction=None)
                if eta == 0.0:
                    is_eligible = False
                    filter_reason = "COST_ENGINE_GATE_REJECTED"
                    final_score = 0.0
                else:
                    final_score = eta * (kappa_i**request.gamma)
            else:
                eta = 0.0
                final_score = 0.0

            scores.append(
                sentinel_pb2.NodeScore(
                    node_id=node_id,
                    eta=eta,
                    confidence=kappa_i,
                    final_score=final_score,
                    is_eligible=is_eligible,
                    filter_reason=filter_reason,
                )
            )

        return sentinel_pb2.ScoreResponse(scores=scores)

    def PlacementCommitted(
        self,
        request: sentinel_pb2.PlacementCommittedRequest,
        context: grpc.ServicerContext,
    ) -> sentinel_pb2.PlacementCommittedResponse:
        """
        Acknowledges if a pod placement was committed (success) or aborted/unreserved.
        Idempotent state handling (F21 fix).
        """
        pod_uid = request.pod_uid
        node_id = request.node_id

        if request.success:
            if pod_uid not in self.committed_placements:
                self.committed_placements[pod_uid] = node_id
                self.total_deposits += 1
                print(
                    f"[Sentinel-Server] COMMIT: Placement confirmed on node {node_id} "
                    f"for pod {pod_uid}."
                )
        else:
            if pod_uid in self.committed_placements:
                del self.committed_placements[pod_uid]
                self.phantom_deposits += 1
                print(
                    f"[Sentinel-Server] ROLLBACK: Unreserved placement on node {node_id} "
                    f"for pod {pod_uid}."
                )

        return sentinel_pb2.PlacementCommittedResponse(acknowledged=True)


def serve(port: int = 50051) -> tuple[grpc.Server, ACOPredictiveSchedulerServicer]:
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ACOPredictiveSchedulerServicer()
    sentinel_pb2_grpc.add_ACOPredictiveSchedulerServicer_to_server(
        servicer, server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Sentinel-Server] Listening on port {port}...")
    return server, servicer


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    args = parser.parse_args()

    server, servicer = serve(port=args.port)

    def shutdown_handler(signum, frame) -> None:
        print(f"[Sentinel-Server] Received signal {signum}, persisting state and shutting down...")
        servicer.persistence.save()
        servicer.persistence.stop()
        servicer.config_watcher.stop()
        servicer.fed_server.stop()
        server.stop(0)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while True:
            time.sleep(86400)
    except SystemExit:
        pass

