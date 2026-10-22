#!/usr/bin/env python3
"""
kwok_scale_jitter_benchmark.py
Kube-Sentinel - Authentic Telemetry Jitter & Adversarial Node Isolation Benchmark

Sends real candidate batches containing arithmetic discrepancies, heartbeat jitter,
and stale heartbeats to the live gRPC daemon, measuring actual confidence discount factors
and eligibility filter decisions.
"""

import sys
import os
import time
import json
import argparse
import numpy as np
import grpc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "v2", "proto")))
import sentinel_pb2
import sentinel_pb2_grpc


def run_jitter_benchmark(host="127.0.0.1:50051", total_nodes=100, jitter_pct=20.0, iterations=10):
    server_proc = None
    if "127.0.0.1" in host or "localhost" in host:
        import socket
        import subprocess
        port = int(host.split(":")[-1])
        s = socket.socket()
        try:
            s.connect(("127.0.0.1", port))
            s.close()
        except OSError:
            print(f"Starting background Sentinel daemon on port {port}...")
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            server_proc = subprocess.Popen([sys.executable, "v2/grpc_server.py", f"--port={port}"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2.0)

    print(f"Connecting to live gRPC server at {host}...")
    channel = grpc.insecure_channel(host)
    stub = sentinel_pb2_grpc.ACOPredictiveSchedulerStub(channel)

    pod = sentinel_pb2.PodSpec(
        uid="pod-jitter-eval",
        name="critical-service",
        namespace="default",
        cpu_cores_requested=2.0,
        memory_gb_requested=4.0,
        workload_type="LATENCY_CRITICAL",
    )

    jitter_count = int(total_nodes * (jitter_pct / 100.0))
    clean_count = total_nodes - jitter_count

    print(f"Total Nodes: {total_nodes} | Clean: {clean_count} | Perturbed: {jitter_count} ({jitter_pct}%)")
    print(f"Running {iterations} live evaluation ticks...\n")

    total_perturbed_evaluated = 0
    total_perturbed_isolated = 0
    clean_confidences = []
    perturbed_confidences = []

    for tick in range(1, iterations + 1):
        now = time.time()
        nodes = []
        # 1. Clean nodes (consistent math, steady 5s heartbeats)
        for i in range(clean_count):
            node = sentinel_pb2.NodeCandidate(
                node_id=f"clean-node-{i:03d}",
                allocatable_cpu=16.0,
                allocatable_memory_gb=64.0,
                scheduler_expected_free_cpu=12.0,
                scheduler_expected_free_memory_gb=48.0,
                reported_allocatable_cpu=16.0,
                reported_used_cpu=4.0,
                reported_free_cpu=12.0,
                reported_allocatable_memory_gb=64.0,
                reported_used_memory_gb=16.0,
                reported_free_memory_gb=48.0,
                last_heartbeat_timestamp=now - 1.0,
                recent_heartbeat_intervals=[5.0, 5.0, 5.0, 5.0],
                hourly_cost_usd=0.60,
                is_spot=False,
            )
            nodes.append(node)

        # 2. Perturbed nodes (lying arithmetic or erratic jitter)
        for i in range(jitter_count):
            if i % 2 == 0:
                # Arithmetic discrepancy (16 - 4 != 30, discrepancy = 18 cores)
                rep_free = 30.0
                intervals = [5.0, 5.0, 5.0]
                hb_time = now - 1.0
            else:
                # Severe jitter (CV > 0.8) & stale
                rep_free = 12.0
                intervals = [1.0, 45.0, 2.0, 50.0]
                hb_time = now - 45.0

            node = sentinel_pb2.NodeCandidate(
                node_id=f"perturbed-node-{i:03d}",
                allocatable_cpu=16.0,
                allocatable_memory_gb=64.0,
                scheduler_expected_free_cpu=12.0,
                scheduler_expected_free_memory_gb=48.0,
                reported_allocatable_cpu=16.0,
                reported_used_cpu=4.0,
                reported_free_cpu=rep_free,
                reported_allocatable_memory_gb=64.0,
                reported_used_memory_gb=16.0,
                reported_free_memory_gb=48.0,
                last_heartbeat_timestamp=hb_time,
                recent_heartbeat_intervals=intervals,
                hourly_cost_usd=0.18,
                is_spot=True,
            )
            nodes.append(node)

        req = sentinel_pb2.ScoreRequest(pod=pod, nodes=nodes, gamma=2.0)
        t0 = time.perf_counter()
        resp = stub.ScoreNodes(req, timeout=5.0)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        for ns in resp.scores:
            if "perturbed" in ns.node_id:
                total_perturbed_evaluated += 1
                perturbed_confidences.append(ns.confidence)
                # An isolated node is marked ineligible by daemon or discounted below 0.05 threshold
                if not ns.is_eligible or ns.confidence < 0.05:
                    total_perturbed_isolated += 1
            else:
                clean_confidences.append(ns.confidence)

        print(f"Tick {tick:02d}: Evaluated {len(nodes)} nodes in {elapsed_ms:.2f}ms | Isolated {total_perturbed_isolated}/{total_perturbed_evaluated} degraded candidates")
        time.sleep(0.5)

    channel.close()
    if server_proc:
        server_proc.kill()

    isolation_rate = (total_perturbed_isolated / max(1, total_perturbed_evaluated)) * 100.0
    mean_clean_conf = float(np.mean(clean_confidences))
    mean_perturbed_conf = float(np.mean(perturbed_confidences))

    print("\n=== Live Jitter Benchmark Results ===")
    print(f"Clean Nodes Mean Trust       : {mean_clean_conf:.4f}")
    print(f"Perturbed Nodes Mean Trust   : {mean_perturbed_conf:.4f}")
    print(f"Degraded Isolation Rate      : {total_perturbed_isolated}/{total_perturbed_evaluated} ({isolation_rate:.1f}%)")

    results = {
        "total_nodes": total_nodes,
        "jitter_pct": jitter_pct,
        "ticks": iterations,
        "clean_mean_confidence": round(mean_clean_conf, 4),
        "perturbed_mean_confidence": round(mean_perturbed_conf, 4),
        "total_perturbed_evaluated": total_perturbed_evaluated,
        "total_perturbed_isolated": total_perturbed_isolated,
        "isolation_rate_pct": round(isolation_rate, 2),
    }

    os.makedirs("docs", exist_ok=True)
    with open("docs/kwok-jitter-benchmark.json", "w") as f:
        json.dump(results, f, indent=2)

    print("Results saved to docs/kwok-jitter-benchmark.json")


def main():
    parser = argparse.ArgumentParser(description="Live Telemetry Jitter Benchmark")
    parser.add_argument("--host", default="127.0.0.1:50051", help="gRPC server address")
    parser.add_argument("--nodes", type=int, default=100, help="Total candidate nodes")
    parser.add_argument("--jitter-pct", type=float, default=20.0, help="Jitter injection percentage")
    parser.add_argument("--ticks", type=int, default=5, help="Number of evaluation ticks")
    args = parser.parse_args()

    run_jitter_benchmark(host=args.host, total_nodes=args.nodes, jitter_pct=args.jitter_pct, iterations=args.ticks)


if __name__ == "__main__":
    main()
