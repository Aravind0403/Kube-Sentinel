#!/usr/bin/env python3
"""
benchmark_grpc.py
Kube-Sentinel - Authentic gRPC Throughput & Latency Knee Benchmark

Executes REAL loopback gRPC calls over socket :50051 using the compiled Protobuf stubs,
measuring actual serialization, transport, and scoring execution latencies.
"""

import sys
import os
os.makedirs("artifacts/benchmarks", exist_ok=True)
import time
import json
import argparse
import subprocess
import numpy as np
import grpc

# Add proto directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "v2", "proto")))
import sentinel_pb2
import sentinel_pb2_grpc


def build_test_request(candidate_count=32):
    """Constructs a realistic ScoreRequest message with candidate nodes."""
    pod = sentinel_pb2.PodSpec(
        uid="bench-pod-001",
        name="inference-worker",
        namespace="default",
        cpu_cores_requested=2.0,
        memory_gb_requested=4.0,
        workload_type="LATENCY_CRITICAL",
        gpu_requested=1.0,
        gpu_type_required="L4",
        min_gpu_memory_gb=16.0,
        required_arch="x86_64",
        cost_ceiling_usd=10.0,
    )

    nodes = []
    now = time.time()
    for i in range(candidate_count):
        is_spot = (i % 2 == 1)
        node = sentinel_pb2.NodeCandidate(
            node_id=f"node-candidate-{i:03d}",
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
            last_heartbeat_timestamp=now - 2.0,
            recent_heartbeat_intervals=[5.0, 5.1, 4.9, 5.0, 5.0],
            hourly_cost_usd=0.60 if not is_spot else 0.18,
            is_spot=is_spot,
            spot_interruption_prob=0.0 if not is_spot else 0.15,
            gpu_type="L4",
            gpu_count=1,
            gpu_memory_gb=24.0,
            arch="x86_64",
            zone="us-central1-a",
        )
        nodes.append(node)

    return sentinel_pb2.ScoreRequest(
        pod=pod,
        nodes=nodes,
        gamma=2.0,
    )


def run_benchmark(target_host="127.0.0.1:50051", burst_steps=None, candidate_nodes=32):
    if burst_steps is None:
        burst_steps = [100, 500, 1000, 2500, 5000]

    server_proc = None
    if "127.0.0.1" in target_host or "localhost" in target_host:
        import socket
        port = int(target_host.split(":")[-1])
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

    print(f"Connecting to live gRPC server at {target_host}...")
    channel = grpc.insecure_channel(target_host)
    stub = sentinel_pb2_grpc.SentinelSchedulerStub(channel)

    print("Executing 50 warm-up RPC queries...")
    warm_req = build_test_request(candidate_count=candidate_nodes)
    for _ in range(50):
        try:
            resp = stub.ScoreNodes(warm_req, timeout=5.0)
            if not resp.scores:
                raise RuntimeError("Empty response received from daemon")
            eligible_count = sum(1 for s in resp.scores if s.is_eligible)
            if eligible_count == 0:
                raise RuntimeError("Zero candidates marked eligible during warm-up - check constraint alignment")
        except Exception as e:
            print(f"ERROR: Failed warm-up connection to gRPC daemon: {e}")
            if server_proc:
                server_proc.kill()
            sys.exit(1)

    print(f"Warm-up complete. Connection healthy ({eligible_count}/{len(warm_req.nodes)} candidates eligible).\n")
    print(f"{'Burst Size':<12} | {'P50 (ms)':<10} | {'P95 (ms)':<10} | {'P99 (ms)':<10} | {'Throughput (RPC/s)':<20}")
    print("-" * 65)

    results = []

    for burst in burst_steps:
        latencies_ms = []
        req = build_test_request(candidate_count=candidate_nodes)

        start_wall_time = time.perf_counter()

        for _ in range(burst):
            t0 = time.perf_counter_ns()
            stub.ScoreNodes(req, timeout=5.0)
            t1 = time.perf_counter_ns()
            latencies_ms.append((t1 - t0) / 1_000_000.0)

        total_wall_sec = time.perf_counter() - start_wall_time

        lat_arr = np.array(latencies_ms)
        p50 = float(np.percentile(lat_arr, 50))
        p95 = float(np.percentile(lat_arr, 95))
        p99 = float(np.percentile(lat_arr, 99))
        throughput = float(burst / total_wall_sec)

        print(f"{burst:<12} | {p50:<10.3f} | {p95:<10.3f} | {p99:<10.3f} | {throughput:<20.1f}")

        results.append({
            "burst_size": burst,
            "candidate_nodes": candidate_nodes,
            "p50_ms": round(p50, 4),
            "p95_ms": round(p95, 4),
            "p99_ms": round(p99, 4),
            "throughput_pods_per_sec": round(throughput, 2),
            "total_wall_sec": round(total_wall_sec, 4),
        })

    channel.close()
    if server_proc:
        server_proc.kill()
    return results


def main():
    parser = argparse.ArgumentParser(description="Live gRPC Throughput Benchmark")
    parser.add_argument("--host", default="127.0.0.1:50051", help="gRPC server address")
    parser.add_argument("--candidate-nodes", type=int, default=32, help="Number of nodes per request")
    parser.add_argument("--max-burst", type=int, default=2500, help="Maximum burst size")
    args = parser.parse_args()

    steps = [100, 500, 1000]
    if args.max_burst >= 2500:
        steps.append(2500)
    if args.max_burst >= 5000:
        steps.append(5000)

    print(f"=== Starting Kube-Sentinel Live gRPC Knee Benchmark ===")
    print(f"Server: {args.host} | Candidate Nodes/Batch: {args.candidate_nodes}")

    results = run_benchmark(target_host=args.host, burst_steps=steps, candidate_nodes=args.candidate_nodes)

    os.makedirs("docs", exist_ok=True)
    out_path = "artifacts/benchmarks/grpc.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nEmpirical benchmark completed. Results saved to {out_path}")


if __name__ == "__main__":
    main()
