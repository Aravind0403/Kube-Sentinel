"""
v2/verify_confidence.py
───────────────────────
Phase 1: Verification script for Kube-Sentinel confidence math.

Simulates four node scenarios (clean, arithmetically compromised, lagging, and flapping)
and prints the resulting confidence trajectories to verify mathematical correctness.
"""

from __future__ import annotations

import time
from confidence import NodeConfidenceTracker


def run_simulation() -> None:
    print("=" * 80)
    print("KUBE-SENTINEL: CONFIDENCE MATH VERIFICATION")
    print("=" * 80)

    # ── Scenario 1: Clean Node ────────────────────────────────────────────────
    # Heartbeats arrive at perfectly regular 5s intervals. Telemetry adds up.
    print("\n--- Scenario 1: Clean Node (Intervals=5.0s, Alloc=100, Used=30, Free=70) ---")
    tracker = NodeConfidenceTracker("node-clean", alpha=0.5, initial_confidence=0.7)
    t = 100.0
    for i in range(12):
        t += 5.0
        tracker.record_heartbeat(t)
        # Alloc = 100, Used = 30, Free = 70 (Adds up: 100 - 30 == 70)
        conf = tracker.update(100.0, 30.0, 70.0, t)
        print(f"Tick {i+1:02d}: Time={t:.1f}s | Raw Conf={tracker.compute_raw_confidence(100.0, 30.0, 70.0, t):.4f} | Smoothed Conf={conf:.4f}")
    assert tracker.current_confidence > 0.95, "Clean node confidence should converge close to 1.0"
    print("✅ Passed: Clean node trust successfully converged to ~1.0.")

    # ── Scenario 2: Arithmetically Lying Node ──────────────────────────────────
    # Heartbeats on time, but telemetry is inconsistent (100 - 30 != 85)
    print("\n--- Scenario 2: Lying Node (Alloc=100, Used=30, Free=85) ---")
    tracker = NodeConfidenceTracker("node-liar", alpha=0.5, initial_confidence=0.7)
    t = 100.0
    for i in range(5):
        t += 5.0
        tracker.record_heartbeat(t)
        # Arithmetic error: Delta = |(100 - 30) - 85| = 15. Score should be 1.0 - 15/100 = 0.85
        conf = tracker.update(100.0, 30.0, 85.0, t)
        raw = tracker.compute_raw_confidence(100.0, 30.0, 85.0, t)
        print(f"Tick {i+1:02d}: Time={t:.1f}s | Raw Conf={raw:.4f} | Smoothed Conf={conf:.4f}")
    assert tracker.current_confidence < 0.90, "Lying node confidence should be penalized"
    print("✅ Passed: Inconsistent metrics successfully penalized node trust.")

    # ── Scenario 3: Lagging Node (Missed Heartbeats) ─────────────────────────
    # Node heartbeats on time initially, then goes quiet for 40 seconds (T_max = 30.0s)
    print("\n--- Scenario 3: Lagging Node (Missed heartbeat: delta_t = 40.0s) ---")
    tracker = NodeConfidenceTracker("node-lag", alpha=0.5, initial_confidence=0.7, t_max=30.0)
    t = 100.0
    # Establish baseline
    for i in range(4):
        t += 5.0
        tracker.record_heartbeat(t)
        tracker.update(100.0, 30.0, 70.0, t)
    print(f"Baseline established. Confidence = {tracker.current_confidence:.4f}")
    
    # 40s lag occurs
    t += 40.0
    conf = tracker.update(100.0, 30.0, 70.0, t)
    raw = tracker.compute_raw_confidence(100.0, 30.0, 70.0, t)
    print(f"After 40s lag: Time={t:.1f}s | Raw Conf={raw:.4f} | Smoothed Conf={conf:.4f}")
    assert raw == 0.0, "Raw confidence must drop to 0.0 when lag exceeds T_max"
    assert conf < 0.5, "Smoothed confidence should decay significantly after lag"
    print("✅ Passed: Missed heartbeats successfully dropped trust to 0.0.")

    # ── Scenario 4: Flapping Node (Cadence Jitter) ────────────────────────────
    # Node reports consistent metrics, but interval variance CV is high (limits CV_max = 0.5)
    print("\n--- Scenario 4: Flapping Node (Erratic arrival intervals) ---")
    tracker = NodeConfidenceTracker("node-flapper", alpha=0.5, initial_confidence=0.7, cv_max=0.5)
    t = 100.0
    intervals = [1.0, 9.0, 1.0, 9.0, 2.0, 8.0, 1.0, 9.0] # mean = 5.0, std = ~4.0, CV = ~0.8
    for i, step in enumerate(intervals):
        t += step
        tracker.record_heartbeat(t)
        conf = tracker.update(100.0, 30.0, 70.0, t)
        raw = tracker.compute_raw_confidence(100.0, 30.0, 70.0, t)
        print(f"Tick {i+1:02d}: Time={t:.1f}s (step={step}s) | Raw Conf={raw:.4f} | Smoothed Conf={conf:.4f}")
    assert raw < 0.5, "Jittery intervals should yield a high CV and degrade raw confidence"
    print("✅ Passed: Jittery reporting cadence successfully degraded node trust.")

    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE: ALL MATHEMATICAL POLICIES CONFIRMED")
    print("=" * 80)


if __name__ == "__main__":
    run_simulation()
