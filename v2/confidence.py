"""
v2/confidence.py
────────────────
Telemetry confidence calculations for Kube-Sentinel.

This module provides the core trust metric math and confidence tracking for
cluster nodes. It implements three dimensions of node telemetry validation:
  1. Internal consistency (Arithmetic check: Allocatable - Used == Free)
  2. Telemetry freshness (Heartbeat age check)
  3. Heartbeat consistency (Cadence variation check via CV)

Confidence values are smoothed over time using an Exponential Moving Average (EMA)
initialized to a moderate default on cold starts.
"""

from __future__ import annotations

import math
from typing import List, Optional


class EMA:
    """
    Simple Exponential Moving Average filter.
    Used to smooth utilization and confidence signals over time.
    """

    def __init__(self, alpha: float = 0.5, initial_value: float = 0.7) -> None:
        """
        Initialize the EMA filter.

        Args:
            alpha: Smoothing factor in (0.0, 1.0]. Higher values give more weight
                   to recent samples. Default 0.5 (horizon H = 2.0).
            initial_value: Starting value (seed) for the moving average.
        """
        if not (0.0 < alpha <= 1.0):
            raise ValueError(f"alpha must be in (0.0, 1.0], got {alpha}")
        self.alpha = alpha
        self.value = initial_value
        self.count = 0

    def update(self, sample: float) -> float:
        """
        Update the EMA with a new sample.

        Args:
            sample: The raw sample value.

        Returns:
            The updated EMA value.
        """
        self.count += 1
        if self.count == 1:
            # First observation: initialize directly from sample to avoid cold-start smoothing of corrupted/dead nodes
            self.value = float(sample)
        elif sample == 0.0:
            # Hard zero-trust event: immediately collapse trust without lag
            self.value = 0.0
        else:
            self.value = self.alpha * sample + (1.0 - self.alpha) * self.value
        return self.value


def calculate_internal_consistency(
    allocatable: float,
    used: float,
    free: float,
) -> float:
    """
    Check if the node's self-reported utilization metrics add up arithmetically.

    Equation:
        Delta = |(Allocatable - Used) - Free|
        score = max(0.0, 1.0 - Delta / Allocatable)

    Args:
        allocatable: Reported allocatable resource capacity (e.g. CPU cores or memory).
        used: Reported used capacity.
        free: Reported free capacity.

    Returns:
        float in [0.0, 1.0]. Returns 0.0 if allocatable is <= 0.
    """
    if allocatable <= 0.0:
        return 0.0

    delta = abs((allocatable - used) - free)
    score = max(0.0, 1.0 - (delta / allocatable))
    return float(score)


def calculate_cross_consistency(
    allocatable: float,
    expected_free: float,
    reported_free: float,
) -> float:
    """
    Check if the node's reported free capacity matches the scheduler's assumed free capacity.

    Equation:
        Delta = |expected_free - reported_free|
        score = max(0.0, 1.0 - Delta / allocatable)
    """
    if allocatable <= 0.0:
        return 0.0

    delta = abs(expected_free - reported_free)
    score = max(0.0, 1.0 - (delta / allocatable))
    return float(score)


def calculate_freshness(
    delta_t: float,
    t_max: float = 30.0,
) -> float:
    """
    Discount confidence based on how stale the last telemetry heartbeat is.

    Equation:
        score = max(0.0, 1.0 - delta_t / t_max)

    Args:
        delta_t: Time elapsed since the last heartbeat in seconds. Must be >= 0.
        t_max: Stale threshold limit in seconds. Default 30.0.

    Returns:
        float in [0.0, 1.0].
    """
    if delta_t < 0.0:
        return 0.0
    if t_max <= 0.0:
        return 1.0

    score = max(0.0, 1.0 - (delta_t / t_max))
    return float(score)


def calculate_heartbeat_consistency(
    intervals: List[float],
    cv_max: float = 0.5,
) -> float:
    """
    Detect reporting cadence anomalies (flapping/jitter) using Coefficient of Variation (CV).

    Equation:
        CV = std_dev(intervals) / mean(intervals)
        score = max(0.0, 1.0 - CV / cv_max)

    Args:
        intervals: List of durations between consecutive heartbeats in seconds.
        cv_max: Max variation coefficient limit. Default 0.5.

    Returns:
        float in [0.0, 1.0]. Returns 1.0 (neutral) if there are fewer than
        two intervals to analyze (cold start).
    """
    if len(intervals) < 2:
        return 1.0  # Cold start: not enough data to measure cadence variance yet.

    n = len(intervals)
    mean = sum(intervals) / n

    if mean <= 1e-6:
        return 0.0

    # Calculate standard deviation
    variance = sum((x - mean) ** 2 for x in intervals) / n
    std_dev = math.sqrt(variance)
    cv = std_dev / mean

    if cv_max <= 0.0:
        return 1.0

    score = max(0.0, 1.0 - (cv / cv_max))
    return float(score)


class NodeConfidenceTracker:
    """
    Tracks and updates the trust confidence score for a single cluster node.

    Combines internal consistency, report freshness, and heartbeat consistency
    into a single composite score, smoothed over time using an EMA filter.
    """

    def __init__(
        self,
        node_id: str,
        alpha: float = 0.5,
        initial_confidence: float = 0.7,
        t_max: float = 30.0,
        cv_max: float = 0.5,
    ) -> None:
        """
        Initialize the node confidence tracker.

        Args:
            node_id: Unique identifier for the tracked node.
            alpha: EMA smoothing factor. Default 0.5.
            initial_confidence: Cold-start trust seed in [0.0, 1.0]. Default 0.7.
            t_max: Freshness timeout limit in seconds. Default 30.0.
            cv_max: Heartbeat interval CV limit. Default 0.5.
        """
        self.node_id = node_id
        self.t_max = t_max
        self.cv_max = cv_max
        self.ema = EMA(alpha=alpha, initial_value=initial_confidence)

        # Heartbeat history: stores timestamps of recent heartbeats
        self.heartbeat_timestamps: List[float] = []
        self._max_history = 11  # Keeps up to 10 intervals (requires 11 timestamps)

    def record_heartbeat(self, timestamp: float) -> None:
        """
        Record the timestamp of a new heartbeat.

        Args:
            timestamp: Unix epoch timestamp in seconds.
        """
        # Ensure timestamps are strictly increasing (guard against out-of-order)
        if self.heartbeat_timestamps and timestamp <= self.heartbeat_timestamps[-1]:
            return

        self.heartbeat_timestamps.append(timestamp)
        if len(self.heartbeat_timestamps) > self._max_history:
            self.heartbeat_timestamps.pop(0)

    def get_intervals(self) -> List[float]:
        """
        Compute inter-heartbeat intervals from the timestamp history.

        Returns:
            List of intervals in seconds.
        """
        if len(self.heartbeat_timestamps) < 2:
            return []
        return [
            self.heartbeat_timestamps[i] - self.heartbeat_timestamps[i - 1]
            for i in range(1, len(self.heartbeat_timestamps))
        ]

    def compute_raw_confidence(
        self,
        reported_allocatable: float,
        reported_used: float,
        reported_free: float,
        current_time: float,
    ) -> float:
        """
        Compute the raw (unsmoothed) composite confidence score.

        Args:
            reported_allocatable: Allocatable capacity metric.
            reported_used: Active usage metric.
            reported_free: Available capacity metric.
            current_time: Current epoch timestamp in seconds.

        Returns:
            Raw confidence float in [0.0, 1.0].
        """
        # 1. Arithmetic check
        k_internal = calculate_internal_consistency(
            reported_allocatable, reported_used, reported_free
        )

        # 2. Freshness check
        if not self.heartbeat_timestamps:
            k_fresh = 0.1  # No history: penalize freshness slightly
        else:
            delta_t = current_time - self.heartbeat_timestamps[-1]
            k_fresh = calculate_freshness(delta_t, self.t_max)

        # 3. Cadence check
        intervals = self.get_intervals()
        k_heartbeat = calculate_heartbeat_consistency(intervals, self.cv_max)

        # Multiplicative product (any zero results in overall zero trust)
        return k_internal * k_fresh * k_heartbeat

    @property
    def last_heartbeat_timestamp(self) -> float:
        """Return the timestamp of the latest recorded heartbeat, or 0.0 if empty."""
        return self.heartbeat_timestamps[-1] if self.heartbeat_timestamps else 0.0

    @property
    def current_confidence(self) -> float:
        """Return the current smoothed confidence level."""
        return self.ema.value

    def update(
        self,
        reported_allocatable: float,
        reported_used: float,
        reported_free: float,
        current_time: float,
    ) -> float:
        """
        Update the running EMA confidence score with new telemetry.

        Args:
            reported_allocatable: Node's reported total allocatable resource.
            reported_used: Node's reported allocated usage.
            reported_free: Node's reported free resource.
            current_time: Current timestamp in seconds.

        Returns:
            Smoothed confidence float in [0.0, 1.0].
        """
        raw_conf = self.compute_raw_confidence(
            reported_allocatable, reported_used, reported_free, current_time
        )
        return self.ema.update(raw_conf)


class AdaptiveThresholdManager:
    """
    Manages and adjusts cv_max and t_max dynamically based on cluster health.
    Implements a 3-state machine with hysteresis to prevent threshold thrashing.
    """

    def __init__(self) -> None:
        self.state = "STABLE"  # STABLE, DEGRADED, RECOVERING
        self.stable_counter = 0
        self.degraded_counter = 0

    def evaluate_state(self, avg_cv: float) -> None:
        # Hysteresis to prevent rapid state flipping
        if self.state == "STABLE":
            if avg_cv > 0.25:  # Higher threshold to enter degraded
                self.degraded_counter += 1
                if self.degraded_counter >= 3:  # 3 consecutive samples
                    self.state = "DEGRADED"
                    self.degraded_counter = 0
            else:
                self.degraded_counter = 0
        elif self.state == "DEGRADED":
            if avg_cv < 0.15:  # Lower threshold to return to stable
                self.stable_counter += 1
                if self.stable_counter >= 5:  # 5 consecutive samples (more conservative)
                    self.state = "STABLE"
                    self.stable_counter = 0
            else:
                self.stable_counter = 0
        elif self.state == "RECOVERING":
            # Intermediate state during heavy load
            if avg_cv < 0.2:
                self.state = "STABLE"
            elif avg_cv > 0.35:
                self.state = "DEGRADED"

    def get_thresholds(self) -> dict:
        if self.state == "STABLE":
            return {"cv_max": 0.3, "t_max": 15.0}
        elif self.state == "DEGRADED":
            return {"cv_max": 0.8, "t_max": 60.0}
        else:  # RECOVERING
            return {"cv_max": 0.5, "t_max": 35.0}
