#!/usr/bin/env bash
# kwok_chaos_failover.sh
# ACO-Sentinel (Version 2) - Chaos Injection & Circuit Breaker Failover Test
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR/.."

echo "=== Executing Live Chaos Injection & Circuit Breaker Failover Test ==="
python3 scripts/run_chaos_failover.py
