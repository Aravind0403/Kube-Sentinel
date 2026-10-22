# Release cleanup

The active implementation is the native Go plugin and Python scoring daemon. The retired colony implementation and HTTP extender, unrelated orchestration/agent/predictor modules, standalone simulator, old deployment/Terraform files, extender dashboards, personal drafts, old charts and superseded benchmark reports were removed from the working tree. Root-level logs and an empty scheduler archive were also removed. They remain recoverable from the unchanged v0.1.0 tag and Git history.

The cost engine and shared models remain because the active daemon imports them. The control-plane package initializer now exports only the cost engine, removing implicit imports of the retired modules. The protobuf service and generated bindings, metadata keys, deployment names, tests and scripts use the new interface consistently.

Three experiments remain with explicit names: RPC latency, telemetry perturbation and offline policy replay. They write ignored results under artifacts/benchmarks. CI relies on the actual native Go daemon failover test rather than the duplicate Python breaker simulation. Markdown is no longer globally ignored.

Existing release acceptance evidence is preserved unchanged. No Git history rewrite or tag replacement is part of this cleanup. Cloud infrastructure will be designed for a CPU-only acceptance check instead of reusing the obsolete multi-GPU Terraform configuration.
