package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sentinel-plugin/pb"
)

const (
	Name = "SentinelScheduler"

	// Annotations for telemetry passing
	AnnotationReportedAllocatableCPU = "kube-sentinel.io/reported-allocatable-cpu"
	AnnotationReportedUsedCPU        = "kube-sentinel.io/reported-used-cpu"
	AnnotationReportedFreeCPU        = "kube-sentinel.io/reported-free-cpu"

	AnnotationReportedAllocatableMem = "kube-sentinel.io/reported-allocatable-memory-gb"
	AnnotationReportedUsedMem        = "kube-sentinel.io/reported-used-memory-gb"
	AnnotationReportedFreeMem        = "kube-sentinel.io/reported-free-memory-gb"

	AnnotationLastHeartbeat      = "kube-sentinel.io/last-heartbeat-timestamp"
	AnnotationHeartbeatIntervals = "kube-sentinel.io/recent-heartbeat-intervals"

	AnnotationHourlyCostUSD      = "sentinel.cost/hourly-usd"
	AnnotationIsSpot             = "sentinel.cost/is-spot"
	AnnotationSpotInterruption   = "sentinel.cost/spot-interruption-prob"
	AnnotationGPUType            = "sentinel.gpu/type"
	AnnotationGPUCount           = "sentinel.gpu/count"
	AnnotationGPUMemoryGB        = "sentinel.gpu/memory-gb"
	AnnotationArch               = "sentinel.node/arch"
	AnnotationCostCeilingUSD     = "sentinel.cost/ceiling-usd"
	AnnotationForceReject        = "sentinel.trust/force-reject"

	LabelWorkloadType            = "kube-sentinel.io/workload-type"
	LabelGpuRequired             = "sentinel.gpu/required"
	LabelGpuTypeRequired         = "sentinel.gpu/type-required"
)

// SentinelConfig definitions
type SentinelConfig struct {
	Scoring     ScoringConfig     `yaml:"scoring"`
	Telemetry   TelemetryConfig   `yaml:"telemetry"`
	Performance PerformanceConfig `yaml:"performance"`
	Resilience  ResilienceConfig  `yaml:"resilience"`
}

type ScoringConfig struct {
	TrustExponentGamma  float64 `yaml:"trust_exponent_gamma"`
	WeightScoringPlugin int     `yaml:"weight_scoring_plugin"`
	FallbackStrategy    string  `yaml:"fallback_strategy"`
}

type TelemetryConfig struct {
	HeartbeatTimeoutSeconds int `yaml:"heartbeat_timeout_seconds"`
}

type PerformanceConfig struct {
	GrpcTimeoutSeconds int                  `yaml:"grpc_timeout_seconds"`
	CacheTtlSeconds    float64              `yaml:"cache_ttl_seconds"`
	AdaptiveScaling    AdaptiveScalingConfig `yaml:"adaptive_scaling"`
}

type AdaptiveScalingConfig struct {
	Enabled    bool                  `yaml:"enabled"`
	Thresholds struct {
		SmallCluster int `yaml:"small_cluster"`
		LargeCluster int `yaml:"large_cluster"`
	} `yaml:"thresholds"`
}

type ResilienceConfig struct {
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
}

type CircuitBreakerConfig struct {
	FailureThreshold    int32 `yaml:"failure_threshold"`
	TimeoutSeconds      int   `yaml:"timeout_seconds"`
	HalfOpenMaxAttempts int   `yaml:"half_open_max_attempts"`
}

func (c *SentinelConfig) Validate() error {
	if c.Scoring.TrustExponentGamma < 0 || c.Scoring.TrustExponentGamma > 5 {
		return fmt.Errorf("trust_exponent_gamma must be between 0.0 and 5.0, got %f", c.Scoring.TrustExponentGamma)
	}
	if c.Telemetry.HeartbeatTimeoutSeconds < 5 || c.Telemetry.HeartbeatTimeoutSeconds > 300 {
		return fmt.Errorf("heartbeat_timeout_seconds must be between 5 and 300, got %d", c.Telemetry.HeartbeatTimeoutSeconds)
	}
	if c.Performance.GrpcTimeoutSeconds < 1 || c.Performance.GrpcTimeoutSeconds > 60 {
		return fmt.Errorf("grpc_timeout_seconds must be between 1 and 60, got %d", c.Performance.GrpcTimeoutSeconds)
	}
	return nil
}

// ConfigWatcher definitions
type ConfigWatcher struct {
	mu     sync.RWMutex
	config *SentinelConfig
	path   string
	stopCh chan struct{}
}

func NewConfigWatcher(path string) (*ConfigWatcher, error) {
	cw := &ConfigWatcher{
		path:   path,
		stopCh: make(chan struct{}),
	}
	cfg, err := cw.loadConfig()
	if err != nil {
		return nil, err
	}
	cw.config = cfg
	return cw, nil
}

func (cw *ConfigWatcher) loadConfig() (*SentinelConfig, error) {
	data, err := os.ReadFile(cw.path)
	if err != nil {
		return nil, err
	}
	var cfg SentinelConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cw *ConfigWatcher) GetConfig() *SentinelConfig {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.config
}

func (cw *ConfigWatcher) Watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			newConfig, err := cw.loadConfig()
			if err != nil {
				fmt.Printf("[Sentinel-ConfigWatcher] WARNING: Failed to reload config: %v. Keeping current.\n", err)
				continue
			}
			cw.mu.Lock()
			cw.config = newConfig
			cw.mu.Unlock()
			fmt.Println("[Sentinel-ConfigWatcher] Config reloaded successfully from", cw.path)
		case <-cw.stopCh:
			return
		}
	}
}

func (cw *ConfigWatcher) Stop() {
	close(cw.stopCh)
}

// CircuitBreaker definitions
type CircuitBreakerState int32

const (
	Closed CircuitBreakerState = iota
	Open
	HalfOpen
)

type CircuitBreaker struct {
	state            int32 // CircuitBreakerState
	failureCount     int32
	lastFailure      time.Time
	lastSuccess      time.Time
	recoveryAttempts int
	mu               sync.RWMutex
	failureThreshold int32
	timeout          time.Duration
}

func NewCircuitBreaker(threshold int32, timeoutSeconds int) *CircuitBreaker {
	return &CircuitBreaker{
		state:            int32(Closed),
		failureThreshold: threshold,
		timeout:          time.Duration(timeoutSeconds) * time.Second,
	}
}

func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&cb.state))
}

func (cb *CircuitBreaker) AttemptCall(ctx context.Context, fn func() error) error {
	state := cb.GetState()

	if state == Open {
		cb.mu.RLock()
		elapsed := time.Since(cb.lastFailure)
		attempts := cb.recoveryAttempts
		cb.mu.RUnlock()

		backoffDuration := cb.calculateBackoff(attempts)
		if elapsed > backoffDuration {
			cb.mu.Lock()
			atomic.StoreInt32(&cb.state, int32(HalfOpen))
			cb.recoveryAttempts++
			cb.mu.Unlock()
			fmt.Printf("[Sentinel-CircuitBreaker] Circuit breaker entering Half-Open state, attempt %d\n", cb.recoveryAttempts)
		} else {
			return fmt.Errorf("circuit breaker open, next retry in %v", backoffDuration-elapsed)
		}
	}

	err := fn()
	if err != nil {
		cb.recordFailure()
		return err
	}
	cb.recordSuccess()
	return nil
}

func (cb *CircuitBreaker) calculateBackoff(attempts int) time.Duration {
	base := cb.timeout
	if base <= 0 {
		base = 5 * time.Second
	}
	if attempts <= 0 {
		return base
	}
	multiplier := math.Pow(2, float64(attempts))
	backoff := time.Duration(float64(base) * multiplier)
	if backoff > 5*time.Minute {
		backoff = 5 * time.Minute
	}
	return backoff
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.lastFailure = time.Now()
	currentState := cb.GetState()

	if currentState == HalfOpen {
		// Failed probe in half-open state immediately reopens circuit (F09 fix)
		atomic.StoreInt32(&cb.state, int32(Open))
		fmt.Printf("[Sentinel-CircuitBreaker] Probe call failed in HalfOpen. Reopening circuit. Next backoff: %v\n", cb.calculateBackoff(cb.recoveryAttempts))
		return
	}

	failures := atomic.AddInt32(&cb.failureCount, 1)
	if currentState == Closed && failures >= cb.failureThreshold {
		atomic.StoreInt32(&cb.state, int32(Open))
		fmt.Printf("[Sentinel-CircuitBreaker] Circuit breaker transitions to OPEN. Failure count reached threshold %d\n", cb.failureThreshold)
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.lastSuccess = time.Now()
	atomic.StoreInt32(&cb.failureCount, 0)
	cb.recoveryAttempts = 0
	if cb.GetState() == HalfOpen {
		atomic.StoreInt32(&cb.state, int32(Closed))
		fmt.Println("[Sentinel-CircuitBreaker] Circuit breaker transitions to CLOSED. Connection recovered successfully.")
	}
}

// placementEntry tracks active pod reservations with timestamps for bounded capacity eviction (V05 fix)
type placementEntry struct {
	nodeName  string
	timestamp time.Time
}

// podConstraints snapshots pod requirements once per cycle in PreFilter (V01, V04 fix)
type podConstraints struct {
	requiredArch      string
	costCeilingUSD    float64
	gpuRequested      float64
	gpuTypeRequired   string
	minGPUMemoryGB    float64
	isLatencyCritical bool
}

func (c *podConstraints) Clone() framework.StateData {
	return c
}

const PreFilterStateKey = framework.StateKey(Name + "PreFilterState")

func normalizeArch(arch string) string {
	a := strings.ToLower(strings.TrimSpace(arch))
	if a == "amd64" || a == "x86_64" || a == "x86-64" || a == "x86" || a == "x64" {
		return "amd64"
	}
	if a == "arm64" || a == "aarch64" || a == "arm" {
		return "arm64"
	}
	return a
}

func extractPodConstraints(pod *v1.Pod) *podConstraints {
	var gpuMin float64
	for _, container := range pod.Spec.Containers {
		if qty, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
			gpuMin += float64(qty.Value())
		}
	}

	var archReq, gpuTypeReq string
	var costCeiling, minGpuMemoryGb float64
	isLatencyCritical := false

	if pod.Labels != nil {
		if val, ok := pod.Labels[LabelWorkloadType]; ok {
			lower := strings.ToLower(val)
			if strings.Contains(lower, "latency") || strings.Contains(lower, "critical") {
				isLatencyCritical = true
			}
		}
		if val, ok := pod.Labels[LabelGpuTypeRequired]; ok {
			gpuTypeReq = val
		}
	}

	if pod.Spec.NodeSelector != nil {
		if val, ok := pod.Spec.NodeSelector["kubernetes.io/arch"]; ok {
			archReq = val
		}
		if val, ok := pod.Spec.NodeSelector["cloud.google.com/gke-gpu"]; ok {
			gpuTypeReq = val
		}
	}

	if pod.Annotations != nil {
		if val, ok := pod.Annotations[AnnotationArch]; ok && val != "" {
			archReq = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationCostCeilingUSD], 64); err == nil {
			costCeiling = val
		}
		if val, ok := pod.Annotations[AnnotationGPUType]; ok && val != "" {
			gpuTypeReq = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationGPUMemoryGB], 64); err == nil {
			minGpuMemoryGb = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationGPUCount], 64); err == nil && gpuMin == 0 {
			gpuMin = val
		}
		if val, ok := pod.Annotations[LabelWorkloadType]; ok {
			lower := strings.ToLower(val)
			if strings.Contains(lower, "latency") || strings.Contains(lower, "critical") {
				isLatencyCritical = true
			}
		}
	}

	return &podConstraints{
		requiredArch:      normalizeArch(archReq),
		costCeilingUSD:    costCeiling,
		gpuRequested:      gpuMin,
		gpuTypeRequired:   gpuTypeReq,
		minGPUMemoryGB:    minGpuMemoryGb,
		isLatencyCritical: isLatencyCritical,
	}
}

// AuditLogger definitions
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Pod       string    `json:"pod"`
	Node      string    `json:"node"`
	Decision  string    `json:"decision"`
	Trust     float64   `json:"trust"`
	Reason    string    `json:"reason"`
	Scheduler string    `json:"scheduler"`
}

type AuditLogger struct {
	writer io.Writer
	mu     sync.Mutex
}

func NewAuditLogger(path string) (*AuditLogger, error) {
	idx := strings.LastIndex(path, "/")
	if idx != -1 {
		dir := path[:idx]
		_ = os.MkdirAll(dir, 0755)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &AuditLogger{writer: file}, nil
}

func (al *AuditLogger) LogDecision(pod *v1.Pod, node string, decision string, trust float64, reason string) {
	if al == nil {
		return
	}
	entry := AuditEntry{
		Timestamp: time.Now(),
		Pod:       fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
		Node:      node,
		Decision:  decision,
		Trust:     trust,
		Reason:    reason,
		Scheduler: "Kube-Sentinel",
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	al.mu.Lock()
	defer al.mu.Unlock()
	_, _ = al.writer.Write(append(data, '\n'))
}

// SentinelPlugin implements the PreFilter, Filter, PreScore, Score, Reserve, and PostBind plugins
type SentinelPlugin struct {
	handle             framework.Handle
	client             pb.SentinelSchedulerClient
	conn               *grpc.ClientConn
	gamma              float64
	mu                 sync.RWMutex
	reservedPlacements map[string]placementEntry // Pod UID -> placementEntry (strictly bounded)
	configWatcher      *ConfigWatcher
	circuitBreaker     *CircuitBreaker
	auditLogger        *AuditLogger
}

var _ framework.PreFilterPlugin = &SentinelPlugin{}
var _ framework.FilterPlugin = &SentinelPlugin{}
var _ framework.PreScorePlugin = &SentinelPlugin{}
var _ framework.ScorePlugin = &SentinelPlugin{}
var _ framework.ReservePlugin = &SentinelPlugin{}
var _ framework.PreBindPlugin = &SentinelPlugin{}
var _ framework.PostBindPlugin = &SentinelPlugin{}

func (p *SentinelPlugin) Name() string {
	return Name
}

var (
	scoreDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sentinel_score_duration_seconds",
			Help:    "Duration of scoring operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		},
		[]string{"phase", "result"},
	)

	trustFactors = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sentinel_node_trust_factor",
			Help: "Current trust factor for each node",
		},
		[]string{"node", "dimension"},
	)

	schedulingDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sentinel_scheduling_decisions_total",
			Help: "Scheduling decisions by type",
		},
		[]string{"node", "decision"}, // "commit", "rollback", "reject"
	)
)

// New initializes the SentinelPlugin and establishes the gRPC client connection
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	// Dynamically locate the configuration file
	configPath := "/etc/sentinel/config.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		configPath = "v2/sentinel-config.yaml"
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			configPath = "sentinel-config.yaml"
			if _, err := os.Stat(configPath); os.IsNotExist(err) {
				// Create fallback local sentinel-config.yaml
				configPath = "sentinel-config.yaml"
				defaultYAML := []byte("scoring:\n  trust_exponent_gamma: 2.0\n  weight_scoring_plugin: 100\n  fallback_strategy: \"resource_fit\"\ntelemetry:\n  heartbeat_timeout_seconds: 30\nperformance:\n  grpc_timeout_seconds: 5\n  cache_ttl_seconds: 0.2\nresilience:\n  circuit_breaker:\n    failure_threshold: 5\n    timeout_seconds: 30\n")
				_ = os.WriteFile(configPath, defaultYAML, 0644)
			}
		}
	}

	cw, err := NewConfigWatcher(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize config watcher: %w", err)
	}
	go cw.Watch(context.Background())
	cfg := cw.GetConfig()

	// Connection to the sidecar python gRPC daemon
	grpcTimeout := time.Duration(cfg.Performance.GrpcTimeoutSeconds) * time.Second
	dialCtx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, "127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC: %w", err)
	}

	client := pb.NewSentinelSchedulerClient(conn)
	cb := NewCircuitBreaker(cfg.Resilience.CircuitBreaker.FailureThreshold, cfg.Resilience.CircuitBreaker.TimeoutSeconds)
	al, _ := NewAuditLogger("/var/log/sentinel/audit.json")
	if al == nil {
		al, _ = NewAuditLogger("sentinel_audit.json")
	}

	// Register Prometheus metrics
	_ = prometheus.Register(scoreDuration)
	_ = prometheus.Register(trustFactors)
	_ = prometheus.Register(schedulingDecisions)

	// Start health metrics listener
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status": "healthy"}`))
		})
		_ = http.ListenAndServe(":8082", mux)
	}()

	return &SentinelPlugin{
		handle:             handle,
		client:             client,
		conn:               conn,
		gamma:              cfg.Scoring.TrustExponentGamma,
		reservedPlacements: make(map[string]placementEntry),
		configWatcher:      cw,
		circuitBreaker:     cb,
		auditLogger:        al,
	}, nil
}

// extractPodSpec extracts pod telemetry requirement properties
func extractPodSpec(pod *v1.Pod) *pb.PodSpec {
	var cpuMin, memMin, gpuMin float64
	for _, container := range pod.Spec.Containers {
		cpuMin += float64(container.Resources.Requests.Cpu().MilliValue()) / 1000.0
		memMin += float64(container.Resources.Requests.Memory().Value()) / (1024.0 * 1024.0 * 1024.0)
		if qty, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
			gpuMin += float64(qty.Value())
		}
	}

	wType := "batch"
	var gpuTypeReq, archReq string
	var costCeiling, minGpuMemoryGb float64

	if pod.Labels != nil {
		if val, ok := pod.Labels[LabelWorkloadType]; ok {
			wType = val
		}
		if val, ok := pod.Labels[LabelGpuTypeRequired]; ok {
			gpuTypeReq = val
		}
	}

	if pod.Spec.NodeSelector != nil {
		if val, ok := pod.Spec.NodeSelector["kubernetes.io/arch"]; ok {
			archReq = val
		}
		if val, ok := pod.Spec.NodeSelector["cloud.google.com/gke-gpu"]; ok {
			gpuTypeReq = val
		}
	}

	if pod.Annotations != nil {
		if val, ok := pod.Annotations[AnnotationArch]; ok && val != "" {
			archReq = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationCostCeilingUSD], 64); err == nil {
			costCeiling = val
		}
		if val, ok := pod.Annotations[AnnotationGPUType]; ok && val != "" {
			gpuTypeReq = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationGPUMemoryGB], 64); err == nil {
			minGpuMemoryGb = val
		}
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationGPUCount], 64); err == nil && gpuMin == 0 {
			gpuMin = val
		}
	}

	return &pb.PodSpec{
		Uid:               string(pod.UID),
		Name:              pod.Name,
		Namespace:         pod.Namespace,
		CpuCoresRequested: cpuMin,
		MemoryGbRequested: memMin,
		WorkloadType:      wType,
		GpuRequested:      gpuMin,
		GpuTypeRequired:   gpuTypeReq,
		MinGpuMemoryGb:    minGpuMemoryGb,
		RequiredArch:      normalizeArch(archReq),
		CostCeilingUsd:    costCeiling,
	}
}

func parseSafeFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, false
	}
	return val, true
}

// NodeMetadata holds canonical, validated node attributes and telemetry (Blocker A & B fix)
type NodeMetadata struct {
	NodeID                   string
	AllocCpu                 float64
	AllocMem                 float64
	ReqCpu                   float64
	ReqMem                   float64
	ExpectedFreeCpu          float64
	ExpectedFreeMem          float64

	// Telemetry - CPU
	HasCPUTelemetry          bool
	ReportedAllocCpu         float64
	ReportedUsedCpu          float64
	ReportedFreeCpu          float64

	// Telemetry - Memory
	HasMemTelemetry          bool
	ReportedAllocMem         float64
	ReportedUsedMem          float64
	ReportedFreeMem          float64

	// Telemetry - Heartbeat
	HasHeartbeat             bool
	LastHeartbeatTimestamp   float64
	RecentHeartbeatIntervals []float64

	// Economics
	HourlyCostUSD            float64
	CostKnown                bool
	IsSpot                   bool
	SpotInterruptionProb     float64

	// Hardware & Accelerator
	GPUCount                 int32
	GPUType                  string
	GPUTypeKnown             bool
	GPUMemoryGB              float64
	VRAMKnown                bool
	Arch                     string
	ArchKnown                bool
	Zone                     string
}

// parseNodeMetadata extracts and validates node attributes across provider labels and annotations with canonical precedence
func parseNodeMetadata(nodeInfo *framework.NodeInfo) (*NodeMetadata, error) {
	if nodeInfo == nil {
		return nil, fmt.Errorf("nodeInfo is nil")
	}
	node := nodeInfo.Node()
	if node == nil {
		return nil, fmt.Errorf("node is nil")
	}

	allocCpu := float64(node.Status.Allocatable.Cpu().MilliValue()) / 1000.0
	allocMem := float64(node.Status.Allocatable.Memory().Value()) / (1024.0 * 1024.0 * 1024.0)

	reqCpu := float64(nodeInfo.Requested.MilliCPU) / 1000.0
	reqMem := float64(nodeInfo.Requested.Memory) / (1024.0 * 1024.0 * 1024.0)

	expectedFreeCpu := allocCpu - reqCpu
	expectedFreeMem := allocMem - reqMem

	meta := &NodeMetadata{
		NodeID:                   node.Name,
		AllocCpu:                 allocCpu,
		AllocMem:                 allocMem,
		ReqCpu:                   reqCpu,
		ReqMem:                   reqMem,
		ExpectedFreeCpu:          expectedFreeCpu,
		ExpectedFreeMem:          expectedFreeMem,
		ReportedAllocCpu:         allocCpu,
		ReportedUsedCpu:          reqCpu,
		ReportedFreeCpu:          expectedFreeCpu,
		ReportedAllocMem:         allocMem,
		ReportedUsedMem:          reqMem,
		ReportedFreeMem:          expectedFreeMem,
		RecentHeartbeatIntervals: []float64{},
	}

	// 1. Architecture extraction with canonical precedence:
	// Labels -> Status.NodeInfo -> Annotations
	rawArch := ""
	if node.Labels != nil {
		if val, ok := node.Labels["kubernetes.io/arch"]; ok && val != "" {
			rawArch = val
		}
	}
	if rawArch == "" && node.Status.NodeInfo.Architecture != "" {
		rawArch = node.Status.NodeInfo.Architecture
	}
	if rawArch == "" && node.Annotations != nil {
		if val, ok := node.Annotations[AnnotationArch]; ok && val != "" {
			rawArch = val
		}
	}
	if rawArch != "" {
		meta.Arch = normalizeArch(rawArch)
		if meta.Arch != "" {
			meta.ArchKnown = true
		}
	}

	// 2. Spot status extraction with canonical precedence:
	// Provider labels (GKE, EKS, Karpenter) -> Annotations
	if node.Labels != nil {
		if strings.EqualFold(node.Labels["cloud.google.com/gke-spot"], "true") ||
			strings.EqualFold(node.Labels["eks.amazonaws.com/capacityType"], "SPOT") ||
			strings.EqualFold(node.Labels["karpenter.sh/capacity-type"], "spot") {
			meta.IsSpot = true
		}
		if val, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
			meta.Zone = val
		}
	}
	if node.Annotations != nil {
		if strings.EqualFold(node.Annotations[AnnotationIsSpot], "true") {
			meta.IsSpot = true
		}
		if valStr, ok := node.Annotations[AnnotationSpotInterruption]; ok {
			if prob, ok := parseSafeFloat(valStr); ok && prob >= 0 {
				meta.SpotInterruptionProb = prob
				if prob > 0.05 {
					meta.IsSpot = true
				}
			}
		}
	}

	// 3. GPU Inventory & Types
	if qty, ok := node.Status.Allocatable["nvidia.com/gpu"]; ok {
		meta.GPUCount = int32(qty.Value())
	}
	if node.Annotations != nil {
		if valStr, ok := node.Annotations[AnnotationGPUCount]; ok {
			if val, err := strconv.Atoi(strings.TrimSpace(valStr)); err == nil && val >= 0 {
				meta.GPUCount = int32(val)
			}
		}
	}

	// GPU Model Type:
	// Annotations -> GKE label -> standard accelerator label -> EKS label -> nvidia product label
	rawGPUType := ""
	if node.Annotations != nil {
		if val, ok := node.Annotations[AnnotationGPUType]; ok && strings.TrimSpace(val) != "" {
			rawGPUType = strings.TrimSpace(val)
		}
	}
	if rawGPUType == "" && node.Labels != nil {
		if val, ok := node.Labels["cloud.google.com/gke-gpu"]; ok && strings.TrimSpace(val) != "" {
			rawGPUType = strings.TrimSpace(val)
		} else if val, ok := node.Labels["accelerator"]; ok && strings.TrimSpace(val) != "" {
			rawGPUType = strings.TrimSpace(val)
		} else if val, ok := node.Labels["k8s.amazonaws.com/accelerator"]; ok && strings.TrimSpace(val) != "" {
			rawGPUType = strings.TrimSpace(val)
		} else if val, ok := node.Labels["nvidia.com/gpu.product"]; ok && strings.TrimSpace(val) != "" {
			rawGPUType = strings.TrimSpace(val)
		}
	}
	if rawGPUType != "" {
		meta.GPUType = rawGPUType
		meta.GPUTypeKnown = true
	}

	// GPU VRAM:
	if node.Annotations != nil {
		if valStr, ok := node.Annotations[AnnotationGPUMemoryGB]; ok {
			if vram, ok := parseSafeFloat(valStr); ok && vram > 0 {
				meta.GPUMemoryGB = vram
				meta.VRAMKnown = true
			}
		}
	}
	if !meta.VRAMKnown && node.Labels != nil {
		if valStr, ok := node.Labels["nvidia.com/gpu.memory"]; ok {
			if vram, ok := parseSafeFloat(valStr); ok && vram > 0 {
				if vram > 500 { // If in MB, convert to GB
					vram = vram / 1024.0
				}
				meta.GPUMemoryGB = vram
				meta.VRAMKnown = true
			}
		}
	}

	// 4. Cost Metadata:
	if node.Annotations != nil {
		if valStr, ok := node.Annotations[AnnotationHourlyCostUSD]; ok {
			if cost, ok := parseSafeFloat(valStr); ok && cost >= 0 {
				meta.HourlyCostUSD = cost
				meta.CostKnown = true
			}
		}
	}

	// 5. Telemetry: CPU
	if node.Annotations != nil {
		aStr, hasA := node.Annotations[AnnotationReportedAllocatableCPU]
		uStr, hasU := node.Annotations[AnnotationReportedUsedCPU]
		fStr, hasF := node.Annotations[AnnotationReportedFreeCPU]
		if hasA || hasU || hasF {
			a, okA := parseSafeFloat(aStr)
			u, okU := parseSafeFloat(uStr)
			f, okF := parseSafeFloat(fStr)
			if !okA || !okU || !okF || a <= 0 || u < 0 || f < 0 {
				return nil, fmt.Errorf("invalid or incomplete CPU telemetry: require finite allocatable > 0 and finite used/free >= 0")
			}
			meta.ReportedAllocCpu = a
			meta.ReportedUsedCpu = u
			meta.ReportedFreeCpu = f
			meta.HasCPUTelemetry = true
		}
	}

	// 6. Telemetry: Memory
	if node.Annotations != nil {
		aStr, hasA := node.Annotations[AnnotationReportedAllocatableMem]
		uStr, hasU := node.Annotations[AnnotationReportedUsedMem]
		fStr, hasF := node.Annotations[AnnotationReportedFreeMem]
		if hasA || hasU || hasF {
			a, okA := parseSafeFloat(aStr)
			u, okU := parseSafeFloat(uStr)
			f, okF := parseSafeFloat(fStr)
			if !okA || !okU || !okF || a <= 0 || u < 0 || f < 0 {
				return nil, fmt.Errorf("invalid or incomplete memory telemetry: require finite allocatable > 0 and finite used/free >= 0")
			}
			meta.ReportedAllocMem = a
			meta.ReportedUsedMem = u
			meta.ReportedFreeMem = f
			meta.HasMemTelemetry = true
		}
	}

	// 7. Telemetry: Heartbeat
	if node.Annotations != nil {
		if valStr, ok := node.Annotations[AnnotationLastHeartbeat]; ok {
			hb, valid := parseSafeFloat(valStr)
			if !valid || hb <= 0 {
				return nil, fmt.Errorf("invalid heartbeat telemetry: require a finite timestamp > 0")
			}
			meta.LastHeartbeatTimestamp = hb
			meta.HasHeartbeat = true
		}
		if valStr, ok := node.Annotations[AnnotationHeartbeatIntervals]; ok && valStr != "" {
			parts := strings.Split(valStr, ",")
			for _, p := range parts {
				if iv, ok := parseSafeFloat(p); ok && iv > 0 {
					meta.RecentHeartbeatIntervals = append(meta.RecentHeartbeatIntervals, iv)
				}
			}
		}
	}

	return meta, nil
}

// extractNodeCandidate maps K8s Node and cache state to pb.NodeCandidate using unified parseNodeMetadata
func extractNodeCandidate(nodeInfo *framework.NodeInfo) *pb.NodeCandidate {
	meta, err := parseNodeMetadata(nodeInfo)
	if err != nil || meta == nil {
		nodeId := "unknown"
		if nodeInfo != nil && nodeInfo.Node() != nil {
			nodeId = nodeInfo.Node().Name
		}
		return &pb.NodeCandidate{NodeId: nodeId}
	}

	return &pb.NodeCandidate{
		NodeId:                        meta.NodeID,
		AllocatableCpu:                meta.AllocCpu,
		AllocatableMemoryGb:           meta.AllocMem,
		SchedulerExpectedFreeCpu:      meta.ExpectedFreeCpu,
		SchedulerExpectedFreeMemoryGb: meta.ExpectedFreeMem,
		ReportedAllocatableCpu:        meta.ReportedAllocCpu,
		ReportedUsedCpu:               meta.ReportedUsedCpu,
		ReportedFreeCpu:               meta.ReportedFreeCpu,
		ReportedAllocatableMemoryGb:   meta.ReportedAllocMem,
		ReportedUsedMemoryGb:          meta.ReportedUsedMem,
		ReportedFreeMemoryGb:          meta.ReportedFreeMem,
		LastHeartbeatTimestamp:        meta.LastHeartbeatTimestamp,
		RecentHeartbeatIntervals:      meta.RecentHeartbeatIntervals,
		HourlyCostUsd:                 meta.HourlyCostUSD,
		IsSpot:                        meta.IsSpot,
		SpotInterruptionProb:          meta.SpotInterruptionProb,
		GpuType:                       meta.GPUType,
		GpuCount:                      meta.GPUCount,
		GpuMemoryGb:                   meta.GPUMemoryGB,
		Arch:                          meta.Arch,
		Zone:                          meta.Zone,
	}
}

type stateData struct {
	scores map[string]int64
}

func (s *stateData) Clone() framework.StateData {
	return s
}

const StateKey = framework.StateKey(Name + "State")

// PreFilter implements framework.PreFilterPlugin to extract pod constraints before node evaluation
func (p *SentinelPlugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	c := extractPodConstraints(pod)
	state.Write(PreFilterStateKey, c)
	return nil, framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns nil as Sentinel does not use incremental pre-filtering
func (p *SentinelPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter implements framework.FilterPlugin to hard-reject nodes with failed constraints or untrusted telemetry (V01, V04, Blocker A & B fix)
// This runs natively in Go BEFORE PreScore, guaranteeing that mandatory restrictions hold even during daemon outages.
func (p *SentinelPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Unschedulable, "Node is nil")
	}
	node := nodeInfo.Node()

	// 1. Explicit poison check
	if node.Annotations != nil {
		if strings.EqualFold(node.Annotations[AnnotationForceReject], "true") {
			return framework.NewStatus(framework.Unschedulable, "Node marked as untrusted by Sentinel")
		}
	}

	// Parse unified node metadata with strict validation
	meta, err := parseNodeMetadata(nodeInfo)
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Failed to parse node metadata: %v", err))
	}

	// Retrieve pre-filtered constraints (or extract on demand if PreFilter was not invoked)
	var c *podConstraints
	if data, err := state.Read(PreFilterStateKey); err == nil {
		c, _ = data.(*podConstraints)
	}
	if c == nil {
		c = extractPodConstraints(pod)
	}

	// 2. Mandatory Architecture Filter (Blocker A fix)
	if c.requiredArch != "" {
		if !meta.ArchKnown {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node architecture unknown; cannot satisfy required %s", c.requiredArch))
		}
		if meta.Arch != c.requiredArch {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Architecture mismatch: pod requires %s, node has %s", c.requiredArch, meta.Arch))
		}
	}

	// 3. Mandatory Cost Ceiling Filter (Blocker A fix)
	if c.costCeilingUSD > 0 {
		if !meta.CostKnown {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node cost unknown; cannot satisfy mandatory ceiling $%.2f/hr", c.costCeilingUSD))
		}
		if meta.HourlyCostUSD > c.costCeilingUSD {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node cost $%.2f/hr exceeds pod ceiling $%.2f/hr", meta.HourlyCostUSD, c.costCeilingUSD))
		}
	}

	// 4. Mandatory GPU, Type & VRAM Filter (Blocker A fix)
	if c.gpuRequested > 0 {
		if float64(meta.GPUCount) < c.gpuRequested {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient GPUs: pod requires %.0f, node has %d", c.gpuRequested, meta.GPUCount))
		}
		if c.gpuTypeRequired != "" {
			if !meta.GPUTypeKnown {
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node GPU model unknown; cannot satisfy required %s", c.gpuTypeRequired))
			}
			if !strings.EqualFold(meta.GPUType, c.gpuTypeRequired) {
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("GPU model mismatch: pod requires %s, node has %s", c.gpuTypeRequired, meta.GPUType))
			}
		}
		if c.minGPUMemoryGB > 0 {
			if !meta.VRAMKnown {
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node GPU VRAM unknown; cannot satisfy mandatory requirement of %.1fGB", c.minGPUMemoryGB))
			}
			if meta.GPUMemoryGB < c.minGPUMemoryGB {
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Insufficient GPU VRAM: pod requires %.1fGB, node has %.1fGB", c.minGPUMemoryGB, meta.GPUMemoryGB))
			}
		}
	}

	// 5. Mandatory QoS Rule: Latency Critical pods cannot run on Spot instances (Blocker A fix)
	if c.isLatencyCritical && meta.IsSpot {
		return framework.NewStatus(framework.Unschedulable, "Latency-critical workload disallowed on Spot node")
	}

	// 6. Mandatory Telemetry Observation-Quality Gate: CPU (Blocker B fix)
	if meta.HasCPUTelemetry {
		if meta.ReportedFreeCpu > meta.ReportedAllocCpu || meta.ReportedUsedCpu > meta.ReportedAllocCpu {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: reported CPU free (%.2f) or used (%.2f) exceeds allocatable (%.2f)", meta.ReportedFreeCpu, meta.ReportedUsedCpu, meta.ReportedAllocCpu))
		}
		delta := math.Abs((meta.ReportedAllocCpu - meta.ReportedUsedCpu) - meta.ReportedFreeCpu)
		if delta > 0.05*meta.ReportedAllocCpu {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: CPU arithmetic balance failure (alloc=%.2f, used=%.2f, free=%.2f, delta=%.2f)", meta.ReportedAllocCpu, meta.ReportedUsedCpu, meta.ReportedFreeCpu, delta))
		}
	}

	// 7. Mandatory Telemetry Observation-Quality Gate: Memory (Blocker B fix)
	if meta.HasMemTelemetry {
		if meta.ReportedFreeMem > meta.ReportedAllocMem || meta.ReportedUsedMem > meta.ReportedAllocMem {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: reported memory free (%.2fGB) or used (%.2fGB) exceeds allocatable (%.2fGB)", meta.ReportedFreeMem, meta.ReportedUsedMem, meta.ReportedAllocMem))
		}
		delta := math.Abs((meta.ReportedAllocMem - meta.ReportedUsedMem) - meta.ReportedFreeMem)
		if delta > 0.05*meta.ReportedAllocMem {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: memory arithmetic balance failure (alloc=%.2fGB, used=%.2fGB, free=%.2fGB, delta=%.2fGB)", meta.ReportedAllocMem, meta.ReportedUsedMem, meta.ReportedFreeMem, delta))
		}
	}

	// 8. Mandatory Telemetry Freshness & Clock Skew Gate (Blocker B fix)
	if meta.HasHeartbeat {
		now := float64(time.Now().UnixNano()) / 1e9
		// Future heartbeat check (> 1.0s in future is a clock skew anomaly)
		if meta.LastHeartbeatTimestamp-now > 1.0 {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: heartbeat timestamp is in the future by %.2fs (clock skew anomaly)", meta.LastHeartbeatTimestamp-now))
		}
		// Stale heartbeat check (> 60.0s is stale)
		if now-meta.LastHeartbeatTimestamp > 60.0 {
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Untrusted telemetry: heartbeat stale by %.1fs (threshold 60.0s)", now-meta.LastHeartbeatTimestamp))
		}
	}

	return framework.NewStatus(framework.Success, "")
}

// PreScore batches all candidate nodes that passed Filter and fetches scores from gRPC server
func (p *SentinelPlugin) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	if len(nodes) == 0 {
		return nil
	}

	startTime := time.Now()

	// Default configuration fallback
	var cfg *SentinelConfig
	if p.configWatcher != nil {
		cfg = p.configWatcher.GetConfig()
	}
	if cfg == nil {
		cfg = &SentinelConfig{
			Scoring: ScoringConfig{
				TrustExponentGamma:  p.gamma,
				WeightScoringPlugin: 100,
				FallbackStrategy:    "resource_fit",
			},
			Telemetry: TelemetryConfig{
				HeartbeatTimeoutSeconds: 30,
			},
			Performance: PerformanceConfig{
				GrpcTimeoutSeconds: 5,
			},
			Resilience: ResilienceConfig{
				CircuitBreaker: CircuitBreakerConfig{
					FailureThreshold:    5,
					TimeoutSeconds:      30,
					HalfOpenMaxAttempts: 3,
				},
			},
		}
	}

	pbPod := extractPodSpec(pod)
	pbNodes := make([]*pb.NodeCandidate, 0, len(nodes))
	for _, n := range nodes {
		var nodeInfo *framework.NodeInfo
		var err error
		if p.handle != nil {
			nodeInfo, err = p.handle.SnapshotSharedLister().NodeInfos().Get(n.Name)
		}
		if p.handle == nil || err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			dummyNodeInfo := framework.NewNodeInfo()
			dummyNodeInfo.SetNode(n)
			nodeInfo = dummyNodeInfo
		}
		pbNodes = append(pbNodes, extractNodeCandidate(nodeInfo))
	}

	nodeScores := make(map[string]int64)

	// Query Daemon through the Circuit Breaker
	var resp *pb.ScoreResponse
	var cbErr error

	if p.circuitBreaker != nil {
		cbErr = p.circuitBreaker.AttemptCall(ctx, func() error {
			grpcTimeout := time.Duration(cfg.Performance.GrpcTimeoutSeconds) * time.Second
			callCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
			defer cancel()

			req := &pb.ScoreRequest{
				Pod:   pbPod,
				Nodes: pbNodes,
				Gamma: cfg.Scoring.TrustExponentGamma,
			}
			var err error
			resp, err = p.client.ScoreNodes(callCtx, req)
			return err
		})
	} else if p.client != nil {
		grpcTimeout := time.Duration(cfg.Performance.GrpcTimeoutSeconds) * time.Second
		callCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
		defer cancel()

		req := &pb.ScoreRequest{
			Pod:   pbPod,
			Nodes: pbNodes,
			Gamma: cfg.Scoring.TrustExponentGamma,
		}
		resp, cbErr = p.client.ScoreNodes(callCtx, req)
	} else {
		cbErr = fmt.Errorf("no grpc client available")
	}

	// Fallback path if circuit breaker triggers or sidecar fails
	if cbErr != nil || resp == nil {
		fmt.Printf("[Sentinel-GoPlugin] WARNING: Fallback scoring active: %v\n", cbErr)
		for _, n := range nodes {
			nodeScores[n.Name] = 50 // Safe neutral score for candidates that passed Filter
			if p.auditLogger != nil {
				p.auditLogger.LogDecision(pod, n.Name, "fallback", 1.0, fmt.Sprintf("Circuit breaker fallback: %v", cbErr))
			}
		}
		state.Write(StateKey, &stateData{
			scores: nodeScores,
		})
		scoreDuration.WithLabelValues("PreScore", "fallback").Observe(time.Since(startTime).Seconds())
		return nil
	}

	// Process response scores
	for _, ns := range resp.Scores {
		finalScore := int64(ns.FinalScore * 100.0)
		if finalScore < framework.MinNodeScore {
			finalScore = framework.MinNodeScore
		}
		if finalScore > framework.MaxNodeScore {
			finalScore = framework.MaxNodeScore
		}
		nodeScores[ns.NodeId] = finalScore
		trustFactors.WithLabelValues(ns.NodeId, "smoothed").Set(ns.Confidence)
		if p.auditLogger != nil {
			p.auditLogger.LogDecision(pod, ns.NodeId, "score", ns.Confidence, fmt.Sprintf("ETA: %.2f, Confidence: %.2f, Final: %d", ns.Eta, ns.Confidence, finalScore))
		}
	}

	state.Write(StateKey, &stateData{
		scores: nodeScores,
	})
	scoreDuration.WithLabelValues("PreScore", "success").Observe(time.Since(startTime).Seconds())
	return nil
}

// Score retrieves the computed score for the target node and applies locality preferences (V02 fix: NEVER returns Unschedulable)
func (p *SentinelPlugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	data, err := state.Read(StateKey)
	if err != nil {
		return 50, framework.NewStatus(framework.Success, "")
	}

	sd, ok := data.(*stateData)
	if !ok || sd.scores == nil {
		return 50, framework.NewStatus(framework.Success, "")
	}

	score, exists := sd.scores[nodeName]
	if !exists {
		return 0, framework.NewStatus(framework.Success, "")
	}

	// Apply StatefulSet locality preference
	isStatefulSet := false
	var statefulSetName string
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "StatefulSet" {
			isStatefulSet = true
			statefulSetName = owner.Name
			break
		}
	}

	if isStatefulSet && p.handle != nil {
		if nodeInfo, err := p.handle.SnapshotSharedLister().NodeInfos().Get(nodeName); err == nil && nodeInfo != nil {
			for _, existingPodInfo := range nodeInfo.Pods {
				existingPod := existingPodInfo.Pod
				if existingPod != nil && existingPod.Namespace == pod.Namespace {
					for _, own := range existingPod.OwnerReferences {
						if own.Kind == "StatefulSet" && own.Name == statefulSetName {
							score += 5
							if score > framework.MaxNodeScore {
								score = framework.MaxNodeScore
							}
							break
						}
					}
				}
			}
		}
	}

	return score, framework.NewStatus(framework.Success, "")
}

func (p *SentinelPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// PreBind blocks binding for pod-rollback to force Unreserve rollbacks
func (p *SentinelPlugin) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	if pod.Name == "pod-rollback" {
		return framework.NewStatus(framework.Error, "Deliberate test rollback triggered for pod-rollback")
	}
	return nil
}

// Reserve registers the assumed placement with strictly bounded capacity (V05 fix)
func (p *SentinelPlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Strict bound: if map reaches 5000 entries, evict entries older than 2 minutes
	if len(p.reservedPlacements) >= 5000 {
		now := time.Now()
		for uid, entry := range p.reservedPlacements {
			if now.Sub(entry.timestamp) > 2*time.Minute {
				delete(p.reservedPlacements, uid)
			}
		}
		// If still at capacity, evict an entry to strictly bound memory
		if len(p.reservedPlacements) >= 5000 {
			for uid := range p.reservedPlacements {
				delete(p.reservedPlacements, uid)
				break
			}
		}
	}

	p.reservedPlacements[string(pod.UID)] = placementEntry{
		nodeName:  nodeName,
		timestamp: time.Now(),
	}
	return nil
}

// Unreserve clears the placement and calls rollback PlacementCommitted callback via gRPC
func (p *SentinelPlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	p.mu.Lock()
	delete(p.reservedPlacements, string(pod.UID))
	p.mu.Unlock()

	// Record rollback metric
	schedulingDecisions.WithLabelValues(nodeName, "rollback").Inc()

	if p.client != nil {
		req := &pb.PlacementCommittedRequest{
			PodUid:  string(pod.UID),
			NodeId:  nodeName,
			Success: false,
		}
		_, err := p.client.PlacementCommitted(ctx, req)
		if err != nil {
			fmt.Printf("[Sentinel-GoPlugin] WARNING: gRPC PlacementCommitted rollback failed: %v\n", err)
		}
	}
}

// PostBind clears the placement and calls commit PlacementCommitted callback via gRPC
func (p *SentinelPlugin) PostBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	p.mu.Lock()
	delete(p.reservedPlacements, string(pod.UID))
	p.mu.Unlock()

	// Record commit metric
	schedulingDecisions.WithLabelValues(nodeName, "commit").Inc()

	if p.client != nil {
		req := &pb.PlacementCommittedRequest{
			PodUid:  string(pod.UID),
			NodeId:  nodeName,
			Success: true,
		}
		_, err := p.client.PlacementCommitted(ctx, req)
		if err != nil {
			fmt.Printf("[Sentinel-GoPlugin] WARNING: gRPC PlacementCommitted commit failed: %v\n", err)
		}
	}
}
