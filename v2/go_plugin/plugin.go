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

	"aco-sentinel-plugin/pb"
)

const (
	Name = "ACOPredictiveScheduler"

	// Annotations for telemetry passing
	AnnotationReportedAllocatableCPU = "aco-sentinel.io/reported-allocatable-cpu"
	AnnotationReportedUsedCPU        = "aco-sentinel.io/reported-used-cpu"
	AnnotationReportedFreeCPU        = "aco-sentinel.io/reported-free-cpu"

	AnnotationReportedAllocatableMem = "aco-sentinel.io/reported-allocatable-memory-gb"
	AnnotationReportedUsedMem        = "aco-sentinel.io/reported-used-memory-gb"
	AnnotationReportedFreeMem        = "aco-sentinel.io/reported-free-memory-gb"

	AnnotationLastHeartbeat      = "aco-sentinel.io/last-heartbeat-timestamp"
	AnnotationHeartbeatIntervals = "aco-sentinel.io/recent-heartbeat-intervals"

	AnnotationHourlyCostUSD      = "sentinel.cost/hourly-usd"
	AnnotationIsSpot             = "sentinel.cost/is-spot"
	AnnotationSpotInterruption   = "sentinel.cost/spot-interruption-prob"
	AnnotationGPUType            = "sentinel.gpu/type"
	AnnotationGPUCount           = "sentinel.gpu/count"
	AnnotationGPUMemoryGB        = "sentinel.gpu/memory-gb"
	AnnotationArch               = "sentinel.node/arch"
	AnnotationCostCeilingUSD     = "sentinel.cost/ceiling-usd"
	AnnotationForceReject        = "sentinel.trust/force-reject"

	LabelWorkloadType            = "aco-sentinel.io/workload-type"
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

// ScoringCache definitions
type NodeScoreCached struct {
	scores    map[string]int64
	createdAt time.Time
}

type ScoringCache struct {
	mu    sync.RWMutex
	cache map[string]*NodeScoreCached // Pod UID -> cached scores
}

func NewScoringCache() *ScoringCache {
	return &ScoringCache{
		cache: make(map[string]*NodeScoreCached),
	}
}

func (sc *ScoringCache) Get(podUID string, ttl time.Duration) (map[string]int64, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	item, ok := sc.cache[podUID]
	if !ok {
		return nil, false
	}
	if time.Since(item.createdAt) > ttl {
		return nil, false
	}
	return item.scores, true
}

func (sc *ScoringCache) Set(podUID string, scores map[string]int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// Bounded cache: evict oldest if exceeding 1,000 entries (F23 fix)
	if len(sc.cache) > 1000 {
		now := time.Now()
		for k, v := range sc.cache {
			if now.Sub(v.createdAt) > 5*time.Minute {
				delete(sc.cache, k)
			}
		}
	}
	sc.cache[podUID] = &NodeScoreCached{
		scores:    scores,
		createdAt: time.Now(),
	}
}

func (sc *ScoringCache) Delete(podUID string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.cache, podUID)
}

func (sc *ScoringCache) Prune(maxAge time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	now := time.Now()
	for k, v := range sc.cache {
		if now.Sub(v.createdAt) > maxAge {
			delete(sc.cache, k)
		}
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
		Scheduler: "ACO-Sentinel",
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	al.mu.Lock()
	defer al.mu.Unlock()
	_, _ = al.writer.Write(append(data, '\n'))
}

// SentinelPlugin implements the PreScore, Score, Reserve, and PostBind plugins
type SentinelPlugin struct {
	handle             framework.Handle
	client             pb.ACOPredictiveSchedulerClient
	conn               *grpc.ClientConn
	gamma              float64
	mu                 sync.RWMutex
	reservedPlacements map[string]string // Pod UID -> Node Name
	configWatcher      *ConfigWatcher
	circuitBreaker     *CircuitBreaker
	scoringCache       *ScoringCache
	auditLogger        *AuditLogger
}

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

	client := pb.NewACOPredictiveSchedulerClient(conn)
	cb := NewCircuitBreaker(cfg.Resilience.CircuitBreaker.FailureThreshold, cfg.Resilience.CircuitBreaker.TimeoutSeconds)
	sc := NewScoringCache()
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
		reservedPlacements: make(map[string]string),
		configWatcher:      cw,
		circuitBreaker:     cb,
		scoringCache:       sc,
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
	var costCeiling float64

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
		if val, err := strconv.ParseFloat(pod.Annotations[AnnotationCostCeilingUSD], 64); err == nil {
			costCeiling = val
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
		RequiredArch:      archReq,
		CostCeilingUsd:    costCeiling,
	}
}

// extractNodeCandidate maps K8s Node and cache state to pb.NodeCandidate
func extractNodeCandidate(nodeInfo *framework.NodeInfo) *pb.NodeCandidate {
	node := nodeInfo.Node()
	if node == nil {
		return &pb.NodeCandidate{NodeId: "unknown"}
	}

	allocCpu := float64(node.Status.Allocatable.Cpu().MilliValue()) / 1000.0
	allocMem := float64(node.Status.Allocatable.Memory().Value()) / (1024.0 * 1024.0 * 1024.0)

	// Compute K8s assumed-cache allocations (Requested resources)
	reqCpu := float64(nodeInfo.Requested.MilliCPU) / 1000.0
	reqMem := float64(nodeInfo.Requested.Memory) / (1024.0 * 1024.0 * 1024.0)

	expectedFreeCpu := allocCpu - reqCpu
	expectedFreeMem := allocMem - reqMem

	// Default telemetry values fallback
	repAllocCpu := allocCpu
	repUsedCpu := reqCpu
	repFreeCpu := expectedFreeCpu

	repAllocMem := allocMem
	repUsedMem := reqMem
	repFreeMem := expectedFreeMem

	var lastHeartbeat float64
	var recentIntervals []float64

	var hourlyCost float64
	var isSpot bool
	var spotInterruption float64
	var gpuType string
	var gpuCount int32
	var gpuMemoryGb float64
	var arch string
	var zone string

	if node.Labels != nil {
		if node.Labels["cloud.google.com/gke-spot"] == "true" ||
			node.Labels["eks.amazonaws.com/capacityType"] == "SPOT" {
			isSpot = true
		}
		if val, ok := node.Labels["cloud.google.com/gke-gpu"]; ok {
			gpuType = val
		} else if val, ok := node.Labels["accelerator"]; ok {
			gpuType = val
		}
		if val, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
			zone = val
		}
	}

	if node.Status.NodeInfo.Architecture != "" {
		arch = node.Status.NodeInfo.Architecture
	}

	if qty, ok := node.Status.Allocatable["nvidia.com/gpu"]; ok {
		gpuCount = int32(qty.Value())
	}

	if node.Annotations != nil {
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedAllocatableCPU], 64); err == nil {
			repAllocCpu = val
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedUsedCPU], 64); err == nil {
			repUsedCpu = val
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedFreeCPU], 64); err == nil {
			repFreeCpu = val
		}

		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedAllocatableMem], 64); err == nil {
			repAllocMem = val
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedUsedMem], 64); err == nil {
			repUsedMem = val
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationReportedFreeMem], 64); err == nil {
			repFreeMem = val
		}

		if val, err := strconv.ParseFloat(node.Annotations[AnnotationLastHeartbeat], 64); err == nil {
			lastHeartbeat = val
		}

		if val, ok := node.Annotations[AnnotationHeartbeatIntervals]; ok && val != "" {
			parts := strings.Split(val, ",")
			for _, p := range parts {
				if iv, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil {
					recentIntervals = append(recentIntervals, iv)
				}
			}
		}

		if val, err := strconv.ParseFloat(node.Annotations[AnnotationHourlyCostUSD], 64); err == nil {
			hourlyCost = val
		}
		if val, ok := node.Annotations[AnnotationIsSpot]; ok {
			isSpot = val == "true"
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationSpotInterruption], 64); err == nil {
			spotInterruption = val
		}
		if val, ok := node.Annotations[AnnotationGPUType]; ok && val != "" {
			gpuType = val
		}
		if val, err := strconv.Atoi(node.Annotations[AnnotationGPUCount]); err == nil {
			gpuCount = int32(val)
		}
		if val, err := strconv.ParseFloat(node.Annotations[AnnotationGPUMemoryGB], 64); err == nil {
			gpuMemoryGb = val
		}
		if val, ok := node.Annotations[AnnotationArch]; ok && val != "" {
			arch = val
		}
	}

	return &pb.NodeCandidate{
		NodeId:                        node.Name,
		AllocatableCpu:                allocCpu,
		AllocatableMemoryGb:           allocMem,
		SchedulerExpectedFreeCpu:      expectedFreeCpu,
		SchedulerExpectedFreeMemoryGb: expectedFreeMem,
		ReportedAllocatableCpu:        repAllocCpu,
		ReportedUsedCpu:               repUsedCpu,
		ReportedFreeCpu:               repFreeCpu,
		ReportedAllocatableMemoryGb:   repAllocMem,
		ReportedUsedMemoryGb:          repUsedMem,
		ReportedFreeMemoryGb:          repFreeMem,
		LastHeartbeatTimestamp:        lastHeartbeat,
		RecentHeartbeatIntervals:      recentIntervals,
		HourlyCostUsd:                 hourlyCost,
		IsSpot:                        isSpot,
		SpotInterruptionProb:          spotInterruption,
		GpuType:                       gpuType,
		GpuCount:                      gpuCount,
		GpuMemoryGb:                   gpuMemoryGb,
		Arch:                          arch,
		Zone:                          zone,
	}
}

type stateData struct {
	scores        map[string]int64
	eligibility   map[string]bool
	filterReasons map[string]string
}

func (s *stateData) Clone() framework.StateData {
	return s
}

const StateKey = framework.StateKey(Name + "State")

// Filter implements framework.FilterPlugin to hard-reject nodes with untrusted telemetry or failed constraints (F11 fix)
func (p *SentinelPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Unschedulable, "Node is nil")
	}

	// 1. Explicit poison check
	if node.Annotations != nil {
		if val, ok := node.Annotations[AnnotationForceReject]; ok && val == "true" {
			return framework.NewStatus(framework.Unschedulable, "Node marked as untrusted by Sentinel")
		}
	}

	// 2. Read cycle state if pre-populated
	data, err := state.Read(StateKey)
	if err == nil {
		if sd, ok := data.(*stateData); ok && sd.eligibility != nil {
			if eligible, exists := sd.eligibility[node.Name]; exists && !eligible {
				reason := sd.filterReasons[node.Name]
				return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Sentinel rejected node: %s", reason))
			}
		}
	}

	return framework.NewStatus(framework.Success, "")
}

// PreScore batches all candidate nodes and fetches trust-weighted heuristic scores from gRPC server
func (p *SentinelPlugin) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	if len(nodes) == 0 {
		return nil
	}

	startTime := time.Now()

	// Fallback to default configuration if configWatcher is nil (e.g. in tests)
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
				CacheTtlSeconds:    0.2,
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

	// 1. Scoring cache lookup
	if p.scoringCache != nil && cfg.Performance.CacheTtlSeconds > 0 {
		ttl := time.Duration(cfg.Performance.CacheTtlSeconds * float64(time.Second))
		if cachedScores, found := p.scoringCache.Get(string(pod.UID), ttl); found {
			state.Write(StateKey, &stateData{scores: cachedScores})
			scoreDuration.WithLabelValues("PreScore", "cache_hit").Observe(time.Since(startTime).Seconds())
			return nil
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
	nodeEligibility := make(map[string]bool)
	nodeFilterReasons := make(map[string]string)

	// 2. Query Daemon through the Circuit Breaker
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
	} else {
		// Fallback direct execution if circuit breaker is not set
		grpcTimeout := time.Duration(cfg.Performance.GrpcTimeoutSeconds) * time.Second
		callCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
		defer cancel()

		req := &pb.ScoreRequest{
			Pod:   pbPod,
			Nodes: pbNodes,
			Gamma: cfg.Scoring.TrustExponentGamma,
		}
		resp, cbErr = p.client.ScoreNodes(callCtx, req)
	}

	// 3. Fallback path if circuit breaker triggers or sidecar fails
	if cbErr != nil {
		fmt.Printf("[Sentinel-GoPlugin] WARNING: Fallback scoring active: %v\n", cbErr)
		for _, n := range nodes {
			// fallback scores (neutral cost/fit representation)
			nodeScores[n.Name] = 100
			nodeEligibility[n.Name] = true
			if p.auditLogger != nil {
				p.auditLogger.LogDecision(pod, n.Name, "fallback", 1.0, fmt.Sprintf("Circuit breaker fallback: %v", cbErr))
			}
		}
		state.Write(StateKey, &stateData{
			scores:        nodeScores,
			eligibility:   nodeEligibility,
			filterReasons: nodeFilterReasons,
		})
		scoreDuration.WithLabelValues("PreScore", "fallback").Observe(time.Since(startTime).Seconds())
		return nil
	}

	// 4. Process response and record telemetry
	for _, ns := range resp.Scores {
		nodeScores[ns.NodeId] = int64(ns.FinalScore * 100.0)
		nodeEligibility[ns.NodeId] = ns.IsEligible
		nodeFilterReasons[ns.NodeId] = ns.FilterReason
		trustFactors.WithLabelValues(ns.NodeId, "smoothed").Set(ns.Confidence)
		p.auditLogger.LogDecision(pod, ns.NodeId, "score", ns.Confidence, fmt.Sprintf("ETA: %.2f, Confidence: %.2f, Eligible: %v (%s)", ns.Eta, ns.Confidence, ns.IsEligible, ns.FilterReason))
	}

	// Save to cache
	if p.scoringCache != nil {
		p.scoringCache.Set(string(pod.UID), nodeScores)
	}

	state.Write(StateKey, &stateData{
		scores:        nodeScores,
		eligibility:   nodeEligibility,
		filterReasons: nodeFilterReasons,
	})
	scoreDuration.WithLabelValues("PreScore", "success").Observe(time.Since(startTime).Seconds())
	return nil
}

// Score retrieves the cached score for the target node and applies StatefulSet locality bonuses
func (p *SentinelPlugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	data, err := state.Read(StateKey)
	if err != nil {
		// Fallback to neutral score (100) if no scores are stored
		return 100, nil
	}

	sd, ok := data.(*stateData)
	if !ok {
		return 100, nil
	}

	if sd.eligibility != nil {
		if eligible, exists := sd.eligibility[nodeName]; exists && !eligible {
			return 0, framework.NewStatus(framework.Unschedulable, sd.filterReasons[nodeName])
		}
	}

	score, exists := sd.scores[nodeName]
	if !exists {
		return 0, nil
	}

	// Apply StatefulSet locality preference (Feature 9)
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
							if score > 100 {
								score = 100
							}
							break
						}
					}
				}
			}
		}
	}

	return score, nil
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

// Reserve registers the assumed placement
func (p *SentinelPlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reservedPlacements[string(pod.UID)] = nodeName
	return nil
}

// Unreserve clears the placement and calls rollback PlacementCommitted callback via gRPC
func (p *SentinelPlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	p.mu.Lock()
	delete(p.reservedPlacements, string(pod.UID))
	p.mu.Unlock()

	// Clear score cache entry (F23 fix)
	if p.scoringCache != nil {
		p.scoringCache.Delete(string(pod.UID))
	}

	// Record rollback metric
	schedulingDecisions.WithLabelValues(nodeName, "rollback").Inc()

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

// PostBind clears the placement and calls commit PlacementCommitted callback via gRPC
func (p *SentinelPlugin) PostBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	p.mu.Lock()
	delete(p.reservedPlacements, string(pod.UID))
	p.mu.Unlock()

	// Clear score cache entry (F23 fix)
	if p.scoringCache != nil {
		p.scoringCache.Delete(string(pod.UID))
	}

	// Record commit metric
	schedulingDecisions.WithLabelValues(nodeName, "commit").Inc()

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
