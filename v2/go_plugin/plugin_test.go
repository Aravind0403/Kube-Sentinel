package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	grpc "google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sentinel-plugin/pb"
)

type mockClient struct {
	pb.SentinelSchedulerClient
	scoreNodesCalled bool
	commitCalled     bool
	lastSuccess      bool
}

func (m *mockClient) ScoreNodes(ctx context.Context, in *pb.ScoreRequest, opts ...grpc.CallOption) (*pb.ScoreResponse, error) {
	m.scoreNodesCalled = true
	var scores []*pb.NodeScore
	for _, n := range in.Nodes {
		scores = append(scores, &pb.NodeScore{
			NodeId:     n.NodeId,
			FinalScore: 0.85,
			IsEligible: true,
		})
	}
	return &pb.ScoreResponse{Scores: scores}, nil
}

func (m *mockClient) PlacementCommitted(ctx context.Context, in *pb.PlacementCommittedRequest, opts ...grpc.CallOption) (*pb.PlacementCommittedResponse, error) {
	m.commitCalled = true
	m.lastSuccess = in.Success
	return &pb.PlacementCommittedResponse{Acknowledged: true}, nil
}

func TestPluginLifecycle(t *testing.T) {
	// Pod requiring amd64, $1.00 cost ceiling, latency-critical
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "critical-service",
			Namespace: "default",
			UID:       types.UID("pod-crit-1"),
			Labels: map[string]string{
				LabelWorkloadType: "LATENCY_CRITICAL",
			},
			Annotations: map[string]string{
				AnnotationArch:           "amd64",
				AnnotationCostCeilingUSD: "1.00",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
						},
					},
				},
			},
		},
	}

	// 1. Node 1: Eligible (amd64, $0.50/hr on-demand, balanced telemetry)
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1-eligible",
			Annotations: map[string]string{
				AnnotationArch:                   "x86_64", // should normalize to amd64
				AnnotationHourlyCostUSD:          "0.50",
				AnnotationIsSpot:                 "false",
				AnnotationReportedAllocatableCPU: "8.0",
				AnnotationReportedUsedCPU:        "2.0",
				AnnotationReportedFreeCPU:        "6.0",
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
			},
		},
	}
	info1 := framework.NewNodeInfo()
	info1.SetNode(node1)

	// 2. Node 2: Ineligible - Cost exceeds ceiling ($10.00 > $1.00)
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-2-expensive",
			Annotations: map[string]string{
				AnnotationArch:          "amd64",
				AnnotationHourlyCostUSD: "10.00",
			},
		},
	}
	info2 := framework.NewNodeInfo()
	info2.SetNode(node2)

	// 3. Node 3: Ineligible - Architecture mismatch (arm64 vs amd64)
	node3 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-3-arm64",
			Annotations: map[string]string{
				AnnotationArch:          "arm64",
				AnnotationHourlyCostUSD: "0.50",
			},
		},
	}
	info3 := framework.NewNodeInfo()
	info3.SetNode(node3)

	// 4. Node 4: Ineligible - Corrupted telemetry (alloc 8, used 2, free 20)
	node4 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-4-corrupt-telemetry",
			Annotations: map[string]string{
				AnnotationArch:                   "amd64",
				AnnotationHourlyCostUSD:          "0.50",
				AnnotationReportedAllocatableCPU: "8.0",
				AnnotationReportedUsedCPU:        "2.0",
				AnnotationReportedFreeCPU:        "20.0", // 8 - 2 != 20
			},
		},
	}
	info4 := framework.NewNodeInfo()
	info4.SetNode(node4)

	// 5. Node 5: Ineligible - Latency Critical workload on Spot instance
	node5 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-5-spot",
			Annotations: map[string]string{
				AnnotationArch:          "amd64",
				AnnotationHourlyCostUSD: "0.20",
				AnnotationIsSpot:        "true",
			},
		},
	}
	info5 := framework.NewNodeInfo()
	info5.SetNode(node5)

	mock := &mockClient{}
	plugin := &SentinelPlugin{
		client:             mock,
		gamma:              1.0,
		reservedPlacements: make(map[string]placementEntry),
	}

	ctx := context.Background()
	state := framework.NewCycleState()

	// STEP 1: PreFilter
	preFilterRes, preFilterStatus := plugin.PreFilter(ctx, state, pod)
	if preFilterStatus != nil && !preFilterStatus.IsSuccess() {
		t.Fatalf("PreFilter failed: %v", preFilterStatus)
	}
	if preFilterRes != nil {
		t.Errorf("Expected nil PreFilterResult")
	}

	// STEP 2: Filter (runs BEFORE PreScore, matching real K8s lifecycle)
	// Node 1 must PASS
	st1 := plugin.Filter(ctx, state, pod, info1)
	if st1 != nil && !st1.IsSuccess() {
		t.Errorf("Expected Node 1 to PASS Filter, got: %v", st1)
	}

	// Node 2 must FAIL (cost ceiling)
	st2 := plugin.Filter(ctx, state, pod, info2)
	if st2 == nil || st2.IsSuccess() {
		t.Errorf("Expected Node 2 to FAIL Filter due to cost ceiling, but it passed")
	}

	// Node 3 must FAIL (architecture mismatch)
	st3 := plugin.Filter(ctx, state, pod, info3)
	if st3 == nil || st3.IsSuccess() {
		t.Errorf("Expected Node 3 to FAIL Filter due to arch mismatch, but it passed")
	}

	// Node 4 must FAIL (corrupt arithmetic)
	st4 := plugin.Filter(ctx, state, pod, info4)
	if st4 == nil || st4.IsSuccess() {
		t.Errorf("Expected Node 4 to FAIL Filter due to corrupt telemetry, but it passed")
	}

	// Node 5 must FAIL (latency critical on spot)
	st5 := plugin.Filter(ctx, state, pod, info5)
	if st5 == nil || st5.IsSuccess() {
		t.Errorf("Expected Node 5 to FAIL Filter due to Spot policy, but it passed")
	}

	// STEP 3: PreScore (only with the node that passed Filter)
	preScoreStatus := plugin.PreScore(ctx, state, pod, []*v1.Node{node1})
	if preScoreStatus != nil && !preScoreStatus.IsSuccess() {
		t.Fatalf("PreScore failed: %v", preScoreStatus)
	}
	if !mock.scoreNodesCalled {
		t.Errorf("Expected PreScore to call ScoreNodes")
	}

	// STEP 4: Score
	score, scoreStatus := plugin.Score(ctx, state, pod, "node-1-eligible")
	if scoreStatus != nil && !scoreStatus.IsSuccess() {
		t.Errorf("Score failed: %v", scoreStatus)
	}
	if score != 85 {
		t.Errorf("Expected score 85, got %d", score)
	}

	// STEP 5: Reserve and Commit
	reserveStatus := plugin.Reserve(ctx, state, pod, "node-1-eligible")
	if reserveStatus != nil && !reserveStatus.IsSuccess() {
		t.Errorf("Reserve failed: %v", reserveStatus)
	}

	plugin.PostBind(ctx, state, pod, "node-1-eligible")
	if !mock.commitCalled || !mock.lastSuccess {
		t.Errorf("Expected PostBind to commit placement")
	}

	plugin.Unreserve(ctx, state, pod, "node-1-eligible")
	if mock.lastSuccess {
		t.Errorf("Expected Unreserve to rollback placement")
	}
}

func TestScoreDoesNotAbortOnMissingCandidate(t *testing.T) {
	plugin := &SentinelPlugin{
		gamma:              1.0,
		reservedPlacements: make(map[string]placementEntry),
	}
	ctx := context.Background()
	state := framework.NewCycleState()
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", UID: "uid-1"}}

	// Calling Score on an unlisted candidate must return (0, Success), NOT an error/Unschedulable
	score, status := plugin.Score(ctx, state, pod, "non-existent-node")
	if status == nil || !status.IsSuccess() {
		t.Errorf("Score must return Success status, got: %v", status)
	}
	if score != 50 { // neutral fallback score when StateData is absent
		t.Errorf("Expected neutral score 50, got %d", score)
	}
}

func TestFilterEnforcesConstraintsDuringDaemonOutage(t *testing.T) {
	// Plugin with nil client (simulating daemon down)
	plugin := &SentinelPlugin{
		client:             nil,
		circuitBreaker:     nil,
		gamma:              1.0,
		reservedPlacements: make(map[string]placementEntry),
	}
	ctx := context.Background()
	state := framework.NewCycleState()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-failover",
			UID:  "uid-failover",
			Annotations: map[string]string{
				AnnotationArch:           "amd64",
				AnnotationCostCeilingUSD: "1.00",
			},
		},
	}

	goodNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "good-node",
			Annotations: map[string]string{
				AnnotationArch:          "amd64",
				AnnotationHourlyCostUSD: "0.50",
			},
		},
	}
	badNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bad-node",
			Annotations: map[string]string{
				AnnotationArch:          "amd64",
				AnnotationHourlyCostUSD: "10.00", // exceeds ceiling
			},
		},
	}

	infoGood := framework.NewNodeInfo()
	infoGood.SetNode(goodNode)
	infoBad := framework.NewNodeInfo()
	infoBad.SetNode(badNode)

	// Filter MUST independently reject badNode even when daemon is unavailable
	if st := plugin.Filter(ctx, state, pod, infoBad); st == nil || st.IsSuccess() {
		t.Errorf("Filter must reject badNode during daemon outage, but it passed")
	}

	if st := plugin.Filter(ctx, state, pod, infoGood); st != nil && !st.IsSuccess() {
		t.Errorf("Filter must accept goodNode during daemon outage, got: %v", st)
	}

	// PreScore activates fallback path for passed nodes
	preScoreSt := plugin.PreScore(ctx, state, pod, []*v1.Node{goodNode})
	if preScoreSt != nil && !preScoreSt.IsSuccess() {
		t.Errorf("PreScore fallback failed: %v", preScoreSt)
	}

	score, scoreSt := plugin.Score(ctx, state, pod, "good-node")
	if scoreSt != nil && !scoreSt.IsSuccess() {
		t.Errorf("Score failed in fallback: %v", scoreSt)
	}
	if score != 50 {
		t.Errorf("Expected fallback score 50, got %d", score)
	}
}

func TestBoundedReservationCache(t *testing.T) {
	plugin := &SentinelPlugin{
		gamma:              1.0,
		reservedPlacements: make(map[string]placementEntry),
	}
	ctx := context.Background()
	state := framework.NewCycleState()

	// Inject 5,100 placements to trigger eviction bound
	for i := 0; i < 5100; i++ {
		pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(fmt.Sprintf("pod-%d", i))}}
		plugin.Reserve(ctx, state, pod, "node-test")
	}

	plugin.mu.RLock()
	count := len(plugin.reservedPlacements)
	plugin.mu.RUnlock()

	if count > 5000 {
		t.Errorf("reservedPlacements exceeded strict bound: %d > 5000", count)
	}
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 1) // 3 failures threshold, 1s timeout
	if cb.GetState() != Closed {
		t.Errorf("Expected initially Closed state")
	}

	bo0 := cb.calculateBackoff(0)
	if bo0 <= 0 {
		t.Errorf("Expected non-zero backoff for attempt 0, got %v", bo0)
	}

	cb.recordFailure()
	cb.recordFailure()
	if cb.GetState() != Closed {
		t.Errorf("Expected Closed after 2 failures")
	}

	cb.recordFailure()
	if cb.GetState() != Open {
		t.Errorf("Expected Open after 3 failures")
	}

	cb.mu.Lock()
	cb.lastFailure = time.Now().Add(-2 * time.Second)
	cb.mu.Unlock()

	// In Open state after timeout elapsed, AttemptCall transitions to HalfOpen
	probeFailed := errors.New("probe network timeout")
	err := cb.AttemptCall(context.Background(), func() error {
		// Inside AttemptCall, state should have transitioned to HalfOpen
		if cb.GetState() != HalfOpen {
			t.Errorf("Expected HalfOpen during probe attempt, got %v", cb.GetState())
		}
		return probeFailed
	})
	if err == nil {
		t.Errorf("Expected probe call to return error")
	}

	// In HalfOpen, a failure immediately reopens
	if cb.GetState() != Open {
		t.Errorf("Expected Open immediately after failure in HalfOpen, got %v", cb.GetState())
	}
}

// TestNativeFilterMetadataParity explicitly verifies Blockers A & B test cases:
// Provider labels (GKE/EKS), unknown price/VRAM rejection, GPU model matching across provider labels.
func TestNativeFilterMetadataParity(t *testing.T) {
	plugin := &SentinelPlugin{}
	ctx := context.Background()

	// Pod 1: Latency-critical workload
	podLC := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-lc",
			Namespace: "default",
			UID:       types.UID("pod-lc-uid"),
			Labels: map[string]string{
				LabelWorkloadType: "LATENCY_CRITICAL",
			},
		},
	}

	// 1. GKE-labeled Spot node (cloud.google.com/gke-spot=true)
	nodeGKESpot := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-gke-spot",
			Labels: map[string]string{
				"cloud.google.com/gke-spot": "true",
			},
		},
	}
	infoGKESpot := framework.NewNodeInfo()
	infoGKESpot.SetNode(nodeGKESpot)

	state := framework.NewCycleState()
	plugin.PreFilter(ctx, state, podLC)
	status := plugin.Filter(ctx, state, podLC, infoGKESpot)
	if status.IsSuccess() {
		t.Fatalf("Expected GKE Spot node to be REJECTED for latency-critical workload")
	}

	// 2. EKS-labeled Spot node (eks.amazonaws.com/capacityType=SPOT)
	nodeEKSSpot := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-eks-spot",
			Labels: map[string]string{
				"eks.amazonaws.com/capacityType": "SPOT",
			},
		},
	}
	infoEKSSpot := framework.NewNodeInfo()
	infoEKSSpot.SetNode(nodeEKSSpot)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, podLC)
	status = plugin.Filter(ctx, state, podLC, infoEKSSpot)
	if status.IsSuccess() {
		t.Fatalf("Expected EKS Spot node to be REJECTED for latency-critical workload")
	}

	// 3. Provider-labeled L4 node evaluated against A100-required workload
	podA100 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-a100",
			Namespace: "default",
			UID:       types.UID("pod-a100-uid"),
			Annotations: map[string]string{
				AnnotationGPUCount: "1",
				AnnotationGPUType:  "A100",
			},
		},
	}
	nodeL4 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-l4-provider",
			Labels: map[string]string{
				"cloud.google.com/gke-gpu": "L4",
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				"nvidia.com/gpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
	}
	infoL4 := framework.NewNodeInfo()
	infoL4.SetNode(nodeL4)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, podA100)
	status = plugin.Filter(ctx, state, podA100, infoL4)
	if status.IsSuccess() {
		t.Fatalf("Expected provider-labeled L4 node to be REJECTED for A100-required workload")
	}

	// 4. Node with no price metadata evaluated against $1.00 ceiling
	podCeiling := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-ceiling",
			Namespace: "default",
			UID:       types.UID("pod-ceiling-uid"),
			Annotations: map[string]string{
				AnnotationCostCeilingUSD: "1.00",
			},
		},
	}
	nodeNoPrice := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-no-price",
		},
	}
	infoNoPrice := framework.NewNodeInfo()
	infoNoPrice.SetNode(nodeNoPrice)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, podCeiling)
	status = plugin.Filter(ctx, state, podCeiling, infoNoPrice)
	if status.IsSuccess() {
		t.Fatalf("Expected node with missing price to be REJECTED when mandatory ceiling is declared")
	}

	// 5. Node with no VRAM metadata evaluated against 80 GB VRAM requirement
	podVRAM := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-vram",
			Namespace: "default",
			UID:       types.UID("pod-vram-uid"),
			Annotations: map[string]string{
				AnnotationGPUCount:    "1",
				AnnotationGPUMemoryGB: "80.0",
			},
		},
	}
	nodeNoVRAM := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-no-vram",
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				"nvidia.com/gpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
	}
	infoNoVRAM := framework.NewNodeInfo()
	infoNoVRAM.SetNode(nodeNoVRAM)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, podVRAM)
	status = plugin.Filter(ctx, state, podVRAM, infoNoVRAM)
	if status.IsSuccess() {
		t.Fatalf("Expected node with missing VRAM metadata to be REJECTED when mandatory VRAM is declared")
	}
}

// TestNativeFilterTelemetryGates tests memory arithmetic, stale heartbeats, and future timestamp rejection.
func TestNativeFilterTelemetryGates(t *testing.T) {
	plugin := &SentinelPlugin{}
	ctx := context.Background()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "generic-pod",
			Namespace: "default",
			UID:       types.UID("gen-pod-1"),
		},
	}

	// 1. Memory telemetry claiming 1000 GB free on 16 GB allocatable
	nodeMemExceed := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-mem-exceed",
			Annotations: map[string]string{
				AnnotationReportedAllocatableMem: "16.0",
				AnnotationReportedUsedMem:        "2.0",
				AnnotationReportedFreeMem:        "1000.0", // 1000 > 16!
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceMemory: *resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
			},
		},
	}
	infoMemExceed := framework.NewNodeInfo()
	infoMemExceed.SetNode(nodeMemExceed)

	state := framework.NewCycleState()
	plugin.PreFilter(ctx, state, pod)
	status := plugin.Filter(ctx, state, pod, infoMemExceed)
	if status.IsSuccess() {
		t.Fatalf("Expected node with free memory exceeding allocatable to be REJECTED")
	}

	// 2. Memory telemetry arithmetic balance failure (16 - 4 != 25)
	nodeMemImbalance := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-mem-imbalance",
			Annotations: map[string]string{
				AnnotationReportedAllocatableMem: "16.0",
				AnnotationReportedUsedMem:        "4.0",
				AnnotationReportedFreeMem:        "25.0",
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceMemory: *resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
			},
		},
	}
	infoMemImbalance := framework.NewNodeInfo()
	infoMemImbalance.SetNode(nodeMemImbalance)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, pod)
	status = plugin.Filter(ctx, state, pod, infoMemImbalance)
	if status.IsSuccess() {
		t.Fatalf("Expected node with memory arithmetic imbalance to be REJECTED")
	}

	// 3. Heartbeat one hour stale
	staleTime := float64(time.Now().Add(-1 * time.Hour).Unix())
	nodeStaleHB := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-stale-hb",
			Annotations: map[string]string{
				AnnotationLastHeartbeat: fmt.Sprintf("%.0f", staleTime),
			},
		},
	}
	infoStaleHB := framework.NewNodeInfo()
	infoStaleHB.SetNode(nodeStaleHB)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, pod)
	status = plugin.Filter(ctx, state, pod, infoStaleHB)
	if status.IsSuccess() {
		t.Fatalf("Expected node with 1-hour stale heartbeat to be REJECTED")
	}

	// 4. Heartbeat in the future (clock anomaly > 1.0s)
	futureTime := float64(time.Now().Add(10 * time.Minute).Unix())
	nodeFutureHB := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-future-hb",
			Annotations: map[string]string{
				AnnotationLastHeartbeat: fmt.Sprintf("%.0f", futureTime),
			},
		},
	}
	infoFutureHB := framework.NewNodeInfo()
	infoFutureHB.SetNode(nodeFutureHB)

	state = framework.NewCycleState()
	plugin.PreFilter(ctx, state, pod)
	status = plugin.Filter(ctx, state, pod, infoFutureHB)
	if status.IsSuccess() {
		t.Fatalf("Expected node with future heartbeat timestamp to be REJECTED")
	}
}
