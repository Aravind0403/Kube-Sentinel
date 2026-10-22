package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"aco-sentinel-plugin/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// This tests the real Go plugin + gRPC connection + actual Python daemon, not a breaker simulator.
// It intentionally does not claim to bind Kubernetes Pods.
func TestAcceptanceNativeDaemonFailover(t *testing.T) {
	python := os.Getenv("SENTINEL_ACCEPTANCE_PYTHON")
	if python == "" {
		t.Skip("set SENTINEL_ACCEPTANCE_PYTHON to a Python environment with repository dependencies")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	address := fmt.Sprintf("127.0.0.1:%d", port)
	var proc *exec.Cmd
	var output bytes.Buffer
	stop := func() {
		if proc != nil && proc.Process != nil {
			_ = proc.Process.Kill()
			_ = proc.Wait()
			proc = nil
		}
	}
	defer stop()
	start := func() {
		proc = exec.Command(python, "v2/grpc_server.py", fmt.Sprintf("--port=%d", port))
		proc.Dir = root
		proc.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
		proc.Stdout = &output
		proc.Stderr = &output
		if err := proc.Start(); err != nil {
			t.Fatal(err)
		}
		ready := false
		for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
			c, e := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if e == nil {
				c.Close()
				ready = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !ready {
			stop()
			t.Fatalf("daemon startup failed: %s", output.String())
		}
	}
	start()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	plugin := &SentinelPlugin{client: pb.NewACOPredictiveSchedulerClient(conn), circuitBreaker: NewCircuitBreaker(2, 1), gamma: 2, reservedPlacements: map[string]placementEntry{}}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "protected", Labels: map[string]string{LabelWorkloadType: "latency-critical"}}, Spec: v1.PodSpec{Containers: []v1.Container{{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	good := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "good", Labels: map[string]string{"kubernetes.io/arch": "amd64"}, Annotations: map[string]string{AnnotationHourlyCostUSD: "0.50", AnnotationLastHeartbeat: "0"}}, Status: v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourceCPU: resource.MustParse("8"), v1.ResourceMemory: resource.MustParse("32Gi")}}}
	spot := good.DeepCopy()
	spot.Name = "spot"
	spot.Labels["cloud.google.com/gke-spot"] = "true"
	cycle := func() (int64, *framework.Status) {
		good.Annotations[AnnotationLastHeartbeat] = fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1e9)
		spot.Annotations[AnnotationLastHeartbeat] = good.Annotations[AnnotationLastHeartbeat]
		state := framework.NewCycleState()
		plugin.PreFilter(context.Background(), state, pod)
		ni := framework.NewNodeInfo()
		ni.SetNode(good)
		if status := plugin.Filter(context.Background(), state, pod, ni); !status.IsSuccess() {
			t.Fatalf("good node rejected: %s", status.Message())
		}
		niSpot := framework.NewNodeInfo()
		niSpot.SetNode(spot)
		if status := plugin.Filter(context.Background(), state, pod, niSpot); status.IsSuccess() {
			t.Fatal("protected Pod accepted Spot during failover")
		}
		if status := plugin.PreScore(context.Background(), state, pod, []*v1.Node{good}); !status.IsSuccess() {
			t.Fatalf("PreScore failed: %v", status)
		}
		return plugin.Score(context.Background(), state, pod, good.Name)
	}
	score, status := cycle()
	if !status.IsSuccess() || score == 50 || plugin.circuitBreaker.GetState() != Closed {
		t.Fatalf("normal scoring failed: score=%d state=%d", score, plugin.circuitBreaker.GetState())
	}
	t.Logf("real daemon normal score=%d, breaker=CLOSED", score)
	stop()
	for i := 0; i < 2; i++ {
		score, status = cycle()
		if !status.IsSuccess() || score != 50 {
			t.Fatalf("outage fallback failed: score=%d", score)
		}
	}
	if plugin.circuitBreaker.GetState() != Open {
		t.Fatal("actual Go breaker did not open")
	}
	t.Log("actual Go breaker=OPEN; neutral fallback=50; provider Spot exclusion preserved")
	// Observe a real recovery probe without changing or replacing the plugin connection.
	probeObserved := false
	wrapped := plugin.client
	plugin.client = &acceptanceProbeObserver{ACOPredictiveSchedulerClient: wrapped, onScore: func() {
		if plugin.circuitBreaker.GetState() == HalfOpen {
			probeObserved = true
		}
	}}
	start()
	recovered := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		time.Sleep(150 * time.Millisecond)
		score, status = cycle()
		if status.IsSuccess() && plugin.circuitBreaker.GetState() == Closed && score != 50 {
			recovered = true
			break
		}
	}
	if !recovered || !probeObserved {
		t.Fatalf("native recovery failed: state=%d observedHalfOpen=%v", plugin.circuitBreaker.GetState(), probeObserved)
	}
	t.Logf("same Go connection recovered through HALF_OPEN -> CLOSED; normal score=%d", score)
}

type acceptanceProbeObserver struct {
	pb.ACOPredictiveSchedulerClient
	onScore func()
}

func (o *acceptanceProbeObserver) ScoreNodes(ctx context.Context, req *pb.ScoreRequest, opts ...grpc.CallOption) (*pb.ScoreResponse, error) {
	o.onScore()
	return o.ACOPredictiveSchedulerClient.ScoreNodes(ctx, req, opts...)
}
