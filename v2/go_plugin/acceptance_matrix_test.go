package main

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"testing"
	"time"
)

// Fixed acceptance matrix for present/absent/valid/invalid CPU, memory and heartbeat observations.
func TestAcceptanceObservationMatrix(t *testing.T) {
	type observationCase struct {
		name        string
		annotations map[string]string
		allowed     bool
	}
	cases := []observationCase{{"absent_optional", nil, true}}
	dimensions := []struct{ name, a, u, f string }{{"cpu", AnnotationReportedAllocatableCPU, AnnotationReportedUsedCPU, AnnotationReportedFreeCPU}, {"memory", AnnotationReportedAllocatableMem, AnnotationReportedUsedMem, AnnotationReportedFreeMem}}
	for _, d := range dimensions {
		cases = append(cases, observationCase{d.name + "_valid", map[string]string{d.a: "16", d.u: "4", d.f: "12"}, true})
		for _, bad := range []string{"NaN", "+Inf", "-Inf", "-1", "", "nonsense"} {
			cases = append(cases, observationCase{d.name + "_invalid_free_" + bad, map[string]string{d.a: "16", d.u: "4", d.f: bad}, false})
		}
		for _, which := range []string{"alloc", "used", "free"} {
			data := map[string]string{}
			key := d.a
			if which == "used" {
				key = d.u
			}
			if which == "free" {
				key = d.f
			}
			data[key] = "8"
			cases = append(cases, observationCase{d.name + "_partial_" + which, data, false})
		}
		cases = append(cases,
			observationCase{d.name + "_zero_alloc", map[string]string{d.a: "0", d.u: "0", d.f: "0"}, false},
			observationCase{d.name + "_negative_used", map[string]string{d.a: "16", d.u: "-1", d.f: "17"}, false},
			observationCase{d.name + "_unbalanced", map[string]string{d.a: "16", d.u: "4", d.f: "10"}, false},
			observationCase{d.name + "_bounds", map[string]string{d.a: "16", d.u: "0", d.f: "1000"}, false})
	}
	now := float64(time.Now().UnixNano()) / 1e9
	cases = append(cases,
		observationCase{"heartbeat_fresh", map[string]string{AnnotationLastHeartbeat: fmt.Sprintf("%.6f", now)}, true},
		observationCase{"heartbeat_stale", map[string]string{AnnotationLastHeartbeat: fmt.Sprintf("%.6f", now-3600)}, false},
		observationCase{"heartbeat_future", map[string]string{AnnotationLastHeartbeat: fmt.Sprintf("%.6f", now+3600)}, false})
	for _, bad := range []string{"NaN", "+Inf", "-Inf", "-1", "0", "", "nonsense"} {
		cases = append(cases, observationCase{"heartbeat_invalid_" + bad, map[string]string{AnnotationLastHeartbeat: bad}, false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &SentinelPlugin{}
			ctx := context.Background()
			state := framework.NewCycleState()
			pod := &v1.Pod{}
			node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: tc.name, Annotations: tc.annotations}}
			ni := framework.NewNodeInfo()
			ni.SetNode(node)
			plugin.PreFilter(ctx, state, pod)
			status := plugin.Filter(ctx, state, pod, ni)
			if status.IsSuccess() != tc.allowed {
				t.Errorf("Filter allowed=%v, expected %v; annotations=%v", status.IsSuccess(), tc.allowed, tc.annotations)
			}
		})
	}
	t.Logf("Acceptance observation cases: %d", len(cases))
}
