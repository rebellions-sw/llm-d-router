/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package roundrobin

import (
	"context"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func scored(name, address, port string, score float64) *fwksched.ScoredEndpoint {
	return &fwksched.ScoredEndpoint{
		Endpoint: fwksched.NewEndpoint(&fwkdl.EndpointMetadata{
			ID:      k8stypes.NamespacedName{Name: name},
			Address: address,
			Port:    port,
		}, nil, nil),
		Score: score,
	}
}

func pickedName(t *testing.T, result *fwksched.ProfileRunResult) string {
	t.Helper()
	if len(result.TargetEndpoints) != 1 {
		t.Fatalf("expected exactly 1 endpoint, got %d", len(result.TargetEndpoints))
	}
	return result.TargetEndpoints[0].GetMetadata().ID.Name
}

// Consecutive calls must visit every candidate once before repeating — that is the
// whole point of this picker over random selection.
func TestPick_VisitsEveryCandidateBeforeRepeating(t *testing.T) {
	candidates := []*fwksched.ScoredEndpoint{
		scored("pod1", "10.0.0.1", "8000", 0),
		scored("pod2", "10.0.0.2", "8000", 0),
		scored("pod3", "10.0.0.3", "8000", 0),
	}
	picker := NewRoundRobinPicker(1)

	seen := map[string]int{}
	for i := 0; i < len(candidates); i++ {
		seen[pickedName(t, picker.Pick(context.Background(), candidates))]++
	}
	if len(seen) != len(candidates) {
		t.Fatalf("expected %d distinct endpoints in one rotation, got %d (%v)", len(candidates), len(seen), seen)
	}

	// A second rotation repeats the same order.
	first := pickedName(t, picker.Pick(context.Background(), candidates))
	if want := "pod1"; first != want {
		t.Fatalf("rotation did not wrap to the first endpoint: got %q, want %q", first, want)
	}
}

// The framework hands candidates over in an unstable order, so the ring is rebuilt from
// endpointKey. Shuffling the input must not change which endpoint the cursor lands on.
func TestPick_OrderOfCandidatesDoesNotChangeSelection(t *testing.T) {
	a := scored("pod1", "10.0.0.1", "8000", 0)
	b := scored("pod2", "10.0.0.2", "8000", 0)
	c := scored("pod3", "10.0.0.3", "8000", 0)

	forward := NewRoundRobinPicker(1)
	reversed := NewRoundRobinPicker(1)

	for i := 0; i < 6; i++ {
		got := pickedName(t, forward.Pick(context.Background(), []*fwksched.ScoredEndpoint{a, b, c}))
		want := pickedName(t, reversed.Pick(context.Background(), []*fwksched.ScoredEndpoint{c, b, a}))
		if got != want {
			t.Fatalf("call %d: input order changed the selection: %q vs %q", i, got, want)
		}
	}
}

// Scores are ignored on purpose — that is what separates this picker from max-score.
func TestPick_IgnoresScores(t *testing.T) {
	candidates := []*fwksched.ScoredEndpoint{
		scored("pod1", "10.0.0.1", "8000", 0),
		scored("pod2", "10.0.0.2", "8000", 100),
	}
	picker := NewRoundRobinPicker(1)

	if got, want := pickedName(t, picker.Pick(context.Background(), candidates)), "pod1"; got != want {
		t.Fatalf("the lowest-scored endpoint must still come first: got %q, want %q", got, want)
	}
}

// The cursor lives on the instance. A PD deployment declares one instance per scheduling
// profile; sharing one would advance the cursor twice per request and visit only half the
// endpoints.
func TestPick_CursorIsPerInstance(t *testing.T) {
	candidates := []*fwksched.ScoredEndpoint{
		scored("pod1", "10.0.0.1", "8000", 0),
		scored("pod2", "10.0.0.2", "8000", 0),
	}
	prefill := NewRoundRobinPicker(1).WithName("rr-prefill")
	decode := NewRoundRobinPicker(1).WithName("rr-decode")

	if got, want := pickedName(t, prefill.Pick(context.Background(), candidates)), "pod1"; got != want {
		t.Fatalf("prefill first pick: got %q, want %q", got, want)
	}
	if got, want := pickedName(t, decode.Pick(context.Background(), candidates)), "pod1"; got != want {
		t.Fatalf("decode must start its own rotation: got %q, want %q", got, want)
	}
}

// Port comes first in the ring key: with pipeline parallelism a pod's N ports are one
// engine, so an address-major ring would send N consecutive requests to the same engine.
func TestPick_RingIsPortMajor(t *testing.T) {
	candidates := []*fwksched.ScoredEndpoint{
		scored("podA-8000", "10.0.0.1", "8000", 0),
		scored("podA-8001", "10.0.0.1", "8001", 0),
		scored("podB-8000", "10.0.0.2", "8000", 0),
		scored("podB-8001", "10.0.0.2", "8001", 0),
	}
	picker := NewRoundRobinPicker(1)

	got := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		got = append(got, pickedName(t, picker.Pick(context.Background(), candidates)))
	}
	want := []string{"podA-8000", "podB-8000", "podA-8001", "podB-8001"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ring is not port-major: got %v, want %v", got, want)
		}
	}
}

func TestPick_NoCandidates(t *testing.T) {
	result := NewRoundRobinPicker(1).Pick(context.Background(), nil)
	if len(result.TargetEndpoints) != 0 {
		t.Fatalf("expected no endpoints, got %d", len(result.TargetEndpoints))
	}
}

// Endpoints without metadata have no stable identity. The ring must still rotate
// deterministically over them rather than reshuffling on every call, which is what an
// address-derived key would do once the framework clones endpoints per cycle.
func TestPick_RotatesDeterministicallyWithoutMetadata(t *testing.T) {
	fresh := func() []*fwksched.ScoredEndpoint {
		return []*fwksched.ScoredEndpoint{
			{Endpoint: fwksched.NewEndpoint(nil, nil, nil), Score: 0},
			{Endpoint: fwksched.NewEndpoint(nil, nil, nil), Score: 0},
			{Endpoint: fwksched.NewEndpoint(nil, nil, nil), Score: 0},
		}
	}
	for _, ep := range fresh() {
		if got := endpointKey(ep); got != "" {
			t.Fatalf("endpointKey must not derive a key from an address: got %q", got)
		}
	}

	picker := NewRoundRobinPicker(1)
	for i := 0; i < 6; i++ {
		candidates := fresh() // the framework hands over freshly cloned endpoints each cycle
		got := picker.Pick(context.Background(), candidates)
		if len(got.TargetEndpoints) != 1 {
			t.Fatalf("call %d: expected 1 endpoint, got %d", i, len(got.TargetEndpoints))
		}
		// Pick puts the *ScoredEndpoint itself into TargetEndpoints, not the inner Endpoint.
		if want := candidates[i%len(candidates)]; got.TargetEndpoints[0] != fwksched.Endpoint(want) {
			t.Fatalf("call %d: cursor did not advance over the incoming order", i)
		}
	}
}
