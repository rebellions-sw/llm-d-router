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

// Package roundrobin implements a scheduling picker that rotates over the candidate
// endpoints in a fixed order, ignoring both the scores computed by scorer plugins and
// any load/running-status signal.
package roundrobin

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker"
)

const (
	// RoundRobinPickerType is the registered name of the round robin picker plugin.
	RoundRobinPickerType = "round-robin-picker"
)

// compile-time type validation
var _ fwksched.Picker = &RoundRobinPicker{}

// RoundRobinPickerFactory defines the factory function for RoundRobinPicker.
func RoundRobinPickerFactory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	parameters := picker.PickerParameters{MaxNumOfEndpoints: picker.DefaultMaxNumOfEndpoints}
	if rawParameters != nil {
		if err := rawParameters.Decode(&parameters); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' picker - %w", RoundRobinPickerType, err)
		}
	}

	return NewRoundRobinPicker(parameters.MaxNumOfEndpoints).WithName(name), nil
}

// NewRoundRobinPicker initializes a new RoundRobinPicker and returns its pointer.
func NewRoundRobinPicker(maxNumOfEndpoints int) *RoundRobinPicker {
	if maxNumOfEndpoints <= 0 {
		maxNumOfEndpoints = picker.DefaultMaxNumOfEndpoints // on invalid configuration value, fallback to default value
	}

	return &RoundRobinPicker{
		typedName:         fwkplugin.TypedName{Type: RoundRobinPickerType, Name: RoundRobinPickerType},
		maxNumOfEndpoints: maxNumOfEndpoints,
	}
}

// RoundRobinPicker picks endpoint(s) by rotating a cursor over the candidates.
//
// Each configured instance owns its own cursor, so a PD deployment must declare one
// instance per scheduling profile — a shared instance advances once per profile and
// would visit only every other endpoint.
type RoundRobinPicker struct {
	typedName         fwkplugin.TypedName
	maxNumOfEndpoints int // maximum number of endpoints to pick
	cursor            atomic.Uint64
}

// WithName sets the picker's name
func (p *RoundRobinPicker) WithName(name string) *RoundRobinPicker {
	p.typedName.Name = name
	return p
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *RoundRobinPicker) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// endpointKey orders the rotation ring. Port comes first on purpose: with pipeline
// parallelism a pod's N ports are one engine, so an address-major ring would send N
// consecutive requests to the same engine instead of alternating pods.
func endpointKey(scoredEndpoint *fwksched.ScoredEndpoint) string {
	metadata := scoredEndpoint.GetMetadata()
	if metadata == nil {
		// No stable identity to sort on. Returning the same key for every such endpoint
		// makes them compare equal, so SortStableFunc keeps their incoming order instead
		// of reordering them. Endpoint.String() must not be used here: it renders the
		// struct with %+v, which includes pointer addresses, and the framework clones
		// endpoints per scheduling cycle -- the ring would be rebuilt differently on
		// every call and the rotation guarantee would be lost without any signal.
		return ""
	}

	return metadata.Port + "|" + metadata.Address
}

// Pick selects endpoint(s) in round robin order, ignoring the scores.
func (p *RoundRobinPicker) Pick(ctx context.Context, scoredEndpoints []*fwksched.ScoredEndpoint) *fwksched.ProfileRunResult {
	numOfCandidates := len(scoredEndpoints)

	log.FromContext(ctx).V(logutil.DEBUG).Info("Selecting endpoints in round robin order", "max-num-of-endpoints", p.maxNumOfEndpoints,
		"num-of-candidates", numOfCandidates, "scored-endpoints", scoredEndpoints)

	if numOfCandidates == 0 {
		return &fwksched.ProfileRunResult{TargetEndpoints: []fwksched.Endpoint{}}
	}

	// The candidate order the framework hands over is not stable across calls, so the
	// ring has to be rebuilt deterministically before the cursor means anything.
	slices.SortStableFunc(scoredEndpoints, func(i, j *fwksched.ScoredEndpoint) int {
		return strings.Compare(endpointKey(i), endpointKey(j))
	})

	start := int((p.cursor.Add(1) - 1) % uint64(numOfCandidates))

	numToPick := p.maxNumOfEndpoints
	if numToPick > numOfCandidates {
		numToPick = numOfCandidates
	}

	targetEndpoints := make([]fwksched.Endpoint, numToPick)
	for i := 0; i < numToPick; i++ {
		targetEndpoints[i] = scoredEndpoints[(start+i)%numOfCandidates]
	}

	log.FromContext(ctx).V(logutil.DEBUG).Info("Round robin selection", "cursor", start, "selected", targetEndpoints)

	return &fwksched.ProfileRunResult{TargetEndpoints: targetEndpoints}
}
