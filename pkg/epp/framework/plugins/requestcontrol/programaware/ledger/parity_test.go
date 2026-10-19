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

package ledger

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Traces recorded from ThunderAgent (llm-d-porting fork, 7d0e88e): every operation
// and the TA ledger state after it. The Go ledger must reach the same state.

type traceStep struct {
	Op       string   `json:"op"`
	ID       string   `json:"id"`
	Tokens   int      `json:"tokens"`
	Delta    int      `json:"delta"`
	Total    int      `json:"total"`
	Snapshot Snapshot `json:"snapshot"`
}

type trace struct {
	Seed         int `json:"seed"`
	Headroom     int `json:"headroom"`
	ColdPrefills int `json:"cold_prefills"`
	Backends     []struct {
		Name     string `json:"name"`
		Capacity int    `json:"capacity"`
	} `json:"backends"`
	Steps []traceStep `json:"steps"`
}

func loadTrace(t *testing.T, path string) trace {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	gz, err := gzip.NewReader(f)
	require.NoError(t, err)

	var tr trace
	require.NoError(t, json.NewDecoder(gz).Decode(&tr))
	return tr
}

func TestParityWithThunderAgent(t *testing.T) {
	paths, err := filepath.Glob("testdata/parity/*.json.gz")
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			replay(t, loadTrace(t, path))
		})
	}
}

func replay(t *testing.T, tr trace) {
	// TA's cold-prefill clock is wall time over a sub-second run; one instant is equivalent.
	now := time.Unix(0, 0)
	l := New(Config{HeadroomTokens: tr.Headroom, ColdPrefills: tr.ColdPrefills})
	for _, b := range tr.Backends {
		l.SetBackend(Backend{Name: b.Name, Capacity: b.Capacity, Healthy: true})
	}

	for i, step := range tr.Steps {
		switch step.Op {
		case "arrive":
			l.Arrive(step.ID, step.Tokens, now)
		case "first_token":
			l.FirstToken(step.ID)
		case "progress":
			l.Progress(step.ID, step.Delta)
		case "finish":
			l.Finish(step.ID, step.Total)
		case "release":
			l.Release(step.ID)
		case "tick":
			l.Tick(now)
		default:
			t.Fatalf("step %d: unknown op %q", i, step.Op)
		}

		require.Equal(t, step.Snapshot, l.Snapshot(now), "seed %d step %d (%s %s)", tr.Seed, i, step.Op, step.ID)
	}
}
