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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Scenario tests ported from ThunderAgent tests/ (rank-aware placement,
// pause accounting, headroom), plus the paths the parity traces do not
// reach: forced resume, release of a held request, unhealthy and unsized
// backends, stale first-round marks.

const (
	rankCapacity  = 192_512
	sessionTokens = 40_000
	headroom      = 16_384
)

var (
	ranks = []string{"rank0", "rank1", "rank2", "rank3"}
	epoch = time.Unix(0, 0)
)

func newLedger(coldPrefills int) *Ledger {
	l := New(Config{HeadroomTokens: headroom, ColdPrefills: coldPrefills, ResumeTimeout: time.Hour})
	for _, name := range ranks {
		l.SetBackend(Backend{Name: name, Capacity: rankCapacity, Healthy: true})
	}
	return l
}

// seat puts a program mid-session on a backend, as TA tests' active_program does.
func seat(l *Ledger, id, backend string, tokens, steps int, status Status) *program {
	l.seq++
	p := &program{id: id, seq: l.seq, backend: backend, status: status, state: Active, tokens: tokens, steps: steps}
	l.programs[id] = p
	l.backends[backend].programs[id] = p
	return p
}

func TestNewProgramGoesToLeastResidentContext(t *testing.T) {
	l := newLedger(0)
	seat(l, "a", "rank0", sessionTokens, 5, Reasoning)
	seat(l, "b", "rank0", sessionTokens, 5, Reasoning)
	seat(l, "c", "rank1", sessionTokens, 5, Reasoning)
	seat(l, "d", "rank3", sessionTokens, 5, Reasoning)

	require.Equal(t, Admission{Backend: "rank2"}, l.Arrive("new", sessionTokens, epoch))
}

func TestResumeReturnsToOriginWhenItFits(t *testing.T) {
	l := newLedger(0)
	p := seat(l, "p", "rank3", sessionTokens, 5, Acting)
	l.pause(p)
	seat(l, "x", "rank0", sessionTokens, 5, Reasoning)

	l.Tick(epoch)

	require.Equal(t, Active, p.state)
	require.Equal(t, "rank3", p.backend)
}

func TestResumeWaitsRatherThanMigrate(t *testing.T) {
	l := newLedger(0)
	p := seat(l, "p", "rank3", sessionTokens, 5, Acting)
	l.pause(p)
	for _, id := range []string{"a", "b", "c"} { // 3×(40k+16k) leaves less than 56k on rank3
		seat(l, id, "rank3", sessionTokens, 5, Reasoning)
	}

	l.Tick(epoch)

	require.Equal(t, Paused, p.state) // rank0..2 are empty, still no move
}

func TestPausedAfterFirstRoundIsNotNew(t *testing.T) {
	// Marked in round 1 and paused when it ended: step 1, but its KV is on origin.
	l := newLedger(0)
	p := seat(l, "p", "rank3", sessionTokens, 1, Acting)
	l.pause(p)
	for _, id := range []string{"a", "b", "c"} {
		seat(l, id, "rank3", sessionTokens, 5, Reasoning)
	}

	l.Tick(epoch)

	require.Equal(t, Paused, p.state)
}

func TestSecondColdPrefillOnABackendWaits(t *testing.T) {
	l := newLedger(1)
	for _, name := range ranks {
		require.Equal(t, Admission{Backend: name}, l.Arrive("first-"+name, sessionTokens, epoch))
	}

	waiting := l.Arrive("late", sessionTokens, epoch)
	require.NotNil(t, waiting.Ready)

	l.FirstToken("first-rank2")
	l.Tick(epoch)

	backend, ok := l.Placement("late")
	require.True(t, ok)
	require.Equal(t, "rank2", backend)
	require.Equal(t, 1, l.Snapshot(epoch).Backends["rank2"].ColdPrefills)
}

func TestStaleColdPrefillIsForgotten(t *testing.T) {
	l := newLedger(1)
	l.Arrive("stuck", sessionTokens, epoch)

	require.Equal(t, 1, l.Snapshot(epoch).Backends["rank0"].ColdPrefills)
	require.Equal(t, 0, l.Snapshot(epoch.Add(coldPrefillMax)).Backends["rank0"].ColdPrefills)
}

// overfullRank puts four rounds on rank0, the last grown so the rank is ~1k over.
func overfullRank(l *Ledger) *backendState {
	const roundTokens, grownTokens = 30_000, 38_000
	for _, id := range []string{"a", "b", "c", "d"} {
		seat(l, id, "rank0", roundTokens, 5, Reasoning)
	}
	l.programs["d"].tokens = grownTokens
	return l.backends["rank0"]
}

func markedIDs(l *Ledger) []string {
	var ids []string
	for _, id := range []string{"a", "b", "c", "d"} {
		if l.programs[id].marked {
			ids = append(ids, id)
		}
	}
	return ids
}

func TestSmallOverflowMarksOnlyTheSmallest(t *testing.T) {
	l := New(Config{HeadroomTokens: headroom})
	l.SetBackend(Backend{Name: "rank0", Capacity: rankCapacity, Healthy: true})
	b := overfullRank(l)
	require.Negative(t, b.remaining(headroom))

	l.Tick(epoch)
	require.Equal(t, []string{"a"}, markedIDs(l))

	l.Tick(epoch) // a's round has not ended: no second mark
	require.Equal(t, []string{"a"}, markedIDs(l))

	for _, id := range []string{"a", "b", "c", "d"} {
		l.Finish(id, l.programs[id].tokens)
	}
	require.Equal(t, Paused, l.programs["a"].state)
	require.Len(t, b.programs, 3)
}

func TestRemainingSubtractsHeadroomPerProgram(t *testing.T) {
	l := newLedger(0)
	seat(l, "a", "rank0", sessionTokens, 2, Acting)
	seat(l, "b", "rank0", sessionTokens, 2, Acting)

	require.Equal(t, rankCapacity-2*(sessionTokens+headroom), l.backends["rank0"].remaining(headroom))
}

func TestExpireForcesHeldNewProgramToFewestPrograms(t *testing.T) {
	l := newLedger(0)
	for i, name := range ranks {
		for j := 0; j <= i; j++ {
			seat(l, name+string(rune('a'+j)), name, rankCapacity/(i+1), 5, Reasoning)
		}
	}
	held := l.Arrive("late", sessionTokens, epoch)
	require.NotNil(t, held.Ready)

	l.Expire("late", epoch)

	<-held.Ready
	backend, ok := l.Placement("late")
	require.True(t, ok)
	require.Equal(t, "rank0", backend)
}

func TestExpireForcesHeldRoundToOrigin(t *testing.T) {
	l := newLedger(0)
	p := seat(l, "p", "rank3", sessionTokens, 5, Acting)
	l.pause(p)
	held := l.Arrive("p", sessionTokens, epoch)

	l.Expire("p", epoch)

	<-held.Ready
	backend, _ := l.Placement("p")
	require.Equal(t, "rank3", backend)
}

func TestReleaseWakesHeldRequestWithoutPlacement(t *testing.T) {
	l := newLedger(1)
	for _, name := range ranks {
		l.Arrive("first-"+name, sessionTokens, epoch)
	}
	held := l.Arrive("late", sessionTokens, epoch)

	require.True(t, l.Release("late"))

	<-held.Ready
	_, ok := l.Placement("late")
	require.False(t, ok)
	require.Empty(t, l.Snapshot(epoch).Queue)
}

func TestUnhealthyBackendIsNeverChosen(t *testing.T) {
	l := newLedger(0)
	for _, name := range ranks[:3] {
		l.SetBackend(Backend{Name: name, Capacity: rankCapacity, Healthy: false})
	}

	require.Equal(t, Admission{Backend: "rank3"}, l.Arrive("new", sessionTokens, epoch))
}

func TestUnsizedBackendNeverLimits(t *testing.T) {
	l := New(Config{HeadroomTokens: headroom})
	l.SetBackend(Backend{Name: "rank0", Healthy: true})
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		require.Equal(t, Admission{Backend: "rank0"}, l.Arrive(id, rankCapacity, epoch))
	}

	l.Tick(epoch)
	require.Empty(t, l.Snapshot(epoch).Queue)
}
