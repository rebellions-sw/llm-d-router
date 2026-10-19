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
	"sort"
	"time"
)

// ProgramView is one program's ledger state.
type ProgramView struct {
	Backend string `json:"backend"`
	Origin  string `json:"origin"`
	Paused  bool   `json:"paused"`
	Acting  bool   `json:"acting"`
	Marked  bool   `json:"marked"`
	Steps   int    `json:"steps"`
	Tokens  int    `json:"tokens"`
}

// BackendView is one backend's ledger totals.
type BackendView struct {
	Capacity     int `json:"capacity"`
	Programs     int `json:"programs"`
	Tokens       int `json:"tokens"`
	FuturePaused int `json:"future_paused"`
	ColdPrefills int `json:"cold_prefills"`
}

// Snapshot is the whole ledger at one instant. Queue is sorted by id.
type Snapshot struct {
	Programs map[string]ProgramView `json:"programs"`
	Backends map[string]BackendView `json:"backends"`
	Queue    []string               `json:"queue"`
}

func (l *Ledger) Snapshot(now time.Time) Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()

	s := Snapshot{
		Programs: map[string]ProgramView{},
		Backends: map[string]BackendView{},
		Queue:    []string{},
	}
	for id, p := range l.programs {
		s.Programs[id] = ProgramView{
			Backend: p.backend,
			Origin:  p.origin,
			Paused:  p.state == Paused,
			Acting:  p.status == Acting,
			Marked:  p.marked,
			Steps:   p.steps,
			Tokens:  p.tokens,
		}
	}
	for name, b := range l.backends {
		s.Backends[name] = BackendView{
			Capacity:     b.Capacity,
			Programs:     len(b.programs),
			Tokens:       b.activeTokens(),
			FuturePaused: b.futurePaused,
			ColdPrefills: b.coldInFlight(now),
		}
	}
	for id := range l.queue {
		s.Queue = append(s.Queue, id)
	}
	sort.Strings(s.Queue)
	return s
}
