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

// Tick is one scheduler pass (TA _scheduled_check): resume what fits, then
// pause until every backend is within capacity. The plugin calls it on a timer.
func (l *Ledger) Tick(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.resumeWaiting(now)

	for _, name := range l.order {
		b := l.backends[name]
		if b.Capacity == 0 || b.remaining(l.cfg.HeadroomTokens) >= 0 {
			continue
		}
		l.pauseUntilSafe(b)
	}
}

// resumeWaiting is TA _greedy_resume with the origin placement.
//
// Selection: walk the queue in priority order (held round > new program >
// between rounds, each by tokens at pause time) and keep every program that
// still fits the pool's total room. Placement: a program that was ever
// placed goes back to its origin or keeps waiting; a new one goes to the
// backend with the most room, at most one resume of any kind per backend
// before it in the same pass.
func (l *Ledger) resumeWaiting(now time.Time) {
	headroom := l.cfg.HeadroomTokens

	room := map[string]int{}
	var roomOrder []string
	total := 0
	for _, name := range l.order {
		b := l.backends[name]
		if b.Capacity == 0 || !b.Healthy {
			continue
		}
		r := b.remaining(headroom)
		if r <= headroom {
			continue
		}
		room[name] = r
		roomOrder = append(roomOrder, name)
		total += r
	}
	if len(roomOrder) == 0 || total <= 0 {
		return
	}

	selected := l.selectResumable(total)

	coldStarted := map[string]bool{}
	var firstRounds []*program
	for _, p := range selected {
		required := p.tokens + headroom
		isNew := p.origin == ""

		var candidates []string
		if isNew {
			candidates = l.coldCandidates(roomOrder, room, coldStarted, now)
		} else if _, ok := room[p.origin]; ok {
			candidates = []string{p.origin}
		}

		target := ""
		for _, name := range candidates {
			if room[name] >= required {
				target = name
				break
			}
		}
		if target == "" {
			continue
		}

		delete(l.queue, p.id)
		if l.resume(p, target) {
			firstRounds = append(firstRounds, p)
		}
		// TA checks origin_backend after _resume_program cleared it, so every
		// resume, not only a new program's, closes its backend to new programs
		// for the rest of the pass. Kept for decision parity.
		room[target] -= required
		coldStarted[target] = true
	}

	for _, p := range firstRounds {
		p.coldSince = now
	}
}

// selectResumable orders the queue by priority and keeps what fits in total.
func (l *Ledger) selectResumable(total int) []*program {
	var waiting []*program
	for id := range l.queue {
		if p := l.programs[id]; p != nil {
			waiting = append(waiting, p)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return l.queue[waiting[i].id].seq < l.queue[waiting[j].id].seq })
	sort.SliceStable(waiting, func(i, j int) bool { return l.queue[waiting[i].id].tokens < l.queue[waiting[j].id].tokens })

	var held, fresh, idle []*program
	for _, p := range waiting {
		switch {
		case p.steps == 1:
			fresh = append(fresh, p)
		case p.status == Reasoning:
			held = append(held, p)
		default:
			idle = append(idle, p)
		}
	}

	var selected []*program
	cumulative := 0
	for _, group := range [][]*program{held, fresh, idle} {
		for _, p := range group {
			required := p.tokens + l.cfg.HeadroomTokens
			if cumulative+required > total {
				continue
			}
			selected = append(selected, p)
			cumulative += required
		}
	}
	return selected
}

// coldCandidates lists backends a new program may start on, most room first.
func (l *Ledger) coldCandidates(roomOrder []string, room map[string]int, coldStarted map[string]bool, now time.Time) []string {
	var names []string
	for _, name := range roomOrder {
		if coldStarted[name] || !l.canTakeCold(l.backends[name], now) {
			continue
		}
		names = append(names, name)
	}
	sort.SliceStable(names, func(i, j int) bool { return room[names[i]] > room[names[j]] })
	return names
}

// pauseUntilSafe is TA _pause_until_safe with net-of-marked accounting:
// pause the smallest program between rounds first; if none, mark the
// smallest unmarked round so it pauses when it ends.
func (l *Ledger) pauseUntilSafe(b *backendState) {
	for b.overflow(l.cfg.HeadroomTokens) > 0 {
		if p := smallest(b, Acting); p != nil {
			l.pause(p)
			continue
		}
		if p := smallest(b, Reasoning); p != nil {
			p.marked = true
			b.futurePaused += p.tokens
			continue
		}
		return
	}
}

// smallest returns the backend's unmarked program with this status and fewest tokens.
func smallest(b *backendState, status Status) *program {
	var ps []*program
	for _, p := range b.programs {
		if p.status != status || p.marked {
			continue
		}
		ps = append(ps, p)
	}
	if len(ps) == 0 {
		return nil
	}
	return sorted(ps, func(p *program) int { return p.tokens })[0]
}
