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

// Package ledger is ThunderAgent's program-aware admission ledger
// (llm-d-porting 7d0e88e, rank-aware arm) as a pure state machine: no
// goroutines, no I/O, time passed in. The EPP plugin drives it.
//
//	request ──Arrive──▶ placed? ──yes──▶ forward to its backend
//	                      │ no
//	                      ▼
//	               waiting queue ◀──pause── Tick: backend over capacity
//	                      │
//	                      └──resume (origin only)── Tick: room again ──▶ forward
//
// A program (session) holds its context on one backend. Each backend's
// ledger counts every resident program's tokens plus a growth headroom:
//
//	used = Σ tokens(program) + programs × headroom   ≤  capacity (KV tokens)
//
// Policies fixed to the measured arm: resume on the origin backend only,
// at most ColdPrefills first-round prefills in flight per backend, and the
// pause loop nets out programs already marked (pause accounting
// "net-of-marked").
package ledger

import (
	"sort"
	"sync"
	"time"
)

// Status is what a program is doing right now.
type Status int

const (
	Reasoning Status = iota // a request is in flight or held
	Acting                  // between rounds (tool call)
)

// State is a program's place in the lifecycle.
type State int

const (
	Active State = iota // resident on a backend
	Paused              // in the waiting queue
)

// coldPrefillMax forgets a first-round mark whose stream never answered.
const coldPrefillMax = 120 * time.Second

type Config struct {
	HeadroomTokens int           // growth reserve per program (TA --program-headroom-tokens)
	ColdPrefills   int           // first-round prefills in flight per backend, 0 = unlimited
	ResumeTimeout  time.Duration // a held request is force-placed after this long
}

// Backend is one endpoint as the metrics layer reports it.
type Backend struct {
	Name     string
	Capacity int // KV tokens = num_gpu_blocks × block_size; 0 = not known yet
	Healthy  bool
}

type program struct {
	id        string
	seq       int // creation order: ties in every sort break by it
	backend   string
	origin    string // backend it was paused from; "" = never placed
	status    Status
	state     State
	tokens    int
	steps     int
	marked    bool          // pause when the current round ends
	waiter    chan struct{} // closed when a held request may go
	coldSince time.Time     // first round forwarded, no token yet
}

type backendState struct {
	Backend
	programs     map[string]*program
	futurePaused int // tokens of marked programs
}

type queued struct {
	tokens int // at pause time, the resume sort key
	seq    int
}

type Ledger struct {
	mu       sync.Mutex
	cfg      Config
	order    []string // backend names in configured order
	backends map[string]*backendState
	programs map[string]*program
	queue    map[string]queued
	seq      int
}

func New(cfg Config) *Ledger {
	return &Ledger{
		cfg:      cfg,
		backends: map[string]*backendState{},
		programs: map[string]*program{},
		queue:    map[string]queued{},
	}
}

// Admission is the answer to Arrive. Ready == nil means the request may go
// to Backend now; otherwise wait on Ready, then ask Placement.
type Admission struct {
	Backend string
	Ready   <-chan struct{}
}

// SetBackend adds a backend or refreshes its capacity and health.
func (l *Ledger) SetBackend(b Backend) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if existing, ok := l.backends[b.Name]; ok {
		existing.Backend = b
		return
	}
	// Sorted, so ties break the same way whatever order discovery reports
	// backends in (TA breaks them by its --backends order, ascending ports).
	l.order = append(l.order, b.Name)
	sort.Strings(l.order)
	l.backends[b.Name] = &backendState{Backend: b, programs: map[string]*program{}}
}

// Arrive sizes a program's next round and places or holds it
// (TA update_program_before_request, tr mode).
func (l *Ledger) Arrive(id string, promptTokens int, now time.Time) Admission {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := l.programs[id]
	if p == nil {
		l.seq++
		p = &program{id: id, seq: l.seq, status: Reasoning, state: Active}
		l.programs[id] = p
	}
	p.steps++
	p.tokens = promptTokens
	p.status = Reasoning

	// Paused between rounds: wait for the scheduler to resume it.
	if p.waiter != nil {
		return Admission{Ready: p.waiter}
	}

	// Placed already: go where its context is.
	if p.backend != "" {
		return Admission{Backend: p.backend}
	}

	// New program: place directly, or queue behind everyone waiting.
	if name := l.pickForNew(p.tokens, now); name != "" {
		l.place(p, name, now)
		return Admission{Backend: name}
	}
	p.state = Paused
	p.waiter = make(chan struct{})
	l.enqueue(p)
	return Admission{Ready: p.waiter}
}

// Placement is where a program may send its request; false if it was released.
func (l *Ledger) Placement(id string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := l.programs[id]
	if p == nil || p.backend == "" {
		return "", false
	}
	return p.backend, true
}

// FirstToken ends a first-round prefill's pacing mark.
func (l *Ledger) FirstToken(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if p := l.programs[id]; p != nil {
		p.coldSince = time.Time{}
	}
}

// Progress grows a streaming round's tokens.
func (l *Ledger) Progress(id string, delta int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if p := l.programs[id]; p != nil {
		p.tokens += delta
	}
}

// Finish closes a round with the engine's usage (TA update_program_after_request).
func (l *Ledger) Finish(id string, totalTokens int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := l.programs[id]
	if p == nil {
		return
	}
	p.coldSince = time.Time{}
	p.status = Acting
	p.tokens = totalTokens
	if !p.marked {
		return
	}

	// TA subtracts the program's tokens after they were updated to this
	// round's total, not the amount it marked; kept for decision parity.
	b := l.backends[p.backend]
	if b == nil {
		return
	}
	p.marked = false
	b.futurePaused = max(0, b.futurePaused-p.tokens)
	l.pause(p)
}

// Release forgets a finished program. A held request is woken and finds no placement.
func (l *Ledger) Release(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := l.programs[id]
	if p == nil {
		return false
	}

	b := l.backends[p.backend]
	if p.state == Paused {
		delete(l.queue, id)
		if p.waiter != nil {
			close(p.waiter)
			p.waiter = nil
		}
	} else if b != nil {
		delete(b.programs, id)
	}
	if b != nil && p.marked {
		b.futurePaused = max(0, b.futurePaused-p.tokens)
	}
	delete(l.programs, id)
	return true
}

// Expire force-places a request held longer than ResumeTimeout
// (TA _wait_for_resume timeout): origin, or the backend with fewest programs.
func (l *Ledger) Expire(id string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := l.programs[id]
	if p == nil || p.state != Paused {
		return
	}
	delete(l.queue, id)

	target := p.origin
	if target == "" {
		target = l.fewestPrograms()
	}
	if l.resume(p, target) {
		p.coldSince = now
	}
}

// pickForNew is TA _select_backend_for_new_program: nobody waiting, enough
// room, no first-round prefill in flight, least resident tokens.
func (l *Ledger) pickForNew(tokens int, now time.Time) string {
	if len(l.queue) > 0 {
		return ""
	}

	required := tokens + l.cfg.HeadroomTokens
	best, bestTokens := "", 0
	for _, name := range l.order {
		b := l.backends[name]
		if !b.Healthy {
			continue
		}
		if b.remaining(l.cfg.HeadroomTokens) < required {
			continue
		}
		if !l.canTakeCold(b, now) {
			continue
		}

		used := b.activeTokens()
		if best == "" || used < bestTokens {
			best, bestTokens = name, used
		}
	}
	return best
}

// fewestPrograms is TA select_backend_for_new_program_default.
func (l *Ledger) fewestPrograms() string {
	load := map[string]int{}
	for _, p := range l.programs {
		load[p.backend]++
	}

	best, bestLoad := "", 0
	for _, name := range l.order {
		if !l.backends[name].Healthy {
			continue
		}
		if best == "" || load[name] < bestLoad {
			best, bestLoad = name, load[name]
		}
	}
	if best == "" && len(l.order) > 0 {
		best = l.order[0]
	}
	return best
}

// place registers a program on a backend and starts its first-round mark.
func (l *Ledger) place(p *program, name string, now time.Time) {
	p.backend = name
	p.state = Active
	l.backends[name].programs[p.id] = p
	if p.steps == 1 {
		p.coldSince = now
	}
}

func (l *Ledger) enqueue(p *program) {
	l.seq++
	l.queue[p.id] = queued{tokens: p.tokens, seq: l.seq}
}

// pause moves a program between rounds off its backend into the queue.
func (l *Ledger) pause(p *program) {
	b := l.backends[p.backend]
	if b == nil {
		return
	}
	delete(b.programs, p.id)
	l.enqueue(p)
	p.origin = p.backend
	p.backend = ""
	p.state = Paused
	if p.waiter == nil {
		p.waiter = make(chan struct{})
	}
}

// resume registers a paused program on target and wakes its held request.
// It reports whether that request is a first round, which the caller marks
// as a cold prefill once its placement pass is over (TA marks it when the
// woken request runs, after the pass).
func (l *Ledger) resume(p *program, target string) bool {
	if p.state != Paused || l.backends[target] == nil {
		return false
	}
	firstRound := p.waiter != nil && p.status == Reasoning && p.steps == 1
	p.origin = ""
	l.backends[target].programs[p.id] = p
	p.backend = target
	p.state = Active
	if p.waiter != nil {
		close(p.waiter)
		p.waiter = nil
	}
	return firstRound
}

func (l *Ledger) canTakeCold(b *backendState, now time.Time) bool {
	if l.cfg.ColdPrefills <= 0 {
		return true
	}
	return b.coldInFlight(now) < l.cfg.ColdPrefills
}

func (b *backendState) coldInFlight(now time.Time) int {
	n := 0
	for _, p := range b.programs {
		if !p.coldSince.IsZero() && now.Sub(p.coldSince) < coldPrefillMax {
			n++
		}
	}
	return n
}

// activeTokens is reasoning + acting tokens (TA tool coefficient 1.0).
func (b *backendState) activeTokens() int {
	sum := 0
	for _, p := range b.programs {
		sum += p.tokens
	}
	return sum
}

// remaining is capacity minus residents and their headroom; unknown capacity never limits.
func (b *backendState) remaining(headroom int) int {
	if b.Capacity == 0 {
		return int(^uint(0) >> 1)
	}
	used := b.activeTokens() + len(b.programs)*headroom
	return b.Capacity - used
}

// overflow nets out marked programs (pause accounting "net-of-marked").
func (b *backendState) overflow(headroom int) int {
	if b.Capacity == 0 {
		return 0
	}
	required := b.activeTokens() + len(b.programs)*headroom - b.futurePaused
	return max(0, required-b.Capacity)
}

// sorted returns programs in creation order, stably sorted by key.
func sorted(ps []*program, key func(*program) int) []*program {
	sort.Slice(ps, func(i, j int) bool { return ps[i].seq < ps[j].seq })
	sort.SliceStable(ps, func(i, j int) bool { return key(ps[i]) < key(ps[j]) })
	return ps
}
