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

package programaware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// The plugin's hooks driven directly, as the EPP director calls them.

const (
	blockSize     = 4096
	numBlocks     = 94 // one rank of the weka158 mns8 shape: 385,024 tokens
	sessionTokens = 100_000
	// (100k + 16k headroom) × 3 fits in 385k, × 4 does not.
	sessionsPerRank = 3
	waitForHold     = 100 * time.Millisecond
)

var ranks = []*datalayer.EndpointMetadata{
	{Address: "127.0.0.1", Port: "8410"},
	{Address: "127.0.0.1", Port: "8411"},
}

func newPlugin(t *testing.T, resumeTimeout time.Duration) *Plugin {
	return newPluginWith(t, func(params *Parameters) { params.ResumeTimeout = duration{resumeTimeout} })
}

// newPluginWith builds the plugin on two fresh ranks of the weka158 shape.
func newPluginWith(t *testing.T, configure func(*Parameters)) *Plugin {
	params := defaultParameters()
	configure(&params)
	p := New(PluginType, params)

	now := time.Now()
	for _, meta := range ranks {
		metrics := datalayer.NewMetrics()
		metrics.CacheBlockSize, metrics.CacheNumBlocks, metrics.UpdateTime = blockSize, numBlocks, now
		require.NoError(t, p.Extract(context.Background(), datalayer.EndpointEvent{
			Type: datalayer.EventAddOrUpdate, Endpoint: datalayer.NewEndpoint(meta, metrics),
		}))
	}
	p.refreshBackends(now)
	return p
}

func request(session string) *scheduling.InferenceRequest {
	return &scheduling.InferenceRequest{
		RequestID: fmt.Sprintf("%s-%d", session, time.Now().UnixNano()),
		Headers:   map[string]string{defaultSessionHeader: session},
		Body: &requesthandling.InferenceRequestBody{
			Completions: &requesthandling.CompletionsRequest{Prompt: requesthandling.Prompt{TokenIDs: make([]uint32, sessionTokens)}},
		},
	}
}

func candidates() []scheduling.Endpoint {
	eps := make([]scheduling.Endpoint, 0, len(ranks))
	for _, meta := range ranks {
		eps = append(eps, scheduling.NewEndpoint(meta, datalayer.NewMetrics(), nil))
	}
	return eps
}

// finish ends one round with usage, as the last response chunk does.
func finish(p *Plugin, req *scheduling.InferenceRequest, total int) {
	p.ResponseBody(context.Background(), req, &requestcontrol.Response{
		RequestID: req.RequestID, StartOfStream: true, EndOfStream: true,
		Usage: requesthandling.Usage{TotalTokens: total},
	}, nil)
}

func TestConcurrentArrivalsNeverOverbook(t *testing.T) {
	// No cold-prefill pacing: capacity is the only limit.
	p := newPluginWith(t, func(params *Parameters) { params.ColdPrefills = 0 })

	const sessions = 10
	admitted := make(chan string, sessions)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if p.RequestHeader(ctx, request(id)) == nil {
				admitted <- id
			}
		}(fmt.Sprintf("s%d", i))
	}
	time.Sleep(waitForHold)

	snapshot := p.ledger.Snapshot(time.Now())
	require.Len(t, admitted, sessionsPerRank*len(ranks))
	require.Len(t, snapshot.Queue, sessions-sessionsPerRank*len(ranks))
	for name, b := range snapshot.Backends {
		require.LessOrEqual(t, b.Tokens+b.Programs*p.params.HeadroomTokens, b.Capacity, name)
	}

	cancel()
	wg.Wait()
}

func TestFilterKeepsOnlyTheProgramsBackend(t *testing.T) {
	p := newPlugin(t, time.Hour)
	req := request("s0")
	require.NoError(t, p.RequestHeader(context.Background(), req))

	kept := p.Filter(context.Background(), req, candidates())

	require.Len(t, kept, 1)
	placed, _ := p.ledger.Placement("s0")
	require.Equal(t, placed, backendName(kept[0].GetMetadata()))
}

func TestRequestWithoutSessionPassesThrough(t *testing.T) {
	p := newPlugin(t, time.Hour)
	req := request("")

	require.NoError(t, p.RequestHeader(context.Background(), req))
	require.Len(t, p.Filter(context.Background(), req, candidates()), len(ranks))
	require.Empty(t, p.ledger.Snapshot(time.Now()).Programs)
}

// holdOne fills both ranks' cold-prefill slots so the next new session is held.
func holdOne(t *testing.T, p *Plugin) <-chan error {
	for i := range ranks {
		require.NoError(t, p.RequestHeader(context.Background(), request(fmt.Sprintf("first%d", i))))
	}
	req := request("late")
	done := make(chan error, 1)
	go func() { done <- p.RequestHeader(context.Background(), req) }()
	time.Sleep(waitForHold)
	require.Equal(t, []string{"late"}, p.ledger.Snapshot(time.Now()).Queue)
	return done
}

func TestReleaseOfHeldProgramAnswers503(t *testing.T) {
	p := newPlugin(t, time.Hour)
	done := holdOne(t, p)

	body, _ := json.Marshal(releaseRequest{ProgramID: "late"})
	rec := httptest.NewRecorder()
	p.adminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, releasePath, bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp releaseResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.True(t, resp.Released)

	err := <-done
	var typed errcommon.Error
	require.ErrorAs(t, err, &typed)
	require.Equal(t, errcommon.ServiceUnavailable, typed.Code)
}

func TestFirstTokenLetsTheHeldProgramIn(t *testing.T) {
	p := newPlugin(t, time.Hour)
	done := holdOne(t, p)

	first := request("first0")
	p.ResponseBody(context.Background(), first, &requestcontrol.Response{RequestID: first.RequestID, StartOfStream: true}, nil)
	p.ledger.Tick(time.Now())

	require.NoError(t, <-done)
	placed, ok := p.ledger.Placement("late")
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:8410", placed)
}

func TestResumeTimeoutForcesPlacement(t *testing.T) {
	// Longer than holdOne's wait, so the request is still queued when it checks.
	p := newPlugin(t, 3*waitForHold)
	done := holdOne(t, p)

	require.NoError(t, <-done)
	_, ok := p.ledger.Placement("late")
	require.True(t, ok)
}

func TestDroppedClientEndsTheHold(t *testing.T) {
	p := newPlugin(t, time.Hour)
	for i := range ranks {
		require.NoError(t, p.RequestHeader(context.Background(), request(fmt.Sprintf("first%d", i))))
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitForHold)
	defer cancel()
	require.ErrorIs(t, p.RequestHeader(ctx, request("late")), context.DeadlineExceeded)
}

func TestStreamedChunksGrowTheRound(t *testing.T) {
	p := newPlugin(t, time.Hour)
	req := request("s0")
	require.NoError(t, p.RequestHeader(context.Background(), req))

	const chunks = 3*tokenProgressInterval + 5
	for i := 0; i < chunks; i++ {
		p.ResponseBody(context.Background(), req, &requestcontrol.Response{RequestID: req.RequestID, StartOfStream: i == 0}, nil)
	}
	require.Equal(t, sessionTokens+3*tokenProgressInterval, p.ledger.Snapshot(time.Now()).Programs["s0"].Tokens)

	finish(p, req, sessionTokens+chunks)
	view := p.ledger.Snapshot(time.Now()).Programs["s0"]
	require.True(t, view.Acting)
	require.Equal(t, sessionTokens+chunks, view.Tokens)
}

func TestStreamWithoutUsageOnlyEndsColdPrefill(t *testing.T) {
	p := newPlugin(t, time.Hour)
	req := request("s0")
	require.NoError(t, p.RequestHeader(context.Background(), req))

	p.ResponseBody(context.Background(), req, &requestcontrol.Response{RequestID: req.RequestID, StartOfStream: true, EndOfStream: true}, nil)

	snapshot := p.ledger.Snapshot(time.Now())
	require.False(t, snapshot.Programs["s0"].Acting)
	for _, b := range snapshot.Backends {
		require.Zero(t, b.ColdPrefills)
	}
}

func TestStaleMetricsMakeABackendUnhealthy(t *testing.T) {
	p := newPlugin(t, time.Hour)
	p.refreshBackends(time.Now().Add(defaultMetricsStaleAfter))

	held := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), waitForHold)
	defer cancel()
	go func() { held <- p.RequestHeader(ctx, request("s0")) }()

	require.ErrorIs(t, <-held, context.DeadlineExceeded)
}
