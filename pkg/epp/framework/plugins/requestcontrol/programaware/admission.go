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
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	// TA reports streamed tokens to its ledger every 20 SSE events.
	tokenProgressInterval = 20
	// Text prompts without token ids are sized from their bytes (EPP's own estimate).
	estimatedBytesPerToken = 4
)

// RequestHeader sizes the round and places it, or holds the request until the
// scheduler resumes the program. The endpoint snapshot is taken after this
// returns, so scheduling sees metrics from after the hold.
func (p *Plugin) RequestHeader(ctx context.Context, request *scheduling.InferenceRequest) error {
	id := p.sessionID(request)
	if id == "" {
		return nil
	}

	admission := p.ledger.Arrive(id, promptTokens(request), time.Now())
	if admission.Ready == nil {
		return nil
	}

	logger := log.FromContext(ctx).WithName(PluginType)
	start := time.Now()
	logger.Info("program held", "program", id)
	if err := p.hold(ctx, id, admission.Ready); err != nil {
		logger.Info("program hold ended", "program", id, "held", time.Since(start).String(), "cause", err.Error())
		return err
	}

	backend, ok := p.ledger.Placement(id)
	if !ok {
		return errcommon.Error{Code: errcommon.ServiceUnavailable, Msg: "program released while held: " + id}
	}
	logger.Info("program resumed", "program", id, "backend", backend, "held", time.Since(start).String())
	return nil
}

// hold waits for the resume; past resumeTimeout the ledger force-places the
// program (TA _wait_for_resume). A dropped client ends the wait; the program
// stays queued, as in TA.
func (p *Plugin) hold(ctx context.Context, id string, ready <-chan struct{}) error {
	timer := time.NewTimer(p.params.ResumeTimeout.Duration)
	defer timer.Stop()

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		p.ledger.Expire(id, time.Now())
	}

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Filter keeps only the backend that holds the program's context.
func (p *Plugin) Filter(_ context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) []scheduling.Endpoint {
	id := p.sessionID(request)
	if id == "" {
		return endpoints
	}

	backend, ok := p.ledger.Placement(id)
	if !ok {
		return endpoints
	}

	for _, endpoint := range endpoints {
		if backendName(endpoint.GetMetadata()) == backend {
			return []scheduling.Endpoint{endpoint}
		}
	}
	return nil
}

// ResponseBody feeds the stream back into the ledger:
//
//	first chunk   → first token (ends the cold-prefill mark)
//	every chunk   → one streamed event, reported in steps of 20
//	last chunk    → round over: the engine's usage replaces the estimate
//
// Envoy forwards each SSE event vLLM flushes as its own chunk, so chunks
// stand in for TA's event count. A stream that ends without usage only
// clears its cold-prefill mark; TA leaves that mark to expire.
func (p *Plugin) ResponseBody(_ context.Context, request *scheduling.InferenceRequest, response *requestcontrol.Response, _ *datalayer.EndpointMetadata) {
	id := p.sessionID(request)
	if id == "" {
		return
	}

	if response.StartOfStream {
		p.ledger.FirstToken(id)
	}

	if response.EndOfStream {
		p.streamed.Delete(response.RequestID)
		if total := response.Usage.TotalTokens; total > 0 {
			p.ledger.Finish(id, total)
		}
		return
	}

	value, _ := p.streamed.LoadOrStore(response.RequestID, new(int))
	pending := value.(*int)
	*pending++
	if *pending >= tokenProgressInterval {
		p.ledger.Progress(id, *pending)
		*pending = 0
	}
}

func (p *Plugin) sessionID(request *scheduling.InferenceRequest) string {
	if request == nil {
		return ""
	}
	return request.Headers[p.params.SessionHeader]
}

// promptTokens is the round's prompt length: token ids when the client sent
// them, the tokenizer's count when a token producer ran, else a byte estimate.
func promptTokens(request *scheduling.InferenceRequest) int {
	body := request.Body
	if body != nil && body.Completions != nil {
		if n := body.Completions.Prompt.TokenCountHint(); n >= 0 {
			return n
		}
	}
	if body != nil && body.TokenizedPrompt != nil {
		return body.TokenizedPrompt.TokenCount()
	}
	return request.RequestSizeBytes / estimatedBytesPerToken
}
