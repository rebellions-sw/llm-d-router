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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The session contract TA clients already speak, on a port of its own:
//
//	POST /programs/release {"program_id": "s1"} → {"program_id": "s1", "released": true}
//	GET  /ta/metrics                            → ledger snapshot
const (
	releasePath      = "/programs/release"
	metricsPath      = "/ta/metrics"
	shutdownGrace    = 5 * time.Second
	readHeaderLimit  = 10 * time.Second
	maxReleaseBodyKB = 64
)

type releaseRequest struct {
	ProgramID string `json:"program_id"`
}

type releaseResponse struct {
	ProgramID string `json:"program_id"`
	Released  bool   `json:"released"`
}

// serveAdmin binds the admin port now, so a taken port fails the EPP at start.
func (p *Plugin) serveAdmin(ctx context.Context) error {
	listener, err := net.Listen("tcp", p.params.AdminAddress)
	if err != nil {
		return fmt.Errorf("%s: admin listen %s: %w", PluginType, p.params.AdminAddress, err)
	}

	server := &http.Server{Handler: p.adminHandler(), ReadHeaderTimeout: readHeaderLimit}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.FromContext(ctx).Error(err, "program-aware admin server stopped")
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return nil
}

func (p *Plugin) adminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+releasePath, p.handleRelease)
	mux.HandleFunc("GET "+metricsPath, p.handleMetrics)
	return mux
}

// handleRelease forgets a finished program; releasing an unknown one is not an error.
func (p *Plugin) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req releaseRequest
	body := http.MaxBytesReader(w, r.Body, maxReleaseBodyKB<<10)
	if err := json.NewDecoder(body).Decode(&req); err != nil || req.ProgramID == "" {
		http.Error(w, "body must be {\"program_id\": \"...\"}", http.StatusBadRequest)
		return
	}

	released := p.ledger.Release(req.ProgramID)
	log.FromContext(r.Context()).WithName(PluginType).Info("program released", "program", req.ProgramID, "known", released)
	writeJSON(w, releaseResponse{ProgramID: req.ProgramID, Released: released})
}

func (p *Plugin) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, p.ledger.Snapshot(time.Now()))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
