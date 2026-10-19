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

// Package programaware is the EPP plugin that runs ThunderAgent's
// program-aware admission (./ledger) inside the llm-d request path.
//
//	client ─x-session-id─▶ Envoy ─ext_proc─▶ EPP
//	                                          ├ RequestHeader ledger.Arrive: place, or hold until resumed
//	                                          ├ Filter        keep only the program's backend
//	                                          ├ ResponseBody  first token / streamed tokens / usage
//	                                          └ ticker        ledger.Tick every schedulerInterval
//	replayer ─POST /programs/release─▶ admin port (release, /ta/metrics)
//
// Requests without the session header pass through untouched.
package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/programaware/ledger"
)

const PluginType = "program-aware"

// Defaults are the rank-aware arm's TA flags.
const (
	defaultHeadroomTokens    = 16_384
	defaultColdPrefills      = 1
	defaultSchedulerInterval = 5 * time.Second
	defaultResumeTimeout     = 2 * time.Hour
	defaultAdminAddress      = ":9320"
	defaultSessionHeader     = "x-session-id"
	// A backend whose metrics are older than this is unhealthy (TA: last scrape failed).
	defaultMetricsStaleAfter = 10 * time.Second
)

var (
	_ requestcontrol.RequestHeaderProcessor = &Plugin{}
	_ scheduling.Filter                     = &Plugin{}
	_ requestcontrol.ResponseBodyProcessor  = &Plugin{}
	_ datalayer.EndpointExtractor           = &Plugin{}
	_ datalayer.Registrant                  = &Plugin{}
)

type Parameters struct {
	HeadroomTokens    int      `json:"programHeadroomTokens"`
	ColdPrefills      int      `json:"coldPrefillsPerBackend"`
	SchedulerInterval duration `json:"schedulerInterval"`
	ResumeTimeout     duration `json:"resumeTimeout"`
	AdminAddress      string   `json:"adminAddress"`
	SessionHeader     string   `json:"sessionHeader"`
	MetricsStaleAfter duration `json:"metricsStaleAfter"`
}

// duration reads "5s"-style strings from the plugin config.
type duration struct{ time.Duration }

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type Plugin struct {
	typedName plugin.TypedName
	params    Parameters
	ledger    *ledger.Ledger

	endpoints sync.Map // backend name → datalayer.Endpoint (live metrics)
	streamed  sync.Map // request id → *int, response chunks not yet reported
}

func defaultParameters() Parameters {
	return Parameters{
		HeadroomTokens:    defaultHeadroomTokens,
		ColdPrefills:      defaultColdPrefills,
		SchedulerInterval: duration{defaultSchedulerInterval},
		ResumeTimeout:     duration{defaultResumeTimeout},
		AdminAddress:      defaultAdminAddress,
		SessionHeader:     defaultSessionHeader,
		MetricsStaleAfter: duration{defaultMetricsStaleAfter},
	}
}

func Factory(name string, raw *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	params := defaultParameters()
	if raw != nil {
		if err := raw.Decode(&params); err != nil {
			return nil, fmt.Errorf("%s: decode parameters: %w", PluginType, err)
		}
	}

	p := New(name, params)
	ctx := handle.Context()
	if err := p.serveAdmin(ctx); err != nil {
		return nil, err
	}
	go p.schedule(ctx)

	log.FromContext(ctx).Info("program-aware admission",
		"headroom", params.HeadroomTokens, "coldPrefills", params.ColdPrefills,
		"schedulerInterval", params.SchedulerInterval.String(), "resumeTimeout", params.ResumeTimeout.String(),
		"admin", params.AdminAddress)
	return p, nil
}

// New builds the plugin without starting its ticker or admin server (tests).
func New(name string, params Parameters) *Plugin {
	return &Plugin{
		typedName: plugin.TypedName{Type: PluginType, Name: name},
		params:    params,
		ledger: ledger.New(ledger.Config{
			HeadroomTokens: params.HeadroomTokens,
			ColdPrefills:   params.ColdPrefills,
			ResumeTimeout:  params.ResumeTimeout.Duration,
		}),
	}
}

func (p *Plugin) TypedName() plugin.TypedName {
	return p.typedName
}

// RegisterDependencies subscribes to endpoint add/delete events.
func (p *Plugin) RegisterDependencies(r datalayer.Registrar) error {
	return r.Register(datalayer.PendingRegistration{
		Owner:         p.TypedName(),
		SourceType:    notifications.EndpointNotificationSourceType,
		Extractor:     p,
		DefaultSource: notifications.NewEndpointDataSource(notifications.EndpointNotificationSourceType, notifications.EndpointNotificationSourceType),
	})
}

// Extract keeps the live endpoint objects the ticker reads metrics from.
func (p *Plugin) Extract(_ context.Context, event datalayer.EndpointEvent) error {
	if event.Endpoint == nil || event.Endpoint.GetMetadata() == nil {
		return nil
	}

	name := backendName(event.Endpoint.GetMetadata())
	switch event.Type {
	case datalayer.EventAddOrUpdate:
		p.endpoints.Store(name, event.Endpoint)
	case datalayer.EventDelete:
		p.endpoints.Delete(name)
		p.ledger.SetBackend(ledger.Backend{Name: name, Healthy: false})
	}
	return nil
}

// schedule refreshes backend capacity and runs a ledger pass on every tick.
func (p *Plugin) schedule(ctx context.Context) {
	ticker := time.NewTicker(p.params.SchedulerInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.refreshBackends(now)
			p.ledger.Tick(now)
		}
	}
}

// refreshBackends turns each endpoint's latest metrics into ledger capacity.
func (p *Plugin) refreshBackends(now time.Time) {
	p.endpoints.Range(func(key, value any) bool {
		metrics := value.(datalayer.Endpoint).GetMetrics()
		fresh := metrics != nil && now.Sub(metrics.UpdateTime) < p.params.MetricsStaleAfter.Duration

		capacity := 0
		if metrics != nil {
			capacity = metrics.CacheNumBlocks * metrics.CacheBlockSize
		}
		p.ledger.SetBackend(ledger.Backend{Name: key.(string), Capacity: capacity, Healthy: fresh})
		return true
	})
}

// backendName is the endpoint's host:port, the form Envoy routes on.
func backendName(meta *datalayer.EndpointMetadata) string {
	return net.JoinHostPort(meta.Address, meta.Port)
}
