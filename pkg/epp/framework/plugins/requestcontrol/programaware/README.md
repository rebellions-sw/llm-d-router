# program-aware

ThunderAgent's program-aware admission as an EPP plugin. Keeps each agent
session's KV on one rank (mixed, non-PD engines).

```
client ─x-session-id─▶ Envoy ─ext_proc─▶ EPP
                                          ├ RequestHeader  ledger: place on a rank, or hold until one has room
                                          ├ Filter         keep only the session's rank
                                          ├ ResponseBody   token growth, usage
                                          └ ticker         pause sessions on an over-capacity rank, resume on their own rank
client ─POST /programs/release─▶ admin :9320 (release, GET /ta/metrics)
```

Rank capacity = `num_gpu_blocks × block_size` from `vllm:cache_config_info`.
Requests without the session header pass through untouched.

## Config

Alpha: start the EPP with `--allow-experimental-plugins`. Put the plugin first
in the profile so scorers see only the session's rank.

```yaml
plugins:
  - name: program-aware
    type: program-aware
    parameters:
      programHeadroomTokens: 16384  # growth reserved per session at admission
      coldPrefillsPerBackend: 1     # first-turn prefills in flight per rank
      schedulerInterval: 5s         # pause/resume pass
      resumeTimeout: 7200s          # a held request is force-placed after this
      adminAddress: ":9320"
      sessionHeader: x-session-id
      metricsStaleAfter: 10s        # a rank with older metrics is unhealthy
  - name: metrics-source             # program-aware reads capacity from these
    type: metrics-data-source
  - name: metrics-extractor
    type: core-metrics-extractor
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: program-aware
      - pluginRef: prefix-cache-scorer
      # ...
```

## Client contract

| Requirement | Role |
|---|---|
| `x-session-id` header on every round | groups rounds into a session |
| `stream_options.include_usage: true` when streaming | final usage ends the round; the session becomes pausable |
| `POST /programs/release {"program_id": "<id>"}` at session end | returns the session's capacity (no idle expiry) |

## Limits

- One EPP replica; the ledger is in-process.
- Mixed engines only (no PD).
- `ledger/` matches ThunderAgent (`llm-d-porting` 7d0e88e) on 30 recorded traces (`TestParityWithThunderAgent`).
