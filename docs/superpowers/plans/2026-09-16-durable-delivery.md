# Durable delivery implementation plan

> Execute with superpowers:subagent-driven-development and test-driven-development.

**Goal:** A Python utility that durably synchronizes Codex usage without running an LLM.
**Architecture:** JSONL -> local SQLite ledger -> acknowledged batches -> existing receiver.
**Stack:** Python standard library, SQLite, native OS startup, existing JS dashboard.

- [x] Baseline: exclude private TOML/data, initialize Git on `durable-delivery`, commit current sources.
- [x] Client (`codex_usage.py`, `test_codex_usage.py`): canonical response validation, atomic
  JSONL cursor + ledger, incomplete-line retry, persistent ACK flags, daily digest reconciliation,
  direct HTTPS independent of inherited proxy, install/run/once/status/doctor commands.
  Start with failing tests; run `python -m unittest -v test_codex_usage.py` after implementation.
- [x] Receiver (`server.py`, `test_sync.py`): authenticated `/v1/usage`, ACK after commit,
  global response ID conflicts, inventory, safe session activation, persistent sync status.
  Start with failing API tests; verify retries, rejection atomicity, DB restart and cross-host conflicts.
- [x] Integration (`dashboard.js`, docs): existing machine card shows last reported queue/error/sync age;
  expose journal source in API/export. Account with a single source per whole session to avoid
  guessing cross-protocol response identities. Retain original OTLP rows; active journal sessions
  use complete journal history. Existing sessions require explicit adoption and cross-host checks.
- [x] Fault checks: real test HTTP server, lost ACK, HTTP 401/429/500, sender/database restart,
  restored older server DB, partial JSONL, copied session and secret sentinel.
  Run `python -m unittest -v test_server.py test_analytics.py test_pricing.py test_sync.py test_codex_usage.py`.
- [x] Review: spec review, then code quality review; resolve findings and rerun affected checks.
- [x] Deployment: backup SQLite, stage immutable release, run Linux tests, update Nginx for
  Bearer-authenticated endpoint, atomic release switch, health + production round-trip.
- [x] Onboarding: install on ASUS, verify real responses, reconcile and deliberately adopt verified
  existing Windows sessions. Publish private Git repo and provide portable one-command instructions.
  Other machines count as installed only after an actual run there; no SSH access is currently supplied.

## Wire contract v1

POST `/v1/usage`, JSON and existing ingest Bearer credential. Envelope: `version=1`,
`action`, `machine`, `collector_id`; maximum 200 events per batch. Responses have `version=1`.

- `events`: `events` list. Return `accepted` response IDs after commit. Conflicting ID -> 409,
  invalid batch -> 400; the entire batch rolls back.
- `inventory`: `days=[{day,count,digest}]`, `status={scanned_at_ms,pending_events,pending_tokens,last_error}`.
  Return `resend_days`. Compare the collector's complete ledger per UTC day, not only pending rows.
- `activate`: `sessions=[session IDs]`. Return `activated` IDs. Whole sessions only; require stored
  journal records, no legacy history under another host, and no competing collector. No approximate
  token matching. New sessions auto-activate after complete scan + ACK; existing sessions only when adopted.

Shared pure functions in standalone `codex_usage.py`, also imported by server:
`canonical_event(data)`, `event_digest(event)`, `day_inventory(events)`.
Canonical input keys: `response_id,machine,collector_id,session,timestamp_ms,model,client_version,
input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_output_tokens,
total_tokens,effort,service_tier`. Unknown metadata stays null; no prompt/tool text persists.
Canonical output adds stable `id=sha256('journal:'+response_id)`, `source='journal'`, UTC
`event_timestamp`, `host=machine`, `client='codex_journal'`, `tool_tokens=null`.
Digest: SHA-256 of sorted compact JSON of canonical event, UTF-8.
Daily digest: SHA-256 of sorted `response_id:event_digest\n` lines for UTC date.
