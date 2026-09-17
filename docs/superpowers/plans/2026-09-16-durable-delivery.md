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

## Historical onboarding extension — 2026-09-17

User requested automatic recovery starting 2026-09-05 before installation.
Reuse the existing ledger, parser, ACK delivery and whole-session activation.

- [x] `backfill --since YYYY-MM-DD` produces a local/server comparison without sending usage.
  `--apply` adopts and synchronizes only complete journals owned by this machine.
  `install --backfill-since YYYY-MM-DD` runs the same step before autostart.
- [x] Read-only authenticated `inspect` sync action returns session owners/collectors,
  active owner, visible totals in the requested window and whether earlier server history exists.
  Unknown/multiple/foreign ownership and competing collectors remain explicitly blocked.
- [x] Reuse the parser in temporary state. Never derive ownership from rewritten cwd.
  For adopted history, per-session `history_since` permits ignoring unsupported prefixes
  before the requested UTC date; do not upload earlier counters. Activation carries
  `since_ms` and rejects any existing server history before that date.
- [x] Dry-run leaves the production ledger/config/server totals unchanged. Apply backs up
  local state, persists adoption atomically, and uses normal retry/deduplication.
  Return structured reasons for exclusions so an agent can resolve provenance separately.
- [x] Tests: copied/unknown owners, old session active after cutoff, legacy prefix before vs
  after cutoff, pre-cutoff server history, failed inspect, repeated apply, lost ACK and installer integration.
- [x] Document exact human command and agent workflow; verify available ASUS history,
  deploy a new immutable release, update the installed sender, commit and push.

Wire: `inspect` envelope has `sessions` (1..200 IDs), `since_ms` (UTC cutoff integer).
Return `{version:1,sessions:[{session,machines,collectors,active_collector,
active_machine,before_since,events,total_tokens}]}`. No registration/activity mutation.
`activate` gains optional `since_ms` (default 0), applied to every ID in that batch.
Client config `history_since` maps explicitly adopted session IDs to UTC milliseconds;
existing adoptions default to 0 and retain their established parsing scope.

Verified: 92 Windows tests, 48 Linux server tests; commit `7cf826e`, release
`20260917T091549Z-durable`. ASUS adopted 15 additional sessions / 3,264 records.
For September 5–15 UTC, public CSV matches 8,907 ledger records; visible total
1,079,242,619 tokens (+141,298,966). All 31,484 original server rows retained.
33 foreign Mac sessions and 6 unknown-owner sessions remain explicitly excluded.

## Mandatory initial recovery — 2026-09-17

Plain `install` now selects 2026-09-05 by default, preserving an explicitly saved date.
The task is persisted before recovery and retried by the existing sender after transport
recovers, including after restart. A completed check is not repeated on every scan.
`status`/`doctor` expose pending state, history errors, exclusion count and the saved
`backfill-report.json`. Unrelated file errors do not suppress recovery of valid history.
Agent onboarding instructions require checking that report and reporting exclusions.
Verified: 95 Windows tests, including offline install/restart, unrelated broken journal,
failed same-date reinstall after success, and preserving a custom start date.
