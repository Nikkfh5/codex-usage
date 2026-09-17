#!/usr/bin/env python3
"""Durable, metadata-only Codex JSONL usage delivery. Client: Python 3.11+.

The three canonical helpers remain importable on the receiver's Python 3.10.
"""
import argparse
import contextlib
import datetime as dt
import getpass
import hashlib
import http.client
import json
import os
from pathlib import Path
import plistlib
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


TOKEN_KEYS = ("input_tokens", "cached_input_tokens", "cache_write_input_tokens",
              "output_tokens", "reasoning_output_tokens", "total_tokens")
TEXT_KEYS = ("response_id", "machine", "collector_id", "session", "model",
             "client_version", "effort", "service_tier")
DEFAULT_BACKFILL_SINCE = "2026-09-05"


def compact(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_event(data):
    if not isinstance(data, dict):
        raise ValueError("invalid_event")
    result = {}
    for key in TEXT_KEYS:
        value = data.get(key)
        required = key in ("response_id", "machine", "collector_id", "session")
        if value is None and not required:
            result[key] = None
        elif not isinstance(value, str) or not value.strip() or len(value) > 256 or any(ord(c) < 32 for c in value):
            raise ValueError("invalid_" + key)
        else:
            result[key] = value
    for key in ("timestamp_ms",) + TOKEN_KEYS:
        value = data.get(key)
        if value is None and key in ("cached_input_tokens", "cache_write_input_tokens", "reasoning_output_tokens"):
            result[key] = None
        elif type(value) is not int or value < 0 or value > 9007199254740991:
            raise ValueError("invalid_" + key)
        else:
            result[key] = value
    if result["total_tokens"] != result["input_tokens"] + result["output_tokens"]:
        raise ValueError("invalid_total_tokens")
    if (result["cached_input_tokens"] or 0) + (result["cache_write_input_tokens"] or 0) > result["input_tokens"]:
        raise ValueError("invalid_input_components")
    if (result["reasoning_output_tokens"] or 0) > result["output_tokens"]:
        raise ValueError("invalid_reasoning_output_tokens")
    try:
        stamp = dt.datetime.fromtimestamp(result["timestamp_ms"] / 1000, dt.timezone.utc)
    except (ValueError, OverflowError, OSError):
        raise ValueError("invalid_timestamp_ms") from None
    result.update(id=hashlib.sha256(("journal:" + result["response_id"]).encode()).hexdigest(),
                  source="journal", event_timestamp=stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                  host=result["machine"], client="codex_journal", tool_tokens=None)
    return result


def event_digest(event):
    return hashlib.sha256(compact(canonical_event(event)).encode("utf-8")).hexdigest()


def day_inventory(events):
    days = {}
    for raw in events:
        event = canonical_event(raw)
        days.setdefault(event["event_timestamp"][:10], []).append(event["response_id"] + ":" + event_digest(event) + "\n")
    return [dict(day=day, count=len(rows), digest=hashlib.sha256("".join(sorted(rows)).encode()).hexdigest())
            for day, rows in sorted(days.items())]


def timestamp_ms(value):
    if not isinstance(value, str):
        raise ValueError("invalid_timestamp")
    stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("invalid_timestamp")
    return int(stamp.timestamp() * 1000)


def now_ms():
    return time.time_ns() // 1_000_000


class DeliveryError(Exception):
    """Only fixed diagnostic codes, never response bodies, credentials or URLs."""


def validate_endpoint(endpoint):
    parts = urllib.parse.urlsplit(endpoint)
    if (parts.scheme != "https" and not (parts.scheme == "http" and parts.hostname in ("127.0.0.1", "::1", "localhost"))) or not parts.hostname:
        raise ValueError("https_endpoint_required")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("invalid_endpoint")
    return endpoint


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("invalid_config")
    for key in ("codex_home", "machine", "collector_id", "endpoint", "token"):
        value = config.get(key)
        if not isinstance(value, str) or not value.strip() or any(c in value for c in "\r\n"):
            raise ValueError("invalid_config")
    if type(config.get("baseline_ms")) is not int or config["baseline_ms"] < 0:
        raise ValueError("invalid_baseline")
    adopted = config.get("adopt_sessions", [])
    if not isinstance(adopted, list) or any(not isinstance(s, str) or not s for s in adopted):
        raise ValueError("invalid_adoption")
    history = config.get("history_since", {})
    if not isinstance(history, dict) or any(s not in adopted or type(value) is not int or not 0 <= value <= 9007199254740991 for s, value in history.items()):
        raise ValueError("invalid_history_since")
    if "backfill_since" in config:
        history_cutoff(config["backfill_since"])
    validate_endpoint(config["endpoint"])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise DeliveryError("redirect_rejected")


class Collector:
    def __init__(self, config, state_dir):
        validate_config(config)
        self.config = dict(config)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.state_dir / "ledger.sqlite", timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS files(
                path TEXT PRIMARY KEY, offset INTEGER NOT NULL DEFAULT 0,
                prefix_hash TEXT NOT NULL DEFAULT '', mtime_ns INTEGER NOT NULL DEFAULT 0,
                size INTEGER NOT NULL DEFAULT 0, context TEXT NOT NULL DEFAULT '{}',
                session TEXT, eligible INTEGER NOT NULL DEFAULT 0,
                complete INTEGER NOT NULL DEFAULT 0, error TEXT);
            CREATE TABLE IF NOT EXISTS events(
                response_id TEXT PRIMARY KEY, session TEXT NOT NULL, day TEXT NOT NULL,
                digest TEXT NOT NULL, data TEXT NOT NULL, total_tokens INTEGER NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS pending_events ON events(acked, response_id);
        """)
        identity = compact({k: config[k] for k in ("machine", "collector_id", "baseline_ms")})
        previous = self._get("identity")
        if previous is not None and previous != identity:
            self.close()
            raise ValueError("ledger_identity_mismatch")
        with self.db:
            self._set("identity", identity)

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def _get(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def _set(self, key, value):
        self.db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def status(self):
        row = self.db.execute("SELECT count(*) ledger_events, coalesce(sum(CASE WHEN acked=0 THEN 1 ELSE 0 END),0) pending_events, coalesce(sum(CASE WHEN acked=0 THEN total_tokens ELSE 0 END),0) pending_tokens FROM events").fetchone()
        result = dict(row)
        result.update(machine=self.config["machine"], collector_id=self.config["collector_id"],
                      baseline_ms=self.config["baseline_ms"],
                      scanned_at_ms=int(self._get("scanned_at_ms", 0)),
                      synced_at_ms=int(self._get("synced_at_ms", 0)))
        result["excluded_files"] = self.db.execute("SELECT count(*) FROM files WHERE eligible=0 AND session IS NOT NULL").fetchone()[0]
        result["incomplete_files"] = self.db.execute("SELECT count(*) FROM files WHERE eligible=1 AND complete=0").fetchone()[0]
        error = self.db.execute("SELECT error FROM files WHERE error IS NOT NULL ORDER BY path LIMIT 1").fetchone()
        result["last_error"] = self._get("delivery_error") or self._get("scan_error") or (error[0] if error else None)
        runtime_error = self.state_dir / "runtime-error.txt"
        if runtime_error.exists():
            code = runtime_error.read_text(encoding="utf-8")
            result["last_error"] = code if code in ("storage_error", "runtime_io_error", "configuration_error") else "runtime_error"
        if self.config.get("backfill_since"):
            history = json.loads(self._get("backfill_state", "{}"))
            same_window = history.get("since") == self.config["backfill_since"]
            report = self.state_dir / "backfill-report.json"
            result.update(backfill_since=self.config["backfill_since"],
                          backfill_pending=not same_window or history.get("pending", True),
                          backfill_error=history.get("last_error") if same_window else None,
                          backfill_excluded_sessions=history.get("excluded_sessions", 0) if same_window else 0,
                          backfill_report=str(report) if report.exists() else None)
        return result

    def _eligible(self, session, created_ms):
        return session in self.config.get("adopt_sessions", []) or created_ms >= self.config["baseline_ms"]

    def scan(self):
        home = Path(self.config["codex_home"])
        seen = set()
        try:
            if not (home / "sessions").is_dir():
                raise OSError("sessions_missing")
            paths = list((home / "sessions").rglob("*.jsonl"))
            if (home / "archived_sessions").is_dir():
                paths.extend((home / "archived_sessions").rglob("*.jsonl"))
            for path in sorted(paths):
                key = str(path.resolve())
                seen.add(key)
                try:
                    self._scan_file(path, key)
                except OSError:
                    with self.db:
                        self.db.execute("INSERT OR IGNORE INTO files(path) VALUES(?)", (key,))
                        self.db.execute("UPDATE files SET error='file_read_error',complete=0 WHERE path=?", (key,))
            with self.db:
                for row in self.db.execute("SELECT path,session FROM files").fetchall():
                    if row["path"] not in seen:
                        # Moving a journal to archived_sessions does not remove its history.
                        replacement = self.db.execute("SELECT path FROM files WHERE session=? AND complete=1", (row["session"],)).fetchall()
                        if any(r[0] in seen for r in replacement):
                            self.db.execute("DELETE FROM files WHERE path=?", (row["path"],))
                        else:
                            self.db.execute("UPDATE files SET error='file_missing',complete=0 WHERE path=?", (row["path"],))
                self._set("scan_error", "")
                self._set("scanned_at_ms", now_ms())
        except OSError:
            with self.db:
                self._set("scan_error", "scan_error")

    def _scan_file(self, path, key):
        stat = path.stat()
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO files(path) VALUES(?)", (key,))
            state = dict(self.db.execute("SELECT * FROM files WHERE path=?", (key,)).fetchone())
            context = json.loads(state["context"])
            admitted = bool(context and self._eligible(context["session"], context["created_ms"]))
            if state["mtime_ns"] == stat.st_mtime_ns and state["size"] == stat.st_size and not state["error"] and (state["complete"] or not admitted) and bool(state["eligible"]) == admitted:
                return
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                # Recheck the committed prefix only when a file changes. Rewrites and
                # truncation restart parsing while the immutable event ledger remains.
                remaining = state["offset"]
                while remaining:
                    chunk = stream.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    digest.update(chunk)
                    remaining -= len(chunk)
                if remaining or (state["offset"] and digest.hexdigest() != state["prefix_hash"]):
                    stream.seek(0)
                    digest = hashlib.sha256()
                    context = {}
                    state.update(offset=0, eligible=0, session=None)
                    self._set("last_rewind_at_ms", now_ms())
                elif not state["eligible"] and admitted:
                    stream.seek(0)
                    digest = hashlib.sha256()
                    context = {}
                    state.update(offset=0)
                state.update(error=None, complete=0)
                while True:
                    if context and not state["eligible"]:
                        break
                    line = stream.readline()
                    if not line:
                        state["complete"] = int(bool(state["eligible"]))
                        break
                    if not line.endswith(b"\n"):
                        break
                    try:
                        record = json.loads(line)
                        if not isinstance(record, dict):
                            raise ValueError("invalid_record")
                        payload = record.get("payload")
                        if not isinstance(payload, dict):
                            raise ValueError("invalid_payload")
                        kind = record.get("type")
                        if kind == "session_meta":
                            session = payload.get("id")
                            if not isinstance(session, str) or not session:
                                raise ValueError("invalid_session")
                            if context and context["session"] != session:
                                raise DeliveryError("session_conflict")
                            context = dict(session=session, created_ms=timestamp_ms(payload.get("timestamp", record.get("timestamp"))),
                                           client_version=payload.get("cli_version"), model=None, effort=None, service_tier=None)
                            state.update(session=session, eligible=int(self._eligible(session, context["created_ms"])))
                        elif not context:
                            raise DeliveryError("missing_session_meta")
                        elif kind == "turn_context":
                            context.update(model=payload.get("model"), effort=payload.get("effort"), service_tier=payload.get("service_tier"))
                        elif kind == "event_msg" and payload.get("type") == "token_count" and payload.get("info"):
                            since_ms = self.config.get("history_since", {}).get(context["session"], 0)
                            info = payload["info"]
                            last = info.get("last_token_usage") if isinstance(info, dict) else None
                            if since_ms and timestamp_ms(record.get("timestamp")) < since_ms:
                                pass  # Legacy prefixes outside the adopted window are not usage.
                            elif not isinstance(last, dict) or any(type(last.get(k)) is not int or last[k] < 0 for k in ("input_tokens", "output_tokens")):
                                total = info.get("total_token_usage", {}) if isinstance(info, dict) else {}
                                if not context.get("last_usage") or (isinstance(total, dict) and any(type(v) is int and v > 0 for v in total.values())):
                                    raise DeliveryError("unsupported_format")
                            elif last["input_tokens"] or last["output_tokens"]:
                                if {k: last.get(k) for k in TOKEN_KEYS} != context.get("last_usage"):
                                    raise DeliveryError("unsupported_format")
                            # Zero-input/output totals can be compaction estimates.
                        elif kind == "token_usage_record":
                            # session_id can identify an API/subagent session; thread_id
                            # is the journal's stable session_meta.id.
                            event_ms = timestamp_ms(record.get("timestamp"))
                            since_ms = self.config.get("history_since", {}).get(context["session"], 0)
                            counters = payload.get("usage")
                            if event_ms < since_ms:
                                if isinstance(counters, dict):
                                    context["last_usage"] = {k: counters.get(k) for k in TOKEN_KEYS}
                            elif payload.get("thread_id", context["session"]) != context["session"]:
                                raise DeliveryError("session_conflict")
                            elif not isinstance(counters, dict):
                                raise ValueError("invalid_usage")
                            else:
                                event = canonical_event(dict(
                                    **{k: counters.get(k) for k in TOKEN_KEYS},
                                    **{k: context.get(k) for k in ("session", "client_version", "model", "effort", "service_tier")},
                                    response_id=payload.get("response_id"), timestamp_ms=event_ms,
                                    machine=self.config["machine"], collector_id=self.config["collector_id"]))
                                previous = self.db.execute("SELECT digest FROM events WHERE response_id=?", (event["response_id"],)).fetchone()
                                event_hash = event_digest(event)
                                if previous and previous[0] != event_hash:
                                    raise DeliveryError("response_conflict")
                                self.db.execute("INSERT OR IGNORE INTO events(response_id,session,day,digest,data,total_tokens) VALUES(?,?,?,?,?,?)",
                                                (event["response_id"], event["session"], event["event_timestamp"][:10], event_hash, compact(event), event["total_tokens"]))
                                context["last_usage"] = {k: event[k] for k in TOKEN_KEYS}
                    except DeliveryError as exc:
                        state["error"] = str(exc)
                        break
                    except (ValueError, TypeError, OverflowError):
                        state["error"] = "parse_error"
                        break
                    digest.update(line)
                    state["offset"] = stream.tell()
                    if not state["eligible"]:
                        break
                state.update(prefix_hash=digest.hexdigest(), mtime_ns=stat.st_mtime_ns,
                             size=stat.st_size, context=compact(context))
                self.db.execute("UPDATE files SET offset=:offset,prefix_hash=:prefix_hash,mtime_ns=:mtime_ns,size=:size,context=:context,session=:session,eligible=:eligible,complete=:complete,error=:error WHERE path=:path", state)

    def _post(self, action, **body):
        request_body = dict(version=1, action=action, machine=self.config["machine"], collector_id=self.config["collector_id"], **body)
        proxy = self.config.get("proxy")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({"https": proxy, "http": proxy} if proxy else {}), NoRedirect())
        request = urllib.request.Request(self.config["endpoint"], compact(request_body).encode(),
                                         {"Authorization": "Bearer " + self.config["token"], "Content-Type": "application/json"})
        try:
            with opener.open(request, timeout=self.config.get("timeout", 15)) as response:
                data = response.read(2 * 1024 * 1024 + 1)
                if len(data) > 2 * 1024 * 1024:
                    raise DeliveryError("invalid_response")
                result = json.loads(data)
                if not isinstance(result, dict) or result.get("version") != 1:
                    raise DeliveryError("invalid_response")
                return result
        except urllib.error.HTTPError as exc:
            raise DeliveryError("HTTP_" + str(exc.code)) from None
        except (urllib.error.URLError, OSError, TimeoutError, http.client.HTTPException):
            raise DeliveryError("network_error") from None
        except (ValueError, UnicodeError):
            raise DeliveryError("invalid_response") from None

    def _send_pending(self):
        while True:
            rows = self.db.execute("SELECT response_id,data FROM events WHERE acked=0 ORDER BY response_id LIMIT 200").fetchall()
            if not rows:
                return
            result = self._post("events", events=[json.loads(r["data"]) for r in rows])
            expected = {r["response_id"] for r in rows}
            accepted = result.get("accepted")
            if not isinstance(accepted, list) or any(not isinstance(v, str) for v in accepted) or set(accepted) != expected or len(accepted) != len(expected):
                raise DeliveryError("invalid_ack")
            with self.db:
                self.db.executemany("UPDATE events SET acked=1 WHERE response_id=?", [(v,) for v in accepted])

    def _inventory(self, mark_resend=True):
        days = day_inventory(json.loads(r[0]) for r in self.db.execute("SELECT data FROM events"))
        current = self.status()
        result = self._post("inventory", days=days, status={k: current[k] for k in ("scanned_at_ms", "pending_events", "pending_tokens", "last_error")})
        if result.get("server_only_days"):
            raise DeliveryError("server_history_missing")
        resend = result.get("resend_days")
        valid = {d["day"] for d in days}
        if not isinstance(resend, list) or any(not isinstance(d, str) or d not in valid for d in resend):
            raise DeliveryError("invalid_inventory")
        if mark_resend:
            with self.db:
                self.db.executemany("UPDATE events SET acked=0 WHERE day=?", [(d,) for d in resend])
        return resend

    def sync(self):
        previous_error = self._get("delivery_error")
        try:
            self._send_pending()
            if self._inventory():
                self._send_pending()
            if not self._get("scan_error"):
                sessions = [r[0] for r in self.db.execute("""
                    SELECT session FROM files WHERE eligible=1 GROUP BY session
                    HAVING min(complete)=1 AND max(error IS NOT NULL)=0
                    AND EXISTS(SELECT 1 FROM events WHERE events.session=files.session)
                    AND NOT EXISTS(SELECT 1 FROM events WHERE events.session=files.session AND acked=0)
                """)]
                windows = {}
                for session in sessions:
                    windows.setdefault(self.config.get("history_since", {}).get(session, 0), []).append(session)
                for since_ms, group in windows.items():
                    for index in range(0, len(group), 200):
                        batch = group[index:index + 200]
                        result = self._post("activate", sessions=batch, since_ms=since_ms)
                        if result.get("activated") != batch:
                            raise DeliveryError("invalid_activation_ack")
            if self._inventory():
                raise DeliveryError("inventory_mismatch")
            with self.db:
                self._set("delivery_error", "")
            if previous_error and self._inventory():
                raise DeliveryError("inventory_mismatch")
            with self.db:
                self._set("synced_at_ms", now_ms())
        except DeliveryError as exc:
            with self.db:
                self._set("delivery_error", str(exc))
            try:
                self._inventory(mark_resend=False)
            except (DeliveryError, sqlite3.Error, OSError, ValueError):
                pass  # Keep the primary failure if its diagnostic cannot be delivered.
        return self.status()

    def once(self):
        config_path = self.state_dir / "config.json"
        if config_path.exists():
            config = read_config(config_path)
            validate_config(config)
            if any(config.get(k) != self.config.get(k) for k in ("machine", "collector_id", "baseline_ms", "codex_home")):
                raise ValueError("ledger_identity_mismatch")
            self.config = config
        (self.state_dir / "runtime-error.txt").unlink(missing_ok=True)
        self.scan()
        result = self.sync()
        if result.get("backfill_pending") and not self._get("delivery_error"):
            restore_install_history(self.state_dir, self.config)
            self.config = read_config(config_path)
            result = self.status()
            result["last_error"] = result["last_error"] or result["backfill_error"]
        return result


def read_config(path):
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def write_config(path, config):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(compact(config))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    os.chmod(path, 0o600)


def history_cutoff(since):
    if not isinstance(since, str) or dt.date.fromisoformat(since).isoformat() != since:
        raise ValueError("invalid_since")
    since_ms = timestamp_ms(since + "T00:00:00Z")
    if since_ms < 0:
        raise ValueError("invalid_since")
    return since_ms


def backup_state(state_dir):
    suffix = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    config_path = state_dir / "config.json"
    if config_path.exists():
        backup = state_dir / ("config.backup-" + suffix + ".json")
        shutil.copyfile(config_path, backup)
        os.chmod(backup, 0o600)
    ledger = state_dir / "ledger.sqlite"
    if ledger.exists():
        backup = state_dir / ("ledger.backup-" + suffix + ".sqlite")
        with contextlib.closing(sqlite3.connect(ledger.as_uri() + "?mode=ro", uri=True)) as source, contextlib.closing(sqlite3.connect(backup)) as destination:
            source.backup(destination)
        os.chmod(backup, 0o600)


def backfill(state_dir, since, apply=False, config=None):
    """Compare owned history in temporary state; persist only an explicit apply."""
    since_ms = history_cutoff(since)
    state_dir = Path(state_dir).resolve()
    config_path = state_dir / "config.json"
    config = dict(config if config is not None else read_config(config_path))
    validate_config(config)
    result = dict(since=since, since_ms=since_ms, applied=False, sessions=[], last_error=None)
    installed = state_dir / "codex_usage.py"
    if apply and installed.exists() and installed.read_bytes() != Path(__file__).read_bytes():
        result["last_error"] = "sender_update_required"
        return result
    home = Path(config["codex_home"])
    if not (home / "sessions").is_dir():
        result["last_error"] = "sessions_missing"
        return result
    paths = sorted(list((home / "sessions").rglob("*.jsonl")) + list((home / "archived_sessions").rglob("*.jsonl")))
    candidates, rows = {}, []
    for path in paths:
        try:
            with path.open("rb") as stream:
                first = json.loads(stream.readline(1024 * 1024))
                meta = first.get("payload", {})
                session = meta.get("id")
                if first.get("type") != "session_meta" or not isinstance(session, str) or not session or len(session) > 256 or any(ord(c) < 32 for c in session):
                    raise ValueError("invalid_session_meta")
                created_ms = timestamp_ms(meta.get("timestamp", first.get("timestamp")))
                # A resumed old session belongs in the window. Only an ended tail
                # can exclude a file; file mtimes and cwd are never provenance.
                size = stream.seek(0, os.SEEK_END)
                start = max(0, size - 65536)
                stream.seek(start)
                tail = stream.read()
                lines = tail.splitlines()[1:] if start else tail.splitlines()
                stamps = []
                for line in reversed(lines):
                    try:
                        stamps.append(timestamp_ms(json.loads(line).get("timestamp")))
                    except (ValueError, TypeError, AttributeError):
                        continue
                if tail.endswith(b"\n") and stamps and max(stamps + [created_ms]) < since_ms:
                    continue
                candidates.setdefault(session, []).append(path)
        except (OSError, ValueError, TypeError, AttributeError):
            rows.append(dict(session=None, file=path.name, status="blocked", reason="invalid_session_meta"))
    result["sessions"] = rows
    adopted = set(config.get("adopt_sessions", []))
    history = config.get("history_since", {})
    probe_config = dict(config, adopt_sessions=sorted(adopted | candidates.keys()),
                        history_since={**history, **{s: history.get(s, 0) if s in adopted else since_ms for s in candidates}})
    with tempfile.TemporaryDirectory(prefix="codex-usage-backfill-") as temporary:
        probe = Collector(probe_config, temporary)
        try:
            for session, session_paths in candidates.items():
                for path in session_paths:
                    try:
                        probe._scan_file(path, str(path.resolve()))
                    except OSError:
                        rows.append(dict(session=session, status="blocked", reason="file_read_error"))
                        break
                else:
                    files = probe.db.execute("SELECT complete,error FROM files WHERE session=?", (session,)).fetchall()
                    reason = next((r["error"] for r in files if r["error"]), None)
                    if not reason and (not files or not all(r["complete"] for r in files)):
                        reason = "incomplete_journal"
                    count, total = probe.db.execute("SELECT count(*),coalesce(sum(total_tokens),0) FROM events WHERE session=? AND day>=?", (session, since)).fetchone()
                    if not reason and not total:
                        reason = "no_usage_in_window"
                    rows.append(dict(session=session, local_events=count, local_tokens=total,
                                     effective_since_ms=probe_config["history_since"][session],
                                     status="blocked" if reason else "pending", reason=reason))
            owners = {}
            sessions = sorted(candidates)
            for index in range(0, len(sessions), 200):
                batch = sessions[index:index + 200]
                inspected = probe._post("inspect", sessions=batch, since_ms=since_ms).get("sessions")
                if not isinstance(inspected, list) or len(inspected) != len(batch):
                    raise DeliveryError("invalid_inspection")
                for owner in inspected:
                    if not isinstance(owner, dict) or owner.get("session") not in batch or owner["session"] in owners:
                        raise DeliveryError("invalid_inspection")
                    if any(not isinstance(owner.get(k), list) or any(not isinstance(v, str) or not v for v in owner[k]) for k in ("machines", "collectors")):
                        raise DeliveryError("invalid_inspection")
                    if any(type(owner.get(k)) is not int or owner[k] < 0 for k in ("events", "total_tokens")) or type(owner.get("before_since")) is not bool:
                        raise DeliveryError("invalid_inspection")
                    if any(k not in owner or (owner[k] is not None and not isinstance(owner[k], str)) for k in ("active_machine", "active_collector")):
                        raise DeliveryError("invalid_inspection")
                    owners[owner["session"]] = owner
            for row in rows:
                owner = owners.get(row["session"])
                if owner is None:
                    continue
                row.update(server_events=owner["events"], server_tokens=owner["total_tokens"],
                           **{k: owner[k] for k in ("machines", "collectors", "active_machine", "active_collector", "before_since")})
                if row["status"] == "blocked":
                    continue
                machines = set(owner["machines"])
                reason = ("unknown_owner" if not machines else "multiple_machines" if len(machines) > 1 else
                          "foreign_machine" if machines != {config["machine"]} else
                          "foreign_collector" if set(owner["collectors"]) - {config["collector_id"]} or owner["active_collector"] not in (None, config["collector_id"]) else
                          "foreign_machine" if owner["active_machine"] not in (None, config["machine"]) else None)
                if reason:
                    row.update(status="blocked", reason=reason)
                elif history.get(row["session"], 0) > since_ms:
                    row.update(status="blocked", reason="history_window_already_limited")
                elif owner["active_collector"] == config["collector_id"] and owner["active_machine"] == config["machine"]:
                    row.update(status="skipped", reason="already_active")
                elif (owner["active_collector"] is None) != (owner["active_machine"] is None):
                    row.update(status="blocked", reason="ambiguous_active_owner")
                elif row["session"] in adopted:
                    row.update(status="skipped", reason="already_adopted")
                elif owner["before_since"]:
                    row.update(status="blocked", reason="server_history_before_since")
                else:
                    row.update(status="safe", reason=None)
        except DeliveryError as exc:
            result["last_error"] = str(exc)
            for row in rows:
                if row["status"] == "pending":
                    row.update(status="blocked", reason="inspection_failed")
            return result
        finally:
            probe.close()
    if not apply:
        return result
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    backup_state(state_dir)
    safe = {r["session"] for r in rows if r["status"] == "safe"}
    config["adopt_sessions"] = sorted(adopted | safe)
    config["history_since"] = {**history, **{s: since_ms for s in safe - adopted}}
    write_config(config_path, config)
    collector = Collector(config, state_dir)
    try:
        established = {r[0] for r in collector.db.execute("SELECT session FROM files WHERE eligible=1")}
        for row in rows:
            if row["status"] == "blocked" or row["session"] not in safe | adopted | established:
                continue
            for path in candidates[row["session"]]:
                collector._scan_file(path, str(path.resolve()))
        with collector.db:
            collector._set("scanned_at_ms", now_ms())
        result.update(collector.sync(), applied=True)
    finally:
        collector.close()
    return result


def restore_install_history(state_dir, config):
    """Persist the initial history task and its report across sender restarts."""
    with contextlib.closing(Collector(config, state_dir)) as collector:
        state = dict(since=config["backfill_since"], pending=True)
        with collector.db:
            collector._set("backfill_state", compact(state))
        result = backfill(state_dir, config["backfill_since"], apply=True, config=config)
        error = (collector._get("delivery_error") or None) if result["applied"] else result["last_error"]
        state.update(pending=not result["applied"] or bool(error), last_error=error,
                     excluded_sessions=sum(r["status"] == "blocked" and r.get("reason") != "no_usage_in_window"
                                           for r in result["sessions"]))
        report = Path(state_dir) / "backfill-report.json"
        result.update(backfill_since=state["since"], backfill_pending=state["pending"],
                      backfill_error=state["last_error"], backfill_excluded_sessions=state["excluded_sessions"],
                      backfill_report=str(report))
        write_config(report, result)
        with collector.db:
            collector._set("backfill_state", compact(state))
    return result


def install(state_dir, codex_home, machine=None, endpoint=None, token=None,
            adopt_sessions=(), autostart=True, proxy=None, backfill_since=None):
    import tomllib
    state_dir, codex_home = Path(state_dir).resolve(), Path(codex_home).resolve()
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    config_path = state_dir / "config.json"
    previous = read_config(config_path) if config_path.exists() else {}
    source = codex_home / "config.toml"
    native = tomllib.loads(source.read_text(encoding="utf-8")) if source.exists() else {}
    otel = native.get("otel", {})
    exporter = otel.get("exporter", {})
    http = exporter.get("otlp-http", {}) if isinstance(exporter, dict) else {}
    inferred_url = http.get("endpoint")
    if inferred_url:
        parsed = urllib.parse.urlsplit(inferred_url)
        inferred_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/v1/usage", "", ""))
    auth = next((v for k, v in http.get("headers", {}).items() if k.lower() == "authorization"), "")
    inferred_token = auth[7:] if isinstance(auth, str) and auth.startswith("Bearer ") else None
    config = dict(previous)
    config.update(version=1, codex_home=str(codex_home),
                  machine=machine or previous.get("machine") or os.environ.get("CODEX_USAGE_MACHINE") or otel.get("environment"),
                  endpoint=endpoint or previous.get("endpoint") or os.environ.get("CODEX_USAGE_ENDPOINT") or inferred_url,
                  token=token or os.environ.get("CODEX_USAGE_TOKEN") or previous.get("token") or inferred_token,
                  collector_id=previous.get("collector_id") or str(uuid.uuid4()),
                  baseline_ms=previous.get("baseline_ms", now_ms()),
                  backfill_since=backfill_since if backfill_since is not None else previous.get("backfill_since", DEFAULT_BACKFILL_SINCE),
                  adopt_sessions=sorted(set(previous.get("adopt_sessions", [])).union(adopt_sessions)),
                  proxy=proxy if proxy is not None else previous.get("proxy"), timeout=15)
    if not config["machine"] or not config["endpoint"]:
        raise ValueError("machine_and_endpoint_required")
    if not config["token"]:
        config["token"] = getpass.getpass("Ingest Bearer token: ")
    if not config["token"] or any(c in config["token"] for c in "\r\n"):
        raise ValueError("invalid_token")
    validate_config(config)
    if previous and (config["machine"] != previous["machine"] or str(codex_home) != previous["codex_home"]):
        raise ValueError("install_identity_change_requires_new_state_directory")
    installed_script = state_dir / "codex_usage.py"
    stopped = installed_script.exists()
    if stopped:
        stop_startup(config["collector_id"])
    startup = "disabled"
    try:
        if Path(__file__).resolve() != installed_script:
            temporary_script = installed_script.with_suffix(".tmp")
            try:
                shutil.copyfile(__file__, temporary_script)
                os.replace(temporary_script, installed_script)
            finally:
                temporary_script.unlink(missing_ok=True)
        backup_state(state_dir)
        write_config(config_path, config)
        history_result = restore_install_history(state_dir, config)
        config = read_config(config_path)
    finally:
        if autostart and config_path.exists() and installed_script.exists():
            startup = install_startup(state_dir, installed_script, config["collector_id"], stop_existing=not stopped)
    result = dict(machine=config["machine"], collector_id=config["collector_id"],
                  baseline_ms=config["baseline_ms"], adopted_sessions=len(config["adopt_sessions"]), autostart=startup)
    result.update(backfill=history_result, backfill_pending=history_result["backfill_pending"],
                  last_error=history_result["last_error"])
    return result


def windows_stop_command(name):
    name = "'" + name.replace("'", "''") + "'"
    return (f"$existing=Get-ScheduledTask -TaskPath '\\' -ErrorAction Stop | Where-Object TaskName -eq {name};"
            "if($existing){Stop-ScheduledTask -InputObject $existing -ErrorAction Stop;"
            "$deadline=(Get-Date).AddSeconds(30);"
            "do{Start-Sleep -Milliseconds 100;"
            f"$running=(Get-ScheduledTask -TaskName {name} -ErrorAction Stop).State -eq 'Running';"
            "}while($running -and (Get-Date) -lt $deadline);"
            "if($running){throw 'sender_stop_timeout';}}")


def stop_startup(collector_id):
    """Stop the registered sender before changing its code or adoption scope."""
    name = "codex-usage-" + collector_id[:8]
    if sys.platform == "win32":
        import base64
        command = "$ErrorActionPreference='Stop';" + windows_stop_command(name)
        subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-EncodedCommand", base64.b64encode(command.encode("utf-16le")).decode()],
                       check=True, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    elif sys.platform == "darwin":
        label = "local." + name
        jobs = subprocess.run(["launchctl", "list"], check=True, capture_output=True, text=True)
        if any(line.split()[-1:] == [label] for line in jobs.stdout.splitlines()):
            subprocess.run(["launchctl", "bootout", "gui/" + str(os.getuid()) + "/" + label], check=True, capture_output=True)
    else:
        unit = name + ".service"
        loaded = subprocess.run(["systemctl", "--user", "show", unit, "--property=LoadState", "--value"], check=True, capture_output=True, text=True)
        if loaded.stdout.strip() != "not-found":
            subprocess.run(["systemctl", "--user", "stop", unit], check=True, capture_output=True)


def install_startup(state_dir, script, collector_id, stop_existing=True):
    name = "codex-usage-" + collector_id[:8]
    python = Path(sys.executable)
    if sys.platform == "win32":
        import base64
        if python.with_name("pythonw.exe").exists():
            python = python.with_name("pythonw.exe")
        quote = lambda value: "'" + str(value).replace("'", "''") + "'"
        arguments = subprocess.list2cmdline([str(script), "run", "--state-dir", str(state_dir)])
        stop = windows_stop_command(name) if stop_existing else ""
        command = (
            "$ErrorActionPreference='Stop';"
            "$u=[System.Security.Principal.WindowsIdentity]::GetCurrent().Name;"
            f"$a=New-ScheduledTaskAction -Execute {quote(python)} -Argument {quote(arguments)};"
            "$t=New-ScheduledTaskTrigger -AtLogOn -User $u;"
            "$p=New-ScheduledTaskPrincipal -UserId $u -LogonType Interactive -RunLevel Limited;"
            "$s=New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1);"
            f"{stop}"
            f"Register-ScheduledTask -TaskName {quote(name)} -Action $a -Trigger $t -Principal $p -Settings $s -Force | Out-Null;"
            f"Start-ScheduledTask -TaskName {quote(name)}"
        )
        subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-EncodedCommand", base64.b64encode(command.encode("utf-16le")).decode()],
                       check=True, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        return "windows_scheduled_task"
    if sys.platform == "darwin":
        label = "local." + name
        path = Path.home() / "Library" / "LaunchAgents" / (label + ".plist")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(plistlib.dumps(dict(Label=label, ProgramArguments=[str(python), str(script), "run", "--state-dir", str(state_dir)], RunAtLoad=True, KeepAlive=True, ThrottleInterval=30)))
        domain = "gui/" + str(os.getuid())
        subprocess.run(["launchctl", "bootout", domain + "/" + label], capture_output=True)
        subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True, capture_output=True)
        return "launchd"
    path = Path.home() / ".config" / "systemd" / "user" / (name + ".service")
    path.parent.mkdir(parents=True, exist_ok=True)
    arguments = " ".join(json.dumps(str(v).replace("%", "%%")) for v in (python, script, "run", "--state-dir", state_dir))
    path.write_text("[Unit]\nDescription=Durable Codex usage sender\n[Service]\nType=simple\nExecStart=" + arguments + "\nRestart=always\nRestartSec=30\n[Install]\nWantedBy=default.target\n", encoding="utf-8")
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, capture_output=True)
    subprocess.run(["systemctl", "--user", "enable", name + ".service"], check=True, capture_output=True)
    subprocess.run(["systemctl", "--user", "restart", name + ".service"], check=True, capture_output=True)
    return "systemd_user"


def main(argv=None):
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("install", "backfill", "run", "once", "status", "doctor"):
        sub = commands.add_parser(command)
        sub.add_argument("--state-dir", type=Path, default=home / "usage-sender")
        if command == "install":
            sub.add_argument("--codex-home", type=Path, default=home)
            sub.add_argument("--machine")
            sub.add_argument("--endpoint")
            sub.add_argument("--proxy")
            sub.add_argument("--adopt-session", action="append", default=[])
            sub.add_argument("--backfill-since", help="history start date (default: saved date or 2026-09-05)")
            sub.add_argument("--no-autostart", action="store_true")
        elif command == "backfill":
            sub.add_argument("--since", required=True)
            sub.add_argument("--apply", action="store_true")
        elif command == "run":
            sub.add_argument("--interval", type=float, default=30)
    args = parser.parse_args(argv)
    collector = None
    try:
        if args.command == "install":
            if sys.version_info < (3, 11):
                raise ValueError("client_requires_python_3_11")
            result = install(args.state_dir, args.codex_home, args.machine, args.endpoint,
                             adopt_sessions=args.adopt_session, autostart=not args.no_autostart, proxy=args.proxy,
                             backfill_since=args.backfill_since)
        elif args.command == "backfill":
            result = backfill(args.state_dir, args.since, apply=args.apply)
        else:
            collector = Collector(read_config(args.state_dir / "config.json"), args.state_dir)
            if args.command == "run":
                delay = max(1, args.interval)
                while True:
                    try:
                        result = collector.once()
                    except (sqlite3.Error, OSError, ValueError) as exc:
                        code = "storage_error" if isinstance(exc, sqlite3.Error) else ("configuration_error" if isinstance(exc, ValueError) else "runtime_io_error")
                        result = {"last_error": code}
                        try:
                            (args.state_dir / "runtime-error.txt").write_text(code, encoding="utf-8")
                        except OSError:
                            pass
                        if sys.stderr is not None:
                            print(json.dumps({"error": code}), file=sys.stderr, flush=True)
                    delay = min(300, max(args.interval, delay * 2)) if result["last_error"] else max(1, args.interval)
                    time.sleep(delay + random.uniform(0, min(5, delay / 10)))
            elif args.command == "once":
                result = collector.once()
            else:
                result = collector.status()
                if args.command == "doctor":
                    result.update(database_check=collector.db.execute("PRAGMA quick_check").fetchone()[0],
                                  sessions_directory_exists=(Path(collector.config["codex_home"]) / "sessions").is_dir(),
                                  transport="explicit_proxy" if collector.config.get("proxy") else "direct",
                                  endpoint_scheme=urllib.parse.urlsplit(collector.config["endpoint"]).scheme)
        print(json.dumps(result, indent=2))
        return 1 if result.get("last_error") or result.get("backfill_pending") else 0
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError):
        # Exceptions can include TOML values, SQL payloads, command lines or tokens.
        print(json.dumps({"error": "startup_or_configuration_error"}), file=sys.stderr)
        return 1
    finally:
        if collector is not None:
            collector.close()


if __name__ == "__main__":
    sys.exit(main())
