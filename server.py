#!/usr/bin/env python3
"""Loopback-only central receiver for Codex OTLP JSON; persists only usage metadata."""
import argparse
import csv
import io
import hashlib
import hmac
import json
import sqlite3
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
import analytics
import pricing

ROOT = Path(__file__).resolve().parent
LOCK = threading.Lock()
TOKEN_FIELDS = {"input_tokens": "input_token_count", "output_tokens": "output_token_count", "cached_input_tokens": "cached_token_count", "reasoning_output_tokens": "reasoning_token_count", "cache_write_input_tokens": "cache_write_token_count", "tool_tokens": "tool_token_count"}
SAFE = set(TOKEN_FIELDS.values()) | {"event.name", "event.kind", "event.timestamp", "env", "host.name", "model", "conversation.id", "service.name", "app.version", "originator", "startup.phase", "model_reasoning_effort", "reasoning_effort", "service_tier"}

def unpack(value):
    if not isinstance(value, dict):
        return None
    for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if key in value:
            return value[key]
    return None

def attrs(items):
    return {v["key"]: unpack(v.get("value")) for v in (items or [])}

def records(payload):
    for resource in payload.get("resourceLogs", []):
        ra = attrs(resource.get("resource", {}).get("attributes"))
        for scope in resource.get("scopeLogs", []):
            for record in scope.get("logRecords", []):
                yield record, {**ra, **attrs(record.get("attributes"))}

def number(value):
    if value is None:
        return None
    if isinstance(value, bool) or not str(value).isdigit():
        raise ValueError("invalid token count")
    result = int(value)
    if result > 2**53 - 1:
        raise ValueError("token count exceeds exact JSON range")
    return result

def normalize(record, values):
    if values.get("event.name") != "codex.sse_event" or values.get("event.kind") != "response.completed":
        return None
    machine = values.get("env")
    if not machine or machine == "dev":
        machine = values.get("host.name")
    if not machine:
        raise ValueError("missing machine")
    timestamp = values.get("event.timestamp")
    if timestamp:
        when = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if when.tzinfo is None:
            raise ValueError("timestamp has no timezone")
        millis = int(when.timestamp() * 1000)
    else:
        ns = number(record.get("timeUnixNano"))
        if not ns:
            raise ValueError("missing event timestamp")
        millis = ns // 1_000_000
        timestamp = str(ns)
    tokens = {key: number(values.get(field)) for key, field in TOKEN_FIELDS.items()}
    if tokens["input_tokens"] is None or tokens["output_tokens"] is None:
        raise ValueError("missing input/output counts")
    for subset, whole in (("cached_input_tokens", "input_tokens"), ("reasoning_output_tokens", "output_tokens")):
        if tokens[subset] is not None and tokens[subset] > tokens[whole]:
            raise ValueError("inconsistent token subset")
    session = values.get("conversation.id")
    if not session:
        raise ValueError("missing conversation identity")
    # No response_id in observed export. This removes identical event deliveries,
    # not arbitrary copied sessions. Counts preserve distinct differing events.
    identity = [machine, session, timestamp, values.get("model"), tokens]
    event_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return {"effort": analytics.label(values.get("model_reasoning_effort", values.get("reasoning_effort"))), "service_tier": analytics.label(values.get("service_tier")), "client_version": values.get("app.version"), "id": event_id, "machine": machine, "host": values.get("host.name"), "timestamp_ms": millis, "event_timestamp": timestamp, "model": values.get("model") or "unknown", "session": session, "client": values.get("service.name"), **tokens, "total_tokens": tokens["input_tokens"] + tokens["output_tokens"]}

class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute("CREATE TABLE IF NOT EXISTS usage (id TEXT PRIMARY KEY, timestamp_ms INTEGER NOT NULL, body TEXT NOT NULL)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS usage_timestamp ON usage(timestamp_ms)")
        self.connection.execute("CREATE TABLE IF NOT EXISTS machine_activity (machine TEXT PRIMARY KEY, host TEXT, client TEXT, version TEXT, first_event_ms INTEGER, last_event_ms INTEGER, last_received_ms INTEGER)")
        self.connection.execute("CREATE TABLE IF NOT EXISTS machine_preferences (machine TEXT PRIMARY KEY, hidden INTEGER NOT NULL CHECK(hidden IN (0,1)))")
        # Legacy usage remains untouched. Its event timestamp is not a receipt timestamp.
        self.connection.execute("""INSERT OR IGNORE INTO machine_activity
            SELECT json_extract(body, '$.machine'), MAX(json_extract(body, '$.host')),
                MAX(json_extract(body, '$.client')), NULL, MIN(timestamp_ms), MAX(timestamp_ms), NULL
            FROM usage GROUP BY json_extract(body, '$.machine')""")
        self.connection.commit()
        self.diagnostics = {"batches": 0, "records": 0, "duplicates": 0, "invalid_usage": 0, "schemas": {}, "last_received_ms": None}

    def ingest(self, payload):
        added = 0
        with LOCK, self.connection:
            self.diagnostics["batches"] += 1
            self.diagnostics["last_received_ms"] = int(time.time() * 1000)
            for record, values in records(payload):
                self.diagnostics["records"] += 1
                machine = values.get("env")
                if not machine or machine == "dev":
                    machine = values.get("host.name")
                if isinstance(machine, str) and 0 < len(machine) <= 240 and str(values.get("event.name", "")).startswith("codex."):
                    received = int(time.time() * 1000)
                    try:
                        event_ms = int(datetime.fromisoformat(str(values.get("event.timestamp", "")).replace("Z", "+00:00")).timestamp() * 1000)
                    except (ValueError, TypeError, OverflowError):
                        event_ms = None
                    self.connection.execute("""INSERT INTO machine_activity VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(machine) DO UPDATE SET host=COALESCE(excluded.host,host),
                        client=COALESCE(excluded.client,client), version=COALESCE(excluded.version,version),
                        first_event_ms=MIN(COALESCE(first_event_ms,excluded.first_event_ms),COALESCE(excluded.first_event_ms,first_event_ms)),
                        last_event_ms=MAX(COALESCE(last_event_ms,excluded.last_event_ms),COALESCE(excluded.last_event_ms,last_event_ms)), last_received_ms=excluded.last_received_ms""",
                        (machine, values.get("host.name"), values.get("service.name"), values.get("app.version"), event_ms, event_ms, received))
                event = str(values.get("event.name", "unknown"))
                shape = self.diagnostics["schemas"].setdefault(event, {"count": 0, "keys": [], "example": {}})
                shape["count"] += 1
                shape["keys"] = sorted(set(shape["keys"]) | set(values))
                shape["example"] = {k: v for k, v in values.items() if k in SAFE}
                try:
                    item = normalize(record, values)
                except (ValueError, TypeError):
                    self.diagnostics["invalid_usage"] += 1
                    continue
                if item:
                    result = self.connection.execute("INSERT OR IGNORE INTO usage VALUES (?, ?, ?)", (item["id"], item["timestamp_ms"], json.dumps(item)))
                    added += result.rowcount
                    self.diagnostics["duplicates"] += 1 - result.rowcount
            self.connection.commit()
        return added

    def snapshot(self, hours=None):
        with LOCK:
            if hours is None:
                query = self.connection.execute("SELECT body FROM usage ORDER BY timestamp_ms")
            else:
                query = self.connection.execute("SELECT body FROM usage WHERE timestamp_ms >= ? ORDER BY timestamp_ms", (int((time.time()-hours*3600)*1000),))
            events = [json.loads(row[0]) for row in query]
            return {"events": events, "diagnostics": json.loads(json.dumps(self.diagnostics)), "now_ms": int(time.time() * 1000)}


    def read_interval(self, start, end):
        with LOCK:
            events = [json.loads(row[0]) for row in self.connection.execute("SELECT body FROM usage WHERE timestamp_ms >= ? AND timestamp_ms < ? ORDER BY timestamp_ms,id", (max(0, start), end))]
            # GPT-5.5's documented long-context rate applies to its full session,
            # including when the long request falls outside the selected interval.
            long_sessions = set(self.connection.execute("""SELECT DISTINCT json_extract(body,'$.machine'), json_extract(body,'$.session') FROM usage
                WHERE json_extract(body,'$.model') IN ('gpt-5.5','gpt-5.5-2026-04-23') AND json_extract(body,'$.input_tokens') > 272000"""))
        for event in events:
            if (event["machine"], event["session"]) in long_sessions:
                event["api_session_long_context"] = True
        return events

    def set_machine_visibility(self, machine, hidden):
        with LOCK, self.connection:
            if not self.connection.execute("SELECT 1 FROM machine_activity WHERE machine=?", (machine,)).fetchone():
                raise KeyError("unknown machine")
            self.connection.execute("INSERT INTO machine_preferences VALUES (?,?) ON CONFLICT(machine) DO UPDATE SET hidden=excluded.hidden", (machine, int(hidden)))

    def activity(self, now_ms=None):
        now_ms = now_ms or int(time.time() * 1000)
        with LOCK:
            rows = self.connection.execute("SELECT * FROM machine_activity ORDER BY machine").fetchall()
            hidden = {row[0] for row in self.connection.execute("SELECT machine FROM machine_preferences WHERE hidden=1")}
        result = []
        for row in rows:
            item = dict(zip(("machine", "host", "client", "version", "first_event_ms", "last_event_ms", "last_received_ms"), row))
            item["hidden"] = item["machine"] in hidden
            received = item["last_received_ms"]
            item["age_ms"] = max(0, now_ms - received) if received is not None else None
            item["status"] = "recent" if received is not None and now_ms - received <= 120000 else "quiet" if received is not None else "historical"
            result.append(item)
        return result

    def analysis(self, params):
        now = int(time.time() * 1000)
        start, end, filters = analytics.parse_query(params, now)
        events = self.read_interval(start - (end - start), end)
        with LOCK:
            first = self.connection.execute("SELECT MIN(timestamp_ms) FROM usage").fetchone()[0]
        return analytics.report(events, start, end, filters, now, first, self.activity(now))

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def respond(self, status, body, kind="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", kind + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(body)

    def valid_host(self):
        return self.headers.get("Host") in {f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}"}

    def do_POST(self):
        if not self.valid_host():
            return self.respond(403, b'{}')
        if self.path == "/api/v1/machines/visibility":
            # This route is behind dashboard Basic auth in Nginx. The non-simple
            # header + JSON require CORS preflight; cross-origin requests are denied.
            site = self.headers.get("Sec-Fetch-Site")
            if self.headers.get("X-Codex-Usage-Action") != "visibility" or site not in (None, "none", "same-origin") or (self.headers.get("Origin") and site != "same-origin"):
                return self.respond(403, b'{}')
            if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                return self.respond(415, b'{}')
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 2048:
                    return self.respond(413, b'{}')
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict) or set(payload) != {"machine", "hidden"} or not isinstance(payload["machine"], str) or not 0 < len(payload["machine"]) <= 240 or type(payload["hidden"]) is not bool:
                    return self.respond(400, b'{}')
                self.server.store.set_machine_visibility(payload["machine"], payload["hidden"])
            except KeyError:
                return self.respond(404, b'{}')
            except (ValueError, TypeError):
                return self.respond(400, b'{}')
            return self.respond(200, b'{"ok":true}')
        if self.headers.get("Origin"):
            return self.respond(403, b'{}')
        if self.path != "/v1/logs":
            return self.respond(404, b'{}')
        token = getattr(self.server, "ingest_token", None)
        if token and not hmac.compare_digest(self.headers.get("Authorization", "").encode(), ("Bearer " + token).encode()):
            return self.respond(401, b'{}')
        if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            return self.respond(415, b'{}')
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size <= 0 or size > 8 * 1024 * 1024:
                return self.respond(413, b'{}')
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict) or not isinstance(payload.get("resourceLogs"), list):
                return self.respond(400, b'{}')
            self.server.store.ingest(payload)
        except (ValueError, TypeError, KeyError, AttributeError):
            return self.respond(400, b'{}')
        self.respond(200, b'{}')

    def do_GET(self):
        if not self.valid_host():
            return self.respond(403, b'{}')
        path = urlsplit(self.path).path
        params = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        if path == "/api/v1/schema":
            return self.respond(200, json.dumps({
                "schema_version": analytics.VERSION,
                "authentication": "HTTPS Basic auth with dashboard credentials; read-only GET endpoints. Ingest credential is separate.",
                "endpoints": {"/api/v1/analytics": "Totals, comparison, timeline, breakdowns, top 100 sessions, recent events, facets, activity", "/api/v1/events": "Filtered events, stable id cursor; limit 1..1000", "/api/v1/export.csv": "Filtered observed events as CSV; maximum 31 days", "/api/v1/machines": "Persistent host list and latest telemetry receipt"},
                "query": {"hours": "1..744; default 720. Rolling interval.", "start,end": "Unix milliseconds, half-open [start,end), max 31 days. Mutually exclusive with hours.", "machine,model,effort,tier,session": "Exact-match optional filters; unknown selects missing effort/tier", "zero_output": "include (default) or exclude", "timezone": "IANA name for daily buckets; default UTC", "events_pagination": "Fix start/end for stable pagination; pass returned cursor; do not change filters between pages"},
                "semantics": analytics.SEMANTICS, "pricing": {**pricing.CONTRACT, "rates_per_million": pricing.RATES},
                "machine_visibility": "POST /api/v1/machines/visibility with JSON {machine,hidden} and X-Codex-Usage-Action: visibility. Dashboard credentials; reversible card preference only; usage retained.",
            }).encode())
        if path == "/api/v1/machines":
            return self.respond(200, json.dumps({"schema_version": analytics.VERSION, "generated_at_ms": int(time.time()*1000), "machines": self.server.store.activity(), "semantics": analytics.SEMANTICS["activity"]}).encode())
        if path == "/api/v1/analytics":
            try:
                if "cursor" in params or "limit" in params:
                    raise ValueError("pagination applies to events only")
                data = self.server.store.analysis(params)
                data["deployment_label"] = self.server.deployment_label
            except (ValueError, TypeError, OverflowError):
                return self.respond(400, b'{"error":"Invalid filters or period; see /api/v1/schema"}')
            return self.respond(200, json.dumps(data).encode())
        if path in ("/api/v1/events", "/api/v1/export.csv"):
            try:
                now = int(time.time()*1000)
                start, end, filters = analytics.parse_query(params, now)
                events = [analytics.enrich(e) for e in self.server.store.read_interval(start, end)]
                events = [e for e in events if analytics.matches(e, filters)]
                if path.endswith(".csv"):
                    if "cursor" in params or "limit" in params:
                        raise ValueError("pagination applies to events only")
                    columns = ["id", "timestamp_ms", "machine", "model", "session", "effort", "service_tier", "tier", *analytics.METRICS, "cache_write_input_tokens", *pricing.FIELDS, "api_price_tier", "api_price_assumed_tier", "api_price_context", "api_price_reason"]
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
                    writer.writeheader()
                    # Prevent spreadsheet formula execution from arbitrary host names or labels.
                    for event in events:
                        writer.writerow({k: ("'"+v if isinstance(v, str) and v.startswith(("=", "+", "-", "@", "\t", "\r")) else v) for k,v in event.items()})
                    return self.respond(200, output.getvalue().encode(), "text/csv")
                if any(len(params.get(key, [""])) != 1 for key in ("limit", "cursor")):
                    raise ValueError("repeated pagination parameter")
                limit = int(params.get("limit", ["200"])[0])
                if not 1 <= limit <= 1000:
                    raise ValueError("invalid limit")
                cursor = params.get("cursor", [None])[0]
                if cursor:
                    position = next((i for i,e in enumerate(events) if e["id"] == cursor), None)
                    if position is None:
                        raise ValueError("cursor not in this slice")
                    events = events[position+1:]
                page = events[:limit]
                return self.respond(200, json.dumps({"schema_version": analytics.VERSION, "generated_at_ms": now, "period": {"start_ms": start, "end_ms": end}, "filters": filters, "events": page, "next_cursor": page[-1]["id"] if len(events)>limit else None}).encode())
            except (ValueError, TypeError, OverflowError):
                return self.respond(400, b'{"error":"Invalid filters, interval or cursor"}')
        if path == "/api/usage":
            try:
                hours = int(parse_qs(urlsplit(self.path).query).get("hours", ["24"])[0])
                if not 1 <= hours <= 744:
                    raise ValueError("unsupported period")
            except (ValueError, IndexError):
                return self.respond(400, b'{}')
            data = self.server.store.snapshot(hours)
            data.update(central=self.server.central, deployment_label=self.server.deployment_label)
            return self.respond(200, json.dumps(data).encode())
        if self.path == "/health":
            return self.respond(200, b'{"ok":true}')
        if self.path == "/favicon.ico":
            return self.respond(204, b'')
        if path == "/":
            return self.respond(200, (ROOT / "index.html").read_bytes(), "text/html")
        assets = {"/dashboard.js": "text/javascript", "/styles.css": "text/css", "/agent-guide.md": "text/markdown"}
        if path in assets:
            return self.respond(200, (ROOT / path.lstrip("/")).read_bytes(), assets[path])
        self.respond(404, b'{}')

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8768)
    p.add_argument("--data-dir", type=Path, default=ROOT / "data")
    p.add_argument("--ingest-token-file", type=Path)
    p.add_argument("--central", action="store_true")
    p.add_argument("--label", default="Контрольная проба · только полученные события")
    args = p.parse_args()
    if args.central and not args.ingest_token_file:
        p.error("central mode requires --ingest-token-file; protect dashboard with reverse proxy authentication")
    token = args.ingest_token_file.read_text().strip() if args.ingest_token_file else None
    if token is not None and len(token) < 32:
        p.error("ingest token must have at least 32 characters")
    args.data_dir.mkdir(mode=0o700, exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.store = Store(args.data_dir / "usage.sqlite")
    server.ingest_token = token
    server.central = args.central
    server.deployment_label = args.label
    print(f"Codex usage: http://127.0.0.1:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        server.store.connection.close()

if __name__ == "__main__":
    main()
