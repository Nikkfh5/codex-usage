import contextlib
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

try:
    import codex_usage as usage
except ImportError:
    usage = None


def event(response="resp-1", **changes):
    result = dict(response_id=response, machine="test-machine", collector_id="collector-1",
                  session="session-new", timestamp_ms=1789603201000, model="gpt-6-astra",
                  client_version="0.153.4", input_tokens=100, cached_input_tokens=80,
                  cache_write_input_tokens=0, output_tokens=20, reasoning_output_tokens=10,
                  total_tokens=120, effort="high", service_tier=None)
    result.update(changes)
    return result


def journal(session="session-new", created="2026-09-17T00:00:00Z", response="resp-1"):
    return [
        dict(timestamp=created, type="session_meta", payload=dict(id=session,
             timestamp=created, cli_version="0.153.4")),
        dict(timestamp=created, type="turn_context", payload=dict(model="gpt-6-astra", effort="high")),
        dict(timestamp="2026-09-17T00:00:01Z", type="token_usage_record",
             payload=dict(response_id=response, session_id=session, usage={
                 k: v for k, v in event().items() if k.endswith("_tokens")})),
    ]


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        state = self.server.state
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        state["requests"].append(body)
        status = state.get("action_status", {}).get(body["action"], state.get("status", 200))
        if body["action"] == "events" and state.get("event_statuses"):
            status = state["event_statuses"].pop(0)
        if self.headers.get("Authorization") != "Bearer sentinel-secret":
            status = 401
        response = {"version": 1}
        if status == 200:
            if body["action"] == "events":
                for record in body["events"]:
                    state["events"][record["response_id"]] = usage.canonical_event(record)
                response["accepted"] = [r["response_id"] for r in body["events"]]
                if state.pop("lose_ack", False):
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
            elif body["action"] == "inventory":
                server_days = {d["day"]: d for d in usage.day_inventory(state["events"].values())}
                response["resend_days"] = [d["day"] for d in body["days"] if server_days.get(d["day"]) != d]
                response["server_only_days"] = sorted(set(server_days) - {d["day"] for d in body["days"]})
                response["active_sessions"] = sorted(state["active"])
            elif body["action"] == "activate":
                state["active"].update(body["sessions"])
                response["activated"] = body["sessions"]
            elif body["action"] == "inspect":
                response["sessions"] = [{"session": session, "machines": [], "collectors": [],
                    "active_collector": None, "active_machine": None, "before_since": False,
                    "events": 0, "total_tokens": 0, **state.get("ownership", {}).get(session, {})}
                    for session in body["sessions"]]
        else:
            response["error"] = "sentinel-secret raw server message"
        data = json.dumps(response).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class SenderTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(usage, "durable sender module must exist")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / "codex"
        (self.home / "sessions").mkdir(parents=True)
        self.state = dict(events={}, active=set(), requests=[])
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        self.server.state = self.state
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.config = dict(version=1, codex_home=str(self.home), machine="test-machine",
                           collector_id="collector-1", baseline_ms=1789516800000,
                           endpoint=f"http://127.0.0.1:{self.server.server_port}/v1/usage",
                           token="sentinel-secret", adopt_sessions=[], timeout=2)
        self.sender = self.make_sender()

    def make_sender(self):
        sender = usage.Collector(self.config, self.root / "state")
        self.addCleanup(sender.close)
        return sender

    def write(self, records=None, name="rollout.jsonl"):
        path = self.home / "sessions" / name
        path.write_text("".join(json.dumps(r) + "\n" for r in (records or journal())), encoding="utf-8")
        return path

    def test_canonical_validation_and_stable_daily_digest(self):
        record = usage.canonical_event(event(prompt="sentinel-secret"))
        self.assertNotIn("prompt", record)
        self.assertEqual(record["id"], hashlib.sha256(b"journal:resp-1").hexdigest())
        self.assertEqual(record["event_timestamp"], "2026-09-17T00:00:01.000Z")
        self.assertIsNone(record["service_tier"])
        self.assertIsNone(usage.canonical_event(event(cached_input_tokens=None))["cached_input_tokens"])
        records = [record, usage.canonical_event(event("resp-2"))]
        self.assertEqual(usage.day_inventory(records), usage.day_inventory(reversed(records)))
        for bad in [dict(total_tokens=999), dict(cached_input_tokens=101),
                    dict(input_tokens=True), dict(reasoning_output_tokens=-1), dict(response_id="")]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                usage.canonical_event(event(**bad))

    def test_rejects_json_inexact_large_integers(self):
        with self.assertRaises(ValueError):
            usage.canonical_event(event(input_tokens=2**53, cached_input_tokens=0,
                                        output_tokens=0, reasoning_output_tokens=0, total_tokens=2**53))

    def test_server_history_missing_from_local_ledger_is_not_synced(self):
        self.state["events"]["lost-local"] = usage.canonical_event(event("lost-local", timestamp_ms=1789516800000))
        result = self.sender.once()
        self.assertEqual(result["last_error"], "server_history_missing")
        self.assertEqual(result["synced_at_ms"], 0)

    def test_runtime_storage_errors_survive_status_and_recover(self):
        (self.root / "state" / "config.json").write_text(json.dumps(self.config))
        error_output = io.StringIO()
        with mock.patch.object(usage.Collector, "once", side_effect=[__import__("sqlite3").OperationalError("sentinel-secret"), KeyboardInterrupt()]), mock.patch.object(usage.time, "sleep"), contextlib.redirect_stderr(error_output):
            self.assertEqual(usage.main(["run", "--state-dir", str(self.root / "state")]), 0)
        self.assertNotIn("sentinel-secret", error_output.getvalue())
        self.assertEqual(self.sender.status()["last_error"], "storage_error")
        self.write()
        self.assertIsNone(self.sender.once()["last_error"])

    def test_new_session_complete_ack_activate_and_idempotent_restart(self):
        self.write()
        result = self.sender.once()
        self.assertEqual(result["pending_events"], 0)
        self.assertEqual(len(self.state["events"]), 1)
        self.assertEqual(self.state["active"], {"session-new"})
        self.sender.close()
        self.sender = self.make_sender()
        self.sender.once()
        self.assertEqual(len(self.state["events"]), 1)
        self.assertEqual(self.sender.status()["ledger_events"], 1)

    def test_partial_line_survives_restart_without_early_activation(self):
        records = journal()
        path = self.write(records[:2])
        encoded = (json.dumps(records[2]) + "\n").encode()
        with path.open("ab") as f:
            f.write(encoded[:60])
        self.sender.once()
        self.assertFalse(self.state["active"])
        self.sender.close()
        self.sender = self.make_sender()
        with path.open("ab") as f:
            f.write(encoded[60:])
        self.sender.once()
        self.assertEqual(len(self.state["events"]), 1)
        self.assertEqual(self.state["active"], {"session-new"})

    def test_old_imported_session_requires_exact_adoption(self):
        self.write(journal(session="copied-mac", created="2026-09-01T00:00:00Z"))
        self.sender.once()
        self.assertFalse(self.state["events"])
        self.assertEqual(self.sender.status()["excluded_files"], 1)
        self.config["adopt_sessions"] = ["copied-mac"]
        self.sender.close()
        self.sender = self.make_sender()
        self.sender.once()
        self.assertEqual(self.state["active"], {"copied-mac"})

    def test_running_collector_reloads_adoption_without_identity_change(self):
        self.write(journal(session="copied-mac", created="2026-09-01T00:00:00Z"))
        self.sender.once()
        config_path = self.root / "state" / "config.json"
        config_path.write_text(json.dumps(dict(self.config, adopt_sessions=["copied-mac"])))
        self.sender.once()
        self.assertEqual(self.state["active"], {"copied-mac"})
        config_path.write_text(json.dumps(dict(self.config, machine="some-other-machine")))
        with self.assertRaises(ValueError):
            self.sender.once()

    def test_missing_token_in_config_is_a_safe_configuration_error(self):
        broken = dict(self.config)
        del broken["token"]
        with self.assertRaises(ValueError):
            unexpected = usage.Collector(broken, self.root / "broken-config")
            unexpected.close()

    def test_legacy_token_count_only_format_is_visible_and_not_activated(self):
        records = journal()[:2]
        records.append(dict(type="event_msg", payload=dict(type="token_count", info={"total_token_usage": {"total_tokens": 120}})))
        self.write(records)
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "unsupported_format")
        self.assertEqual(self.sender.status()["ledger_events"], 0)
        self.assertFalse(self.state["active"])

    def test_accumulated_token_counts_do_not_duplicate_response_usage(self):
        records = journal()
        records.extend([dict(type="event_msg", payload=dict(type="token_count", info={"total_token_usage": {"total_tokens": 120}, "last_token_usage": records[-1]["payload"]["usage"]}))] * 3)
        self.write(records)
        self.sender.once()
        self.assertIsNone(self.sender.status()["last_error"])
        self.assertEqual(self.sender.status()["ledger_events"], 1)

    def test_legacy_prefix_blocks_later_modern_session_takeover(self):
        records = journal()
        records.insert(2, dict(type="event_msg", payload=dict(type="token_count", info={
            "last_token_usage": dict(input_tokens=50, output_tokens=10, total_tokens=60),
            "total_token_usage": {"total_tokens": 60}})))
        self.write(records)
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "unsupported_format")
        self.assertFalse(self.state["active"])

    def test_later_legacy_usage_is_reported_after_modern_session_activation(self):
        path = self.write()
        self.sender.once()
        self.assertEqual(self.state["active"], {"session-new"})
        legacy = dict(type="event_msg", payload=dict(type="token_count", info={
            "last_token_usage": dict(input_tokens=51, output_tokens=10, total_tokens=61),
            "total_token_usage": {"total_tokens": 181}}))
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(legacy) + "\n")
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "unsupported_format")
        self.assertEqual(self.sender.status()["ledger_events"], 1)

    def test_compaction_estimate_is_not_a_missing_response(self):
        records = journal()
        records.append(dict(type="event_msg", payload=dict(type="token_count", info={
            "last_token_usage": dict(input_tokens=0, output_tokens=0, total_tokens=500),
            "total_token_usage": {"total_tokens": 620}})))
        self.write(records)
        self.sender.once()
        self.assertIsNone(self.sender.status()["last_error"])
        self.assertEqual(self.sender.status()["ledger_events"], 1)

    def test_appends_to_old_excluded_session_never_claim_its_usage(self):
        path = self.write(journal(session="copied-mac", created="2026-09-01T00:00:00Z"))
        for _ in range(4):
            self.sender.once()
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(dict(type="response_item", payload={"text": "ignored"})) + "\n")
        self.assertFalse(self.state["events"])
        self.assertEqual(self.sender.status()["ledger_events"], 0)

    def test_file_cursor_and_event_insert_commit_together(self):
        self.write()
        self.sender.db.execute("CREATE TRIGGER interrupt_cursor BEFORE UPDATE ON files BEGIN SELECT RAISE(ABORT,'simulated_crash'); END")
        with self.assertRaises(__import__("sqlite3").IntegrityError):
            self.sender.scan()
        self.assertEqual(self.sender.status()["ledger_events"], 0)
        self.sender.db.execute("DROP TRIGGER interrupt_cursor")
        self.sender.close()
        self.sender = self.make_sender()
        self.sender.once()
        self.assertEqual(len(self.state["events"]), 1)

    def test_thread_id_identifies_journal_not_api_session_id(self):
        records = journal()
        records[-1]["payload"].update(thread_id="session-new", session_id="api-session-other")
        self.write(records)
        self.sender.once()
        self.assertEqual(self.sender.status()["pending_events"], 0)
        self.assertIsNone(self.sender.status()["last_error"])
        self.assertEqual(self.state["events"]["resp-1"]["session"], "session-new")

    def test_other_thread_id_is_a_persistent_conflict(self):
        records = journal()
        records[-1]["payload"]["thread_id"] = "other-thread"
        self.write(records)
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "session_conflict")
        self.assertFalse(self.state["events"])

    def test_wire_batch_is_bounded_to_200_events(self):
        records = journal()[:2]
        records.extend(journal(response=f"resp-{i}")[-1] for i in range(405))
        self.write(records)
        self.sender.once()
        batches = [r["events"] for r in self.state["requests"] if r["action"] == "events"]
        self.assertEqual([len(b) for b in batches], [200, 200, 5])
        self.assertEqual(self.sender.status()["pending_events"], 0)

    def test_http_failures_leave_queue_and_sanitize_status(self):
        self.write()
        for code in (401, 429, 500, 409):
            with self.subTest(code=code):
                self.state["status"] = code
                result = self.sender.once()
                self.assertEqual(result["pending_events"], 1)
                self.assertEqual(result["last_error"], f"HTTP_{code}")
                self.assertNotIn("sentinel-secret", json.dumps(result))
        self.state["status"] = 200
        self.assertEqual(self.sender.once()["pending_events"], 0)

    def test_activation_error_is_reported_until_successful_recovery(self):
        self.write()
        self.state["action_status"] = {"activate": 409}
        for _ in range(2):
            self.assertEqual(self.sender.once()["last_error"], "HTTP_409")
            latest = [r for r in self.state["requests"] if r["action"] == "inventory"][-1]
            self.assertEqual(latest["status"]["last_error"], "HTTP_409")
            self.assertEqual(latest["status"]["pending_events"], 0)
        self.state["action_status"].clear()
        self.assertIsNone(self.sender.once()["last_error"])
        self.assertEqual(self.state["active"], {"session-new"})
        self.assertIsNone(self.state["requests"][-1]["status"]["last_error"])

    def test_failed_error_report_does_not_mask_activation_failure(self):
        self.write()
        original_post = self.sender._post
        failed_activation = False

        def fail(action, **body):
            nonlocal failed_activation
            if action == "activate":
                failed_activation = True
                raise usage.DeliveryError("HTTP_409")
            if action == "inventory" and failed_activation:
                raise usage.DeliveryError("HTTP_500")
            return original_post(action, **body)

        with mock.patch.object(self.sender, "_post", side_effect=fail):
            self.assertEqual(self.sender.once()["last_error"], "HTTP_409")

    def test_partial_upload_diagnostic_preserves_prior_batch_ack_after_restart(self):
        records = journal()[:2]
        records.extend(journal(response=f"resp-{i:03d}")[-1] for i in range(405))
        self.write(records)
        self.state["event_statuses"] = [200, 500]
        result = self.sender.once()
        self.assertEqual(result["last_error"], "HTTP_500")
        self.assertEqual(len(self.state["events"]), 200)
        self.assertEqual(result["pending_events"], 205)
        acknowledged = set(self.state["events"])
        next_request = len(self.state["requests"])
        self.sender.close()
        self.sender = self.make_sender()
        self.assertEqual(self.sender.once()["pending_events"], 0)
        retried = {event["response_id"] for request in self.state["requests"][next_request:]
                   if request["action"] == "events" for event in request["events"]}
        self.assertFalse(acknowledged & retried)
        self.assertEqual(len(retried), 205)
        self.assertEqual(len(self.state["events"]), 405)

    def test_lost_ack_replays_identical_event_after_restart(self):
        self.write()
        self.state["lose_ack"] = True
        self.assertEqual(self.sender.once()["pending_events"], 1)
        self.assertEqual(len(self.state["events"]), 1)
        self.sender.close()
        self.sender = self.make_sender()
        self.assertEqual(self.sender.once()["pending_events"], 0)
        batches = [r["events"] for r in self.state["requests"] if r["action"] == "events"]
        self.assertEqual(batches[0], batches[1])

    def test_server_rollback_replays_acknowledged_ledger_and_activation(self):
        self.write()
        self.sender.once()
        self.state["events"].clear()
        self.state["active"].clear()
        self.sender.close()
        self.sender = self.make_sender()
        self.sender.once()
        self.assertEqual(len(self.state["events"]), 1)
        self.assertEqual(self.state["active"], {"session-new"})
        self.assertEqual(self.state["requests"][-1]["status"]["pending_events"], 0)

    def test_malformed_record_persists_error_and_blocks_session_activation(self):
        path = self.write()
        with path.open("a", encoding="utf-8") as f:
            f.write('{"sentinel-secret": invalid}\n')
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "parse_error")
        self.assertFalse(self.state["active"])
        self.sender.close()
        self.sender = self.make_sender()
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "parse_error")

    def test_duplicate_response_mismatch_blocks_cursor_and_activation(self):
        records = journal()
        conflict = json.loads(json.dumps(records[-1]))
        conflict["payload"]["usage"].update(input_tokens=101, total_tokens=121)
        self.write(records + [conflict])
        self.sender.once()
        self.assertEqual(self.sender.status()["last_error"], "response_conflict")
        self.assertFalse(self.state["active"])
        self.assertEqual(self.sender.status()["ledger_events"], 1)

    def test_truncation_rewinds_without_deleting_acknowledged_ledger(self):
        path = self.write()
        self.sender.once()
        path.write_text(json.dumps(journal()[0]) + "\n", encoding="utf-8")
        self.sender.once()
        self.assertEqual(self.sender.status()["ledger_events"], 1)
        with path.open("a", encoding="utf-8") as f:
            f.write("".join(json.dumps(r) + "\n" for r in journal(response="resp-2")[1:]))
        self.sender.once()
        self.assertEqual(set(self.state["events"]), {"resp-1", "resp-2"})

    def test_secret_prompt_and_proxy_environment_never_leave_client(self):
        records = journal()
        records.insert(2, dict(type="response_item", payload={"text": "sentinel-secret prompt"}))
        self.write(records)
        with mock.patch.dict(os.environ, {"HTTP_PROXY": "http://127.0.0.1:1", "HTTPS_PROXY": "http://127.0.0.1:1", "NO_PROXY": ""}):
            self.sender.once()
        self.assertEqual(len(self.state["events"]), 1)
        self.assertNotIn("sentinel-secret", json.dumps(self.state["requests"]))
        self.assertNotIn(b"sentinel-secret", (self.root / "state" / "ledger.sqlite").read_bytes())

    @unittest.skipIf(sys.version_info < (3, 11), "installer requires Python 3.11 tomllib")
    def test_install_infers_config_preserves_identity_baseline_and_hides_secret(self):
        native_endpoint = self.config["endpoint"].replace("/v1/usage", "/v1/logs")
        (self.home / "config.toml").write_text('[otel]\nenvironment="test-machine"\n[otel.exporter.otlp-http]\nendpoint="' + native_endpoint + '"\n[otel.exporter.otlp-http.headers]\nAuthorization="Bearer sentinel-secret"\n', encoding="utf-8")
        target = self.root / "installed"
        with mock.patch.object(usage, "stop_startup"):
            first = usage.install(target, self.home, autostart=False, backfill_since="2026-09-07")
            second = usage.install(target, self.home, autostart=False, adopt_sessions=["session-old"])
        self.assertEqual(first["collector_id"], second["collector_id"])
        self.assertEqual(first["baseline_ms"], second["baseline_ms"])
        config = json.loads((target / "config.json").read_text())
        self.assertEqual(config["endpoint"], self.config["endpoint"])
        self.assertEqual(config["token"], "sentinel-secret")
        self.assertEqual(config["adopt_sessions"], ["session-old"])
        self.assertEqual(config["backfill_since"], "2026-09-07")
        self.assertNotIn("sentinel-secret", json.dumps(second))

    def test_windows_update_stops_existing_task_before_hidden_restart(self):
        with mock.patch.object(usage.sys, "platform", "win32"), mock.patch.object(usage.subprocess, "CREATE_NO_WINDOW", 0x08000000, create=True), mock.patch.object(usage.subprocess, "run") as run:
            usage.install_startup(self.root, self.root / "codex_usage.py", "collector-1")
        args = run.call_args.args[0]
        command = base64.b64decode(args[-1]).decode("utf-16le")
        self.assertIn("Get-ScheduledTask -TaskPath", command)
        self.assertIn("if($existing)", command)
        self.assertIn("Stop-ScheduledTask -InputObject $existing -ErrorAction Stop", command)
        self.assertIn("sender_stop_timeout", command)
        self.assertIn(".State -eq 'Running'", command)
        self.assertLess(command.index("Stop-ScheduledTask"), command.index("Register-ScheduledTask"))
        self.assertLess(command.index("Register-ScheduledTask"), command.index("Start-ScheduledTask"))
        self.assertNotIn("SilentlyContinue", command)
        self.assertEqual(args[args.index("-WindowStyle") + 1], "Hidden")
        self.assertIn("New-ScheduledTaskSettingsSet -Hidden", command)
        self.assertEqual(run.call_args.kwargs["creationflags"], 0x08000000)
        self.assertTrue(run.call_args.kwargs["check"])

    def test_linux_update_enables_then_restarts_existing_service(self):
        with mock.patch.object(usage.sys, "platform", "linux"), mock.patch.object(usage.Path, "home", return_value=self.root), mock.patch.object(usage.subprocess, "run") as run:
            usage.install_startup(self.root, self.root / "codex_usage.py", "collector-1")
        calls = [call.args[0] for call in run.call_args_list]
        self.assertEqual(calls, [
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", "codex-usage-collecto.service"],
            ["systemctl", "--user", "restart", "codex-usage-collecto.service"],
        ])
        self.assertTrue(all(call.kwargs["check"] for call in run.call_args_list))

    def prepare_history(self, session="session-old", **owner):
        path = self.write(journal(session=session, created="2026-09-01T00:00:00Z"))
        (self.root / "state" / "config.json").write_text(json.dumps(self.config))
        self.state["ownership"] = {session: dict(machines=["test-machine"], **owner)}
        return path

    def test_backfill_dry_run_finds_old_continued_session_without_mutating_state(self):
        self.prepare_history()
        ended = journal(session="ended", created="2026-09-01T00:00:00Z", response="ended-response")
        ended[-1]["timestamp"] = "2026-09-04T23:59:59Z"
        self.write(ended, name="ended.jsonl")
        before = {p.name: p.read_bytes() for p in (self.root / "state").iterdir()}
        result = usage.backfill(self.root / "state", "2026-09-05")
        self.assertEqual(result["sessions"][0]["status"], "safe")
        self.assertEqual(result["sessions"][0]["local_events"], 1)
        self.assertEqual(result["sessions"][0]["local_tokens"], 120)
        self.assertFalse(result["applied"])
        self.assertEqual(before, {p.name: p.read_bytes() for p in (self.root / "state").iterdir()})
        self.assertEqual([r["action"] for r in self.state["requests"]], ["inspect"])
        self.assertEqual(self.state["requests"][0]["sessions"], ["session-old"])
        self.assertFalse(self.state["events"])
        self.assertNotIn("sentinel-secret", json.dumps(result))

    def test_backfill_blocks_unknown_multiple_foreign_and_competing_owners(self):
        self.prepare_history()
        cases = [({}, "unknown_owner"),
                 ({"machines": ["other-machine"]}, "foreign_machine"),
                 ({"machines": ["test-machine", "other-machine"]}, "multiple_machines"),
                 ({"machines": ["test-machine"], "collectors": ["another"]}, "foreign_collector"),
                 ({"machines": ["test-machine"], "active_collector": "another", "active_machine": "test-machine"}, "foreign_collector"),
                 ({"machines": ["test-machine"], "before_since": True}, "server_history_before_since")]
        for owner, reason in cases:
            with self.subTest(reason=reason):
                self.state["ownership"]["session-old"] = owner
                result = usage.backfill(self.root / "state", "2026-09-05", apply=True)
                self.assertEqual(result["sessions"][0]["reason"], reason)
                self.assertEqual(result["sessions"][0]["status"], "blocked")
                self.assertEqual(usage.read_config(self.root / "state" / "config.json")["adopt_sessions"], [])
                self.assertFalse(self.state["events"])

    def test_backfill_ignores_legacy_prefix_but_enforces_requested_window(self):
        path = self.prepare_history()
        old_usage = journal(response="old-response")[-1]
        old_usage["timestamp"] = "2026-09-04T23:59:58Z"
        old_usage["payload"].update(thread_id="unverified-old-thread", usage={"input_tokens": "legacy"})
        legacy = dict(timestamp="2026-09-04T23:59:59Z", type="event_msg",
                      payload=dict(type="token_count", info={"total_token_usage": {"total_tokens": 999}}))
        records = journal(session="session-old", created="2026-09-01T00:00:00Z")
        self.write(records[:2] + [old_usage, legacy] + records[2:])
        result = usage.backfill(self.root / "state", "2026-09-05", apply=True)
        self.assertEqual(result["sessions"][0]["status"], "safe")
        self.assertEqual(set(self.state["events"]), {"resp-1"})
        self.assertEqual(self.state["events"]["resp-1"]["effort"], "high")
        config = usage.read_config(self.root / "state" / "config.json")
        self.assertEqual(config["history_since"], {"session-old": usage.timestamp_ms("2026-09-05T00:00:00Z")})
        activations = [r for r in self.state["requests"] if r["action"] == "activate"]
        self.assertEqual(activations[-1]["since_ms"], config["history_since"]["session-old"])
        legacy["timestamp"] = "2026-09-05T00:00:00Z"
        self.write(records[:2] + [legacy] + records[2:])
        result = usage.backfill(self.root / "state", "2026-09-05")
        self.assertEqual(result["sessions"][0]["reason"], "unsupported_format")

    def test_backfill_failed_inspection_and_invalid_local_files_cannot_adopt(self):
        path = self.prepare_history()
        before = (self.root / "state" / "config.json").read_bytes()
        self.state["action_status"] = {"inspect": 500}
        result = usage.backfill(self.root / "state", "2026-09-05", apply=True)
        self.assertEqual(result["last_error"], "HTTP_500")
        self.assertFalse(result["applied"])
        self.assertEqual(before, (self.root / "state" / "config.json").read_bytes())
        self.assertFalse(list((self.root / "state").glob("*.backup-*")))
        self.state["action_status"].clear()
        with path.open("a", encoding="utf-8") as stream:
            stream.write('{"timestamp":')
        result = usage.backfill(self.root / "state", "2026-09-05", apply=True)
        self.assertEqual(result["sessions"][0]["reason"], "incomplete_journal")
        self.assertFalse(self.state["events"])

    def test_backfill_backup_reapply_lost_ack_and_existing_scope(self):
        self.prepare_history()
        before = (self.root / "state" / "config.json").read_bytes()
        self.state["lose_ack"] = True
        first = usage.backfill(self.root / "state", "2026-09-05", apply=True)
        self.assertEqual(first["pending_events"], 1)
        self.assertTrue(first["applied"])
        config_backups = list((self.root / "state").glob("config.backup-*.json"))
        self.assertEqual(len(config_backups), 1)
        self.assertEqual(config_backups[0].read_bytes(), before)
        backups = list((self.root / "state").glob("ledger.backup-*.sqlite"))
        self.assertEqual(len(backups), 1)
        with contextlib.closing(__import__("sqlite3").connect(backups[0])) as db:
            self.assertEqual(db.execute("PRAGMA quick_check").fetchone()[0], "ok")
        second = usage.backfill(self.root / "state", "2026-09-06", apply=True)
        self.assertEqual(second["pending_events"], 0)
        self.assertEqual(len(self.state["events"]), 1)
        self.assertEqual(usage.read_config(self.root / "state" / "config.json")["history_since"]["session-old"],
                         usage.timestamp_ms("2026-09-05T00:00:00Z"))
        config = usage.read_config(self.root / "state" / "config.json")
        config.pop("history_since")
        (self.root / "state" / "config.json").write_text(json.dumps(config))
        self.state["ownership"]["session-old"].update(active_collector="collector-1", active_machine="test-machine", before_since=True)
        third = usage.backfill(self.root / "state", "2026-09-05", apply=True)
        self.assertEqual(third["sessions"][0]["reason"], "already_active")
        self.assertEqual(usage.read_config(self.root / "state" / "config.json").get("history_since", {}).get("session-old", 0), 0)

    def test_install_backfill_preserves_identity_and_runs_before_startup(self):
        self.prepare_history()
        def startup(state_dir, script, collector, **kwargs):
            self.assertEqual(self.state["active"], {"session-old"})
            self.assertEqual(collector, self.config["collector_id"])
            return "test_startup"
        with mock.patch.object(usage, "install_startup", side_effect=startup):
            result = usage.install(self.root / "state", self.home)
        config = usage.read_config(self.root / "state" / "config.json")
        for key in ("collector_id", "baseline_ms", "token", "machine"):
            self.assertEqual(config[key], self.config[key])
        self.assertEqual(result["autostart"], "test_startup")
        self.assertEqual(config["backfill_since"], "2026-09-05")
        self.assertFalse(result["backfill_pending"])
        self.assertNotIn("sentinel-secret", json.dumps(result))
        with mock.patch.object(usage, "backfill", return_value={"applied": False}) as backfill, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(usage.main(["backfill", "--state-dir", str(self.root / "state"), "--since", "2026-09-05"]), 0)
        self.assertEqual(backfill.call_args.args[:2], (self.root / "state", "2026-09-05"))

    def test_default_install_retries_history_after_restart_without_blocking_new_usage(self):
        self.prepare_history()
        target = self.root / "state"
        self.state["action_status"] = {"inspect": 500}
        with mock.patch.object(usage, "install_startup", return_value="test_startup"):
            result = usage.install(target, self.home)
        self.assertTrue(result["backfill_pending"])
        self.assertEqual(result["last_error"], "HTTP_500")
        self.sender.close()
        self.write(journal(response="new-response"), name="new.jsonl")
        config = usage.read_config(target / "config.json")
        with contextlib.closing(usage.Collector(config, target)) as restarted:
            failed = restarted.once()
            self.assertTrue(failed["backfill_pending"])
            self.assertEqual(failed["backfill_error"], "HTTP_500")
            self.assertEqual(set(self.state["events"]), {"new-response"})
        self.state["action_status"].clear()
        with contextlib.closing(usage.Collector(config, target)) as restarted:
            recovered = restarted.once()
            self.assertFalse(recovered["backfill_pending"])
            self.assertIsNone(recovered["last_error"])
            self.assertEqual(set(self.state["events"]), {"new-response", "resp-1"})
            count = sum(r["action"] == "inspect" for r in self.state["requests"])
            restarted.once()
            self.assertEqual(sum(r["action"] == "inspect" for r in self.state["requests"]), count)
        report = usage.read_config(target / "backfill-report.json")
        self.assertTrue(report["applied"])
        self.assertFalse(report["backfill_pending"])
        self.assertNotIn("sentinel-secret", json.dumps(report))
        self.assertTrue(any(s["session"] == "session-old" and s["status"] == "safe" for s in report["sessions"]))

    def test_history_retry_can_recover_owned_usage_despite_an_unrelated_bad_file(self):
        self.prepare_history()
        target = self.root / "state"
        self.state["action_status"] = {"inspect": 500}
        with mock.patch.object(usage, "install_startup", return_value="test_startup"):
            usage.install(target, self.home)
        broken = self.write(journal(session="broken")[:2], name="broken.jsonl")
        with broken.open("a", encoding="utf-8") as stream:
            stream.write("invalid JSON\n")
        self.state["action_status"].clear()
        with contextlib.closing(usage.Collector(usage.read_config(target / "config.json"), target)) as restarted:
            result = restarted.once()
            self.assertEqual(set(self.state["events"]), {"resp-1"})
            self.assertFalse(result["backfill_pending"])
            self.assertEqual(result["last_error"], "parse_error")
            self.assertIsNone(result["backfill_error"])
            self.assertEqual(result["backfill_excluded_sessions"], 1)
            count = sum(r["action"] == "inspect" for r in self.state["requests"])
            restarted.once()
            self.assertEqual(sum(r["action"] == "inspect" for r in self.state["requests"]), count)

    def test_failed_same_date_reinstall_remains_pending_after_prior_success(self):
        self.prepare_history()
        target = self.root / "state"
        with mock.patch.object(usage, "install_startup", return_value="test_startup"), mock.patch.object(usage, "stop_startup"):
            self.assertFalse(usage.install(target, self.home)["backfill_pending"])
            with mock.patch.object(usage, "backfill", side_effect=OSError("interrupted_history")):
                with self.assertRaises(OSError):
                    usage.install(target, self.home)
        with contextlib.closing(usage.Collector(usage.read_config(target / "config.json"), target)) as restarted:
            self.assertTrue(restarted.status()["backfill_pending"])
            self.assertFalse(restarted.once()["backfill_pending"])

    def test_install_stops_old_sender_and_restores_autostart_after_failed_inspection(self):
        self.prepare_history()
        target = self.root / "state"
        script = target / "codex_usage.py"
        script.write_text("old sender")
        self.state["action_status"] = {"inspect": 500}
        actions = []
        def stop(collector_id):
            self.assertEqual(script.read_text(), "old sender")
            self.assertEqual(usage.read_config(target / "config.json")["adopt_sessions"], [])
            actions.append("stop")
        def start(state_dir, installed, collector_id, **kwargs):
            self.assertEqual(script.read_bytes(), Path(usage.__file__).read_bytes())
            self.assertEqual(usage.read_config(target / "config.json")["collector_id"], self.config["collector_id"])
            actions.append("start")
            self.assertFalse(kwargs["stop_existing"])
            return "test_startup"
        with mock.patch.object(usage, "stop_startup", side_effect=stop), mock.patch.object(usage, "install_startup", side_effect=start):
            result = usage.install(target, self.home, backfill_since="2026-09-05")
        self.assertEqual(actions, ["stop", "start"])
        self.assertEqual(result["last_error"], "HTTP_500")
        self.assertEqual(result["autostart"], "test_startup")
        self.assertEqual(usage.read_config(target / "config.json")["adopt_sessions"], [])
        self.assertTrue(list(target.glob("config.backup-*.json")))

    def test_standalone_backfill_requires_updated_installed_sender(self):
        self.prepare_history()
        target = self.root / "state"
        (target / "codex_usage.py").write_text("old sender")
        before = {p.name: p.read_bytes() for p in target.iterdir()}
        result = usage.backfill(target, "2026-09-05", apply=True)
        self.assertEqual(result["last_error"], "sender_update_required")
        self.assertFalse(result["applied"])
        self.assertEqual(before, {p.name: p.read_bytes() for p in target.iterdir()})
        self.assertFalse(self.state["events"])

    def test_install_exception_after_stop_restores_existing_sender(self):
        self.prepare_history()
        target = self.root / "state"
        script = target / "codex_usage.py"
        script.write_text("old sender")
        before = (target / "config.json").read_bytes()
        with mock.patch.object(usage, "backfill", side_effect=ValueError("simulated_history_error")), mock.patch.object(usage, "stop_startup") as stop, mock.patch.object(usage, "install_startup") as start:
            with self.assertRaisesRegex(ValueError, "simulated_history_error"):
                usage.install(target, self.home, backfill_since="2026-09-05")
        stop.assert_called_once()
        start.assert_called_once()
        saved = usage.read_config(target / "config.json")
        self.assertEqual(saved["backfill_since"], "2026-09-05")
        for key in ("collector_id", "baseline_ms", "adopt_sessions", "history_since", "token"):
            self.assertEqual(saved.get(key), json.loads(before).get(key))
        self.assertTrue(any(p.read_bytes() == before for p in target.glob("config.backup-*.json")))
        script.write_text("old sender")
        def failed_copy(source, destination):
            Path(destination).write_text("truncated")
            raise OSError("simulated_copy_error")
        with mock.patch.object(usage, "shutil") as shutil, mock.patch.object(usage, "stop_startup"), mock.patch.object(usage, "install_startup") as start:
            shutil.copyfile.side_effect = failed_copy
            with self.assertRaisesRegex(OSError, "simulated_copy_error"):
                usage.install(target, self.home, backfill_since="2026-09-05")
        start.assert_called_once()
        self.assertEqual(script.read_text(), "old sender")

    def test_fresh_install_without_journals_still_enables_future_collection(self):
        target = self.root / "fresh-install"
        (self.home / "sessions").rename(self.home / "missing-sessions")
        with mock.patch.object(usage, "install_startup", return_value="test_startup") as start:
            result = usage.install(target, self.home, machine="test-machine", endpoint=self.config["endpoint"],
                                   token="sentinel-secret", backfill_since="2026-09-05")
        self.assertEqual(result["last_error"], "sessions_missing")
        self.assertEqual(result["autostart"], "test_startup")
        self.assertEqual(usage.read_config(target / "config.json")["adopt_sessions"], [])
        start.assert_called_once()

    def test_backfill_reports_previously_limited_scope_without_widening_it(self):
        self.config.update(adopt_sessions=["session-old"], history_since={"session-old": usage.timestamp_ms("2026-09-10T00:00:00Z")})
        self.prepare_history(active_collector="collector-1", active_machine="test-machine")
        result = usage.backfill(self.root / "state", "2026-09-05")
        self.assertEqual(result["sessions"][0]["reason"], "history_window_already_limited")
        self.assertEqual(result["sessions"][0]["effective_since_ms"], self.config["history_since"]["session-old"])

    def test_backfill_bounds_inspection_and_groups_activation_by_scope(self):
        self.prepare_history()
        for index in range(201):
            self.write(journal(session=f"old-{index}", created="2026-09-01T00:00:00Z", response=f"response-{index}"), name=f"old-{index}.jsonl")
        usage.backfill(self.root / "state", "2026-09-05")
        inspections = [r for r in self.state["requests"] if r["action"] == "inspect"]
        self.assertEqual([len(r["sessions"]) for r in inspections], [200, 2])
        self.sender.config.update(adopt_sessions=["session-old"], history_since={"session-old": usage.timestamp_ms("2026-09-05T00:00:00Z")})
        self.write(journal(response="new-response"), name="new.jsonl")
        self.sender.scan()
        self.sender.sync()
        activations = [r for r in self.state["requests"] if r["action"] == "activate"]
        self.assertEqual({r["since_ms"]: r["sessions"] for r in activations}, {0: ["session-new"], usage.timestamp_ms("2026-09-05T00:00:00Z"): ["session-old"]})


if __name__ == "__main__":
    unittest.main()
