"""Durable receiver invariants, including coexistence with the old OTLP stream."""
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from http.server import ThreadingHTTPServer

from server import Handler, Store
from test_server import batch


def event(response_id="resp-1", **changes):
    return dict(response_id=response_id, machine="mac-a", collector_id="collector-a",
                session="conversation-1", timestamp_ms=1788615088075,
                model="test-model", client_version="0.153.4", input_tokens=100,
                cached_input_tokens=80, cache_write_input_tokens=0, output_tokens=10,
                reasoning_output_tokens=6, total_tokens=110, effort=None,
                service_tier=None, **changes)


def message(action, **values):
    return dict(version=1, action=action, machine="mac-a", collector_id="collector-a", **values)


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")

    def tearDown(self):
        self.store.connection.close()

    def test_ack_is_durable_and_duplicate_retry_is_identical(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "usage.sqlite"
            first = Store(path)
            payload = message("events", events=[event()])
            try:
                self.assertEqual(first.sync(payload)["accepted"], ["resp-1"])
            finally:
                first.connection.close()
            second = Store(path)
            try:
                self.assertEqual(second.sync(payload)["accepted"], ["resp-1"])
                self.assertEqual(second.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)
            finally:
                second.connection.close()

    def test_conflicting_batch_rolls_back_every_event(self):
        self.store.sync(message("events", events=[event()]))
        changed = {**event(), "model": "different"}
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.store.sync(message("events", events=[event("resp-new"), changed]))
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)
        with self.assertRaises(ValueError):
            self.store.sync(message("events", events=[event("resp-new"), {**event("bad"), "total_tokens": 111}]))
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)

    def test_one_source_per_session_and_original_rows_retained(self):
        self.store.ingest(batch())
        self.store.sync(message("events", events=[event(), event("resp-2")]))
        self.assertEqual(len(self.store.snapshot()["events"]), 1)
        self.store.sync(message("activate", sessions=["conversation-1"]))
        events = self.store.read_interval(0, 2**53 - 1)
        self.assertEqual(len(events), 2)
        self.assertTrue(all(e["source"] == "journal" for e in events))
        self.store.ingest(batch(**{"event.timestamp": "2026-09-05T13:31:29.075Z"}))
        self.assertEqual(len(self.store.snapshot()["events"]), 2)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 4)

    def test_copied_session_and_response_cannot_move_between_hosts(self):
        self.store.ingest(batch(env="other-host"))
        self.store.sync(message("events", events=[event()]))
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.store.sync(message("activate", sessions=["conversation-1"]))
        other = {**event(), "machine": "other-host", "collector_id": "collector-b"}
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.store.sync({**message("events", events=[other]), "machine": "other-host", "collector_id": "collector-b"})
        self.assertEqual(self.store.snapshot()["events"][0]["machine"], "other-host")

    def test_late_copied_native_session_does_not_double_count(self):
        self.store.sync(message("events", events=[event()]))
        self.store.sync(message("activate", sessions=["conversation-1"]))
        self.store.ingest(batch(env="other-host"))
        self.assertEqual(len(self.store.snapshot()["events"]), 1)
        self.assertEqual(self.store.snapshot()["events"][0]["machine"], "mac-a")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 2)

    def test_inventory_finds_server_loss_and_status_survives_restart(self):
        from codex_usage import canonical_event, day_inventory
        days = day_inventory([canonical_event(event())])
        status = dict(scanned_at_ms=1788615089000, pending_events=1, pending_tokens=110, last_error="HTTP_500")
        payload = message("inventory", days=days, status=status)
        self.assertEqual(self.store.sync(payload)["resend_days"], [days[0]["day"]])
        self.store.sync(message("events", events=[event()]))
        self.assertEqual(self.store.sync(payload)["resend_days"], [])
        activity = self.store.activity()[0]["journal"]
        self.assertEqual(activity["pending_events"], 1)
        self.assertEqual(activity["last_error"], "HTTP_500")
        self.store.connection.execute("DELETE FROM usage")
        self.store.connection.commit()
        self.assertEqual(self.store.sync(payload)["resend_days"], [days[0]["day"]])
        self.store.sync(message("events", events=[event()]))
        self.assertEqual(self.store.sync(payload)["resend_days"], [])

    def test_untrusted_event_fields_are_discarded(self):
        self.store.sync(message("events", events=[{**event(), "prompt": "PRIVATE_SENTINEL"}]))
        self.assertNotIn("PRIVATE_SENTINEL", str(self.store.connection.execute("SELECT body FROM usage").fetchall()))

    def test_failed_activation_rolls_back_all_sessions(self):
        other = {**event("resp-other"), "session": "other-session"}
        self.store.sync(message("events", events=[event(), other]))
        self.store.ingest(batch(env="other-host", **{"conversation.id": "other-session"}))
        with self.assertRaises(ValueError):
            self.store.sync(message("activate", sessions=["conversation-1", "other-session"]))
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM journal_sessions").fetchone()[0], 0)

    def test_database_failure_returns_no_ack(self):
        self.store.connection.execute("CREATE TRIGGER disk_failure BEFORE INSERT ON usage BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        import sqlite3
        with self.assertRaises(sqlite3.Error):
            self.store.sync(message("events", events=[event()]))
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM journal_collectors").fetchone()[0], 0)


class SyncHTTPTests(unittest.TestCase):
    def test_real_sender_recovers_lost_ack_and_empty_server_restore(self):
        from codex_usage import Collector
        from test_codex_usage import journal
        import socket
        class DropAckOnce(Handler):
            def respond(self, status, body, kind="application/json"):
                if self.server.drop_ack and b'"accepted"' in body:
                    self.server.drop_ack = False
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                super().respond(status, body, kind)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sessions").mkdir()
            (root / "sessions" / "rollout.jsonl").write_text("".join(json.dumps(row)+"\n" for row in journal()), encoding="utf-8")
            http = ThreadingHTTPServer(("127.0.0.1", 0), DropAckOnce)
            http.store = Store(":memory:")
            http.ingest_token, http.drop_ack = "x" * 40, True
            worker = threading.Thread(target=http.serve_forever, daemon=True)
            worker.start()
            sender = Collector(dict(codex_home=str(root), machine="test-machine", collector_id="collector-1", baseline_ms=0,
                endpoint=f"http://127.0.0.1:{http.server_port}/v1/usage", token=http.ingest_token, timeout=3), root / "state")
            try:
                first = sender.once()
                self.assertEqual(first["pending_events"], 1)
                self.assertEqual(first["last_error"], "network_error")
                self.assertEqual(http.store.connection.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)
                self.assertIsNone(sender.once()["last_error"])
                self.assertEqual(len(http.store.snapshot()["events"]), 1)
                # A restored server lost both acknowledged rows and session activation.
                http.store.connection.close()
                http.store = Store(":memory:")
                self.assertIsNone(sender.once()["last_error"])
                self.assertEqual(sender.status()["pending_events"], 0)
                self.assertEqual(http.store.snapshot()["events"][0]["total_tokens"], 120)
                report = http.store.activity()[0]["journal"]
                self.assertEqual((report["pending_events"], report["mismatched_days"], report["active_sessions"]), (0, 0, 1))
            finally:
                sender.close()
                http.shutdown()
                http.server_close()
                worker.join()
                http.store.connection.close()

    def test_auth_errors_and_ack_after_database_commit(self):
        http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        http.store = Store(":memory:")
        http.ingest_token = "x" * 40
        worker = threading.Thread(target=http.serve_forever, daemon=True)
        worker.start()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = f"http://127.0.0.1:{http.server_port}/v1/usage"
        def send(payload, token="x" * 40):
            request = urllib.request.Request(url, json.dumps(payload).encode(),
                {"Content-Type": "application/json", "Authorization": "Bearer " + token})
            return opener.open(request, timeout=3)
        try:
            with self.assertRaises(urllib.error.HTTPError) as error:
                send(message("events", events=[event()]), "wrong")
            self.assertEqual(error.exception.code, 401)
            with send(message("events", events=[event()])) as response:
                self.assertEqual(json.load(response)["accepted"], ["resp-1"])
            self.assertFalse(http.store.connection.in_transaction)
            for value, expected in (({**event(), "model": "conflict"}, 409), ({**event(), "total_tokens": -1}, 400)):
                with self.assertRaises(urllib.error.HTTPError) as error:
                    send(message("events", events=[value]))
                self.assertEqual(error.exception.code, expected)
        finally:
            http.shutdown()
            http.server_close()
            worker.join()
            http.store.connection.close()


if __name__ == "__main__":
    unittest.main()
