import json
import tempfile
import unittest
import threading
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path

from server import Store, Handler

def batch(**changes):
    values = {"event.name": "codex.sse_event", "event.kind": "response.completed", "event.timestamp": "2026-09-05T13:31:28.075Z", "env": "mac-a", "host.name": "mac-a", "conversation.id": "conversation-1", "model": "test-model", "input_token_count": "100", "cached_token_count": "80", "output_token_count": "10", "reasoning_token_count": "6", "user.email": "PRIVATE_SENTINEL", "prompt": "PRIVATE_SENTINEL"}
    values.update(changes)
    attributes = [{"key": k, "value": {"stringValue": v}} for k, v in values.items() if v is not None]
    return {"resourceLogs": [{"resource": {"attributes": None}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "0", "body": None, "attributes": attributes}]}]}]}

class UsageTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")

    def tearDown(self):
        self.store.connection.close()

    def test_cache_and_reasoning_are_subsets(self):
        self.store.ingest(batch())
        event = self.store.snapshot()["events"][0]
        self.assertEqual(event["total_tokens"], 110)
        self.assertEqual(event["cached_input_tokens"], 80)
        self.assertEqual(event["reasoning_output_tokens"], 6)

    def test_retries_do_not_change_total_but_distinct_requests_do(self):
        self.assertEqual(self.store.ingest(batch()), 1)
        self.assertEqual(self.store.ingest(batch()), 0)
        self.assertEqual(self.store.ingest(batch(**{"event.timestamp": "2026-09-05T13:31:28.076Z"})), 1)
        self.assertEqual(self.store.ingest(batch(env="mac-b")), 1)
        self.assertEqual(len(self.store.snapshot()["events"]), 3)

    def test_absent_usage_is_not_zero(self):
        self.store.ingest(batch(input_token_count=None))
        self.assertEqual(self.store.snapshot()["events"], [])
        self.assertEqual(self.store.snapshot()["diagnostics"]["invalid_usage"], 1)
        self.store.ingest(batch(cached_token_count=None))
        self.assertIsNone(self.store.snapshot()["events"][0]["cached_input_tokens"])

    def test_real_zero_output_is_retained(self):
        self.store.ingest(batch(output_token_count="0", reasoning_token_count="0"))
        self.assertEqual(self.store.snapshot()["events"][0]["total_tokens"], 100)

    def test_unrelated_events_and_inconsistent_counters_are_not_counted(self):
        self.store.ingest(batch(**{"event.kind": "response.created"}))
        self.store.ingest(batch(cached_token_count="101"))
        self.store.ingest(batch(**{"event.timestamp": None}))
        self.assertEqual(self.store.snapshot()["events"], [])

    def test_private_values_are_discarded(self):
        self.store.ingest(batch())
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(self.store.snapshot()))
        self.assertNotIn("PRIVATE_SENTINEL", str(self.store.connection.execute("SELECT body FROM usage").fetchall()))

    def test_restart_keeps_usage_and_deduplication(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "usage.sqlite"
            first = Store(path)
            first.ingest(batch())
            first.connection.close()
            second = Store(path)
            self.assertEqual(second.ingest(batch()), 0)
            self.assertEqual(len(second.snapshot()["events"]), 1)
            second.connection.close()

class ReceiverTests(unittest.TestCase):
    def test_ingestion_requires_its_own_token(self):
        http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        http.store = Store(':memory:')
        http.ingest_token = 'test-token-' + 'x' * 32
        http.central = True
        http.deployment_label = 'Test'
        worker = threading.Thread(target=http.serve_forever, daemon=True)
        worker.start()
        url = f'http://127.0.0.1:{http.server_port}'
        try:
            payload = json.dumps(batch()).encode()
            request = urllib.request.Request(url+'/v1/logs', data=payload, headers={'Content-Type':'application/json'})
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(request)
            self.assertEqual(error.exception.code, 401)
            self.assertEqual(http.store.snapshot()['events'], [])
            request.add_header('Authorization', 'Bearer '+http.ingest_token)
            self.assertEqual(urllib.request.urlopen(request).status, 200)
            self.assertEqual(len(http.store.snapshot()['events']), 1)
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(url+'/api/usage?hours=999')
            self.assertEqual(error.exception.code, 400)
        finally:
            http.shutdown()
            http.server_close()
            worker.join()
            http.store.connection.close()

if __name__ == "__main__":
    unittest.main()
