"""Ephemeral browser fixture. Run directly; never writes to a production database."""
import json
import time
from http.server import ThreadingHTTPServer
from server import Store, Handler
from test_analytics import fixture, event


def main():
    http = ThreadingHTTPServer(('127.0.0.1', 8770), Handler)
    http.store = Store(':memory:')
    http.ingest_token = None
    http.central = False
    http.deployment_label = 'Browser fixture — not production'
    now = int(time.time() * 1000)
    rows = fixture() + [event(4000, machine='idle-host', session='zero-session', output_tokens=0, reasoning_output_tokens=0, total_tokens=100, effort=None, service_tier=None)]
    for i, e in enumerate(rows):
        e['timestamp_ms'] = now - 60000 + i * 1000
        e['model'] = 'gpt-5.6-sol' if e['model'] == 'model-a' else 'gpt-5.5'
        e['cache_write_input_tokens'] = 0
        http.store.connection.execute('INSERT INTO usage VALUES (?,?,?)', (e['id'], e['timestamp_ms'], json.dumps(e)))
    for machine in ['mac', 'server', 'idle-host']:
        http.store.connection.execute('INSERT INTO machine_activity VALUES (?,?,?,?,?,?,?)', (machine, machine, 'test', 'fixture', now - 86400000, now - 60000, now if machine != 'idle-host' else now - 86400000))
    http.store.connection.commit()
    print('Browser fixture ready on 8770', flush=True)
    try:
        http.serve_forever()
    finally:
        http.server_close()
        http.store.connection.close()


if __name__ == '__main__':
    main()
