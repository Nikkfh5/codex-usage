import json
import sqlite3
import tempfile
import time
import unittest
import urllib.request
import urllib.error
import threading
from pathlib import Path
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer

import analytics
from server import Store, Handler, normalize, records
from test_server import batch

NOW = 1788739200000
START = NOW - 86400000


def event(offset, **changes):
    values = dict(id=str(offset), timestamp_ms=START+offset, machine='mac', model='model-a', session='s1', input_tokens=100, cached_input_tokens=80, output_tokens=20, reasoning_output_tokens=10, total_tokens=120, effort='high', service_tier='priority')
    values.update(changes)
    return values


def fixture():
    return [event(1000), event(2000, input_tokens=200, cached_input_tokens=120, output_tokens=30, reasoning_output_tokens=15, total_tokens=230), event(3000, machine='server', session='s2', model='model-b', input_tokens=300, cached_input_tokens=0, output_tokens=40, reasoning_output_tokens=10, total_tokens=340, effort=None, service_tier='default')]


class AnalyticsTests(unittest.TestCase):
    def test_weighted_ratios_and_nonoverlapping_parts(self):
        t=analytics.summarize([analytics.enrich(e) for e in fixture()])
        self.assertEqual(t['total_tokens'],690)
        self.assertEqual(t['uncached_input_tokens'],400)
        self.assertEqual(t['visible_output_tokens'],55)
        self.assertEqual(t['average_tokens'],230)
        self.assertAlmostEqual(t['cache_total_pct'],100*200/690,places=3)
        self.assertAlmostEqual(t['cache_input_pct'],100*200/600,places=3)
        self.assertAlmostEqual(t['reasoning_output_pct'],100*35/90,places=3)
        self.assertEqual(sum(t[k] for k in ['uncached_input_tokens','cached_input_tokens','visible_output_tokens','reasoning_output_tokens']),690)

    def test_missing_values_and_empty_period_are_not_zeroes(self):
        e=analytics.enrich(event(1, cached_input_tokens=None, reasoning_output_tokens=None, effort=None, service_tier=None))
        t=analytics.summarize([e])
        self.assertIsNone(t['cache_total_pct']);self.assertIsNone(t['reasoning_output_pct'])
        self.assertEqual(e['effort'],'unknown');self.assertEqual(e['tier'],'unknown')
        self.assertIsNone(analytics.summarize([])['total_tokens'])
        self.assertIsNone(analytics.ratio(0,0))

    def test_mode_changes_within_a_session_remain_per_response(self):
        raw=fixture()+[event(4000, effort='low', service_tier='default')]
        d=analytics.report(raw,START,NOW,{'effort':'high','tier':'fast'},NOW,START,[])
        self.assertEqual(d['totals']['total_tokens'],350)
        self.assertEqual(d['totals']['events'],2)
        self.assertEqual(d['sessions'][0]['efforts'],['high'])
        self.assertEqual(d['sessions'][0]['tiers'],['fast'])
        self.assertEqual(analytics.tier_name(None),'unknown')
        self.assertEqual(analytics.tier_name('auto'),'auto')
        self.assertEqual(analytics.tier_name('flex'),'flex')

    def test_comparison_same_filters_half_open_ranges(self):
        raw=fixture()+[event(-1000),event(86400000),event(-86400001)]
        d=analytics.report(raw,START,NOW,{'machine':'mac'},NOW,START-86400000,[])
        self.assertEqual(d['totals']['total_tokens'],350)
        self.assertEqual(d['comparison']['totals']['total_tokens'],120)
        self.assertAlmostEqual(d['comparison']['change_pct']['total_tokens'],191.67)
        self.assertEqual(sum(r['total_tokens'] for r in d['timeline']),350)
        self.assertEqual(sum(r['total_tokens'] for r in d['previous_timeline']),120)

    def test_machine_facets_survive_empty_period_and_session_ids_are_host_scoped(self):
        raw=[event(1),event(2,machine='server')]
        d=analytics.report(raw,START,NOW,{},NOW,START,[{'machine':'idle-host'}])
        self.assertEqual(d['totals']['sessions'],2)
        self.assertEqual(len(d['sessions']),2)
        self.assertIn('idle-host',d['facets']['machine'])
        empty=analytics.report(raw,START,NOW,{'machine':'idle-host'},NOW,START,[{'machine':'idle-host'}])
        self.assertEqual(empty['totals']['events'],0)
        self.assertIn('idle-host',empty['facets']['machine'])

    def test_zero_output_filter_applies_everywhere(self):
        raw=fixture()+[event(5000,output_tokens=0,reasoning_output_tokens=0,total_tokens=100)]
        d=analytics.report(raw,START,NOW,{'zero_output':'exclude'},NOW,START,[])
        self.assertEqual(d['totals']['total_tokens'],690)
        self.assertEqual(d['totals']['zero_output_events'],0)
        for key in ['machine','model','effort','tier']:
            self.assertEqual(sum(r['total_tokens'] for r in d['breakdowns'][key]),690)

    def test_month_and_strict_query_contract(self):
        start,end,filters=analytics.parse_query({'hours':['720'],'timezone':['Europe/Moscow']},NOW)
        self.assertEqual(end-start,30*86400000)
        self.assertEqual(filters['timezone'],'Europe/Moscow')
        for params in [{'hours':['745']},{'hours':['0']},{'hours':['24','48']},{'effrot':['high']},{'start':['1']},{'start':['1'],'end':[str(NOW)],'hours':['24']},{'timezone':['not/a/zone']}]:
            with self.assertRaises(ValueError):analytics.parse_query(params,NOW)

    def test_daily_buckets_follow_timezone_and_dst(self):
        start=int(datetime(2026,3,27,tzinfo=timezone.utc).timestamp()*1000);end=start+5*86400000
        at=int(datetime(2026,3,29,12,tzinfo=timezone.utc).timestamp()*1000)
        d=analytics.report([event(0,timestamp_ms=at)],start,end,{'timezone':'Europe/Berlin'},end,start,[])
        row=d['timeline'][0]
        self.assertEqual(row['end_ms']-row['timestamp_ms'],23*3600000)
        self.assertEqual(row['total_tokens'],120)


class PersistenceTests(unittest.TestCase):
    def test_metadata_does_not_change_retry_identity(self):
        one=next(records(batch()));two=next(records(batch(model_reasoning_effort='high',service_tier='fast')))
        a=normalize(*one);b=normalize(*two)
        self.assertEqual(a['id'],b['id']);self.assertEqual(b['effort'],'high');self.assertEqual(b['service_tier'],'fast')
        self.assertIsNone(a['effort'])

    def test_legacy_migration_keeps_events_and_receipt_time_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'old.sqlite';c=sqlite3.connect(path);c.execute('CREATE TABLE usage (id TEXT PRIMARY KEY,timestamp_ms INTEGER NOT NULL,body TEXT NOT NULL)')
            e=event(1);e.pop('effort');e.pop('service_tier');body=json.dumps(e);c.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],body));c.commit();c.close()
            store=Store(path)
            self.assertEqual(store.connection.execute('SELECT body FROM usage').fetchone()[0],body)
            self.assertIsNone(store.activity()[0]['last_received_ms'])
            self.assertEqual(store.activity()[0]['status'],'historical')
            store.connection.close()

    def test_any_native_activity_is_persistent_and_not_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'db';store=Store(p)
            store.ingest(batch(**{'event.name':'codex.user_prompt','prompt':'PRIVATE_SENTINEL'}))
            self.assertEqual(store.snapshot()['events'],[])
            state=store.activity();self.assertEqual(state[0]['status'],'recent')
            self.assertNotIn('PRIVATE_SENTINEL',json.dumps(state))
            store.connection.close();store=Store(p)
            self.assertEqual(store.activity()[0]['status'],'recent')
            self.assertEqual(store.activity(now_ms=int(time.time()*1000)+121000)[0]['status'],'quiet')
            store.connection.close()


class HTTPTestCase(unittest.TestCase):
    def setUp(self):
        self.http=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.http.store=Store(':memory:');self.http.ingest_token='x'*40;self.http.central=True;self.http.deployment_label='test'
        for i,e in enumerate(fixture()):self.http.store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)))
        self.http.store.connection.commit();self.thread=threading.Thread(target=self.http.serve_forever,daemon=True);self.thread.start();self.base='http://127.0.0.1:'+str(self.http.server_port)
        self.query='?start='+str(START)+'&end='+str(NOW)
        # Current test clock is dynamic; fixed analytical fixtures may be future relative to CI.
        self.original=analytics.parse_query
        def parse(params,now):return self.original(params,max(now,NOW))
        analytics.parse_query=parse
    def tearDown(self):
        analytics.parse_query=self.original;self.http.shutdown();self.http.server_close();self.thread.join();self.http.store.connection.close()
    def get(self,path):
        with urllib.request.urlopen(self.base+path) as r:return r.read()


class APITests(HTTPTestCase):
    def test_dashboard_query_url_is_reloadable(self):
        self.assertIn(b'id="total"',self.get('/?period=720&machine=mac'))

    def test_agent_pagination_and_filters_match_summary(self):
        d=json.loads(self.get('/api/v1/analytics'+self.query+'&machine=mac'))
        first=json.loads(self.get('/api/v1/events'+self.query+'&machine=mac&limit=1'))
        second=json.loads(self.get('/api/v1/events'+self.query+'&machine=mac&limit=1&cursor='+first['next_cursor']))
        self.assertIsNone(second['next_cursor'])
        self.assertEqual(sum(e['total_tokens'] for e in first['events']+second['events']),d['totals']['total_tokens'])
        self.assertEqual(json.loads(self.get('/api/v1/schema'))['schema_version'],'2.2')
    def test_csv_formulas_escaped_and_contract_rejects_bad_params(self):
        e=event(999,machine='=HYPERLINK("bad")');self.http.store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)));self.http.store.connection.commit()
        text=self.get('/api/v1/export.csv'+self.query).decode();self.assertIn("'=HYPERLINK",text)
        for path in ['/api/v1/analytics?effrot=high','/api/v1/analytics?limit=1','/api/v1/events'+self.query+'&cursor=missing','/api/v1/export.csv'+self.query+'&limit=1','/api/v1/events'+self.query+'&limit=1&limit=2']:
            with self.assertRaises(urllib.error.HTTPError) as error:self.get(path)
            self.assertEqual(error.exception.code,400);error.exception.close()
