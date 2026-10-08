import json
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.request
import urllib.error
import threading
from pathlib import Path
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer

import analytics
import pricing
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
    def test_session_ranking_summarizes_only_the_displayed_top_100(self):
        raw=[event(i,session=str(i),input_tokens=100+i,total_tokens=120+i) for i in range(130)]
        with patch('analytics.summarize',wraps=analytics.summarize) as summarize:
            d=analytics.report(raw,START,NOW,{},NOW,START,[])
        self.assertEqual(d['session_count'],130)
        self.assertEqual([s['key'] for s in d['sessions']],[str(i) for i in range(129,29,-1)])
        self.assertEqual(sum(len(call.args[0])==1 for call in summarize.call_args_list),100)

    def test_filtered_report_prices_only_selected_events_and_keeps_facets(self):
        raw=fixture()+[event(-1000,effort=' HIGH ',service_tier=' PRIORITY ')]
        with patch('pricing.estimate', wraps=analytics.pricing.estimate) as estimate:
            d=analytics.report(raw,START,NOW,{'machine':'mac','effort':'high','tier':'fast'},NOW,START,[])
        self.assertEqual(d['totals']['events'],2)
        self.assertEqual(d['comparison']['totals']['events'],1)
        self.assertEqual(d['facets']['machine'],['mac','server'])
        self.assertEqual(d['facets']['effort'],['high','unknown'])
        self.assertEqual(d['facets']['tier'],['fast','standard'])
        self.assertEqual(estimate.call_count,3)

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
        e.pop('cached_input_tokens')
        self.assertIsNone(analytics.summarize([e])['cached_input_tokens'])

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
    def test_cached_window_reads_again_on_boundary_or_database_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'usage.sqlite'
            store=Store(path)
            queries=[]
            def insert(connection,e):
                connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)))
                connection.commit()
            try:
                insert(store.connection,event(1000))
                insert(store.connection,event(86400001))
                self.assertEqual(len(store.read_interval(START,NOW,cached=True)),1)
                store.connection.set_trace_callback(queries.append)
                self.assertEqual(len(store.read_interval(START+1,NOW+1,cached=True)),1)
                self.assertFalse(any('FROM visible_usage' in q for q in queries))
                self.assertEqual(len(store.read_interval(START+1001,NOW+2,cached=True)),1)
                self.assertTrue(any('FROM visible_usage' in q for q in queries))
                insert(store.connection,event(86400000))
                self.assertEqual(len(store.read_interval(START+1001,NOW+2,cached=True)),2)
                other=sqlite3.connect(path)
                try:
                    other.execute('DELETE FROM usage WHERE id=?',('86400000',));other.commit()
                    self.assertEqual(len(store.read_interval(START+1001,NOW+2,cached=True)),1)
                finally:
                    other.close()
            finally:
                store.connection.close()

    def test_cached_report_keeps_activity_coverage_and_bucket_size_live(self):
        store=Store(':memory:')
        params={'hours':['24']}
        raw=event(86400000-1000)
        try:
            store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(raw['id'],raw['timestamp_ms'],json.dumps(raw)))
            store.connection.commit()
            with patch('server.time.time',return_value=NOW/1000):
                initial=store.analysis(params)
            old=event(-3*86400000)
            store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(old['id'],old['timestamp_ms'],json.dumps(old)))
            store.connection.execute('INSERT INTO machine_activity VALUES (?,?,?,?,?,?,?)',('idle-host',None,None,None,None,None,NOW))
            store.connection.commit()
            store.set_machine_visibility('idle-host',True)
            with patch('analytics.grouped',side_effect=AssertionError('unchanged events were regrouped')):
                with patch('server.time.time',return_value=(NOW+1)/1000):
                    updated=store.analysis(params)
            expected=analytics.report([raw],START+1,NOW+1,initial['filters'],NOW+1,old['timestamp_ms'],store.activity(NOW+1))
            self.assertEqual(updated,expected)
            self.assertTrue(updated['machine_activity'][0]['hidden'])
            with patch('server.time.time',return_value=(NOW+1)/1000):
                hourly=store.analysis({'hours':['1']})
            self.assertEqual(hourly['period']['bucket_ms'],60000)
            self.assertEqual(hourly['timeline'][0]['end_ms']-hourly['timeline'][0]['timestamp_ms'],60000)
        finally:
            store.connection.close()

    def test_rolling_report_reuses_unchanged_events_and_groups(self):
        store=Store(':memory:')
        raw=[event(1000),event(2000,session='s2'),event(-1000)]
        try:
            for e in raw:
                store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)))
            store.connection.commit()
            with patch('server.time.time',return_value=NOW/1000):
                store.analysis({'hours':['24']})
            with patch('analytics.enrich',wraps=analytics.enrich) as enriched, patch('analytics.summarize',wraps=analytics.summarize) as summarized, patch('analytics.grouped',wraps=analytics.grouped) as grouped:
                with patch('server.time.time',return_value=(NOW+1)/1000):
                    warm=store.analysis({'hours':['24']})
                self.assertEqual(enriched.call_count,0)
                self.assertEqual(summarized.call_count,0)
                self.assertEqual(grouped.call_count,0)
            expected=analytics.report(raw,START+1,NOW+1,{'zero_output':'include','timezone':'UTC'},NOW+1,START-1000,[])
            self.assertEqual(warm,expected)
            added=event(3000,session='s2')
            store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(added['id'],added['timestamp_ms'],json.dumps(added)))
            store.connection.commit()
            with patch('analytics.enrich',wraps=analytics.enrich) as enriched, patch('analytics.summarize',wraps=analytics.summarize) as summarized:
                with patch('server.time.time',return_value=(NOW+1)/1000):
                    updated=store.analysis({'hours':['24']})
                self.assertEqual(enriched.call_count,1)
                self.assertFalse(any([e['id'] for e in call.args[0]]==['1000'] for call in summarized.call_args_list))
            self.assertEqual(updated,analytics.report(raw+[added],START+1,NOW+1,expected['filters'],NOW+1,START-1000,[]))
        finally:
            store.connection.close()

    def test_cached_report_tracks_boundaries_edits_deletes_and_rates(self):
        store=Store(':memory:')
        params={'hours':['24']}
        raw=[event(0,model='gpt-5.5',service_tier='standard'),event(1000,model='gpt-5.5',service_tier='standard')]
        def check(now):
            with patch('server.time.time',return_value=now/1000):
                actual=store.analysis(params)
            start,end,filters=analytics.parse_query(params,now)
            first=store.connection.execute('SELECT MIN(timestamp_ms) FROM visible_usage').fetchone()[0]
            expected=analytics.report(store.read_interval(start-(end-start),end),start,end,filters,now,first,store.activity(now))
            self.assertEqual(actual,expected)
            return actual
        try:
            for e in raw:
                store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)))
            store.connection.commit()
            check(NOW)
            crossed=check(NOW+1)
            self.assertEqual(crossed['totals']['events'],1)
            self.assertEqual(crossed['comparison']['totals']['events'],1)
            changed={**raw[1],'input_tokens':200,'total_tokens':220}
            store.connection.execute('UPDATE usage SET body=? WHERE id=?',(json.dumps(changed),changed['id']))
            store.connection.commit()
            edited=check(NOW+1)
            self.assertEqual(edited['totals']['total_tokens'],220)
            with patch.dict(pricing.RATES,{'gpt-5.5':{**pricing.RATES['gpt-5.5'],'standard':(10,1,None,60)}}):
                repriced=check(NOW+1)
                self.assertEqual(repriced['totals']['api_cost_usd'],2*edited['totals']['api_cost_usd'])
            check(NOW+1)
            long=event(86400001,model='gpt-5.5',input_tokens=300000,total_tokens=300020,service_tier='standard')
            store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(long['id'],long['timestamp_ms'],json.dumps(long)))
            store.connection.commit()
            self.assertGreater(check(NOW+1)['totals']['api_cost_usd'],edited['totals']['api_cost_usd'])
            store.connection.execute('DELETE FROM usage WHERE id=?',(changed['id'],))
            store.connection.commit()
            self.assertEqual(check(NOW+1)['totals']['events'],0)
        finally:
            store.connection.close()

    def test_analytics_reports_do_not_allocate_in_parallel(self):
        store=Store(':memory:')
        entered=threading.Event(); overlap=threading.Event(); release=threading.Event()
        original=analytics.report
        def report(*args, **kwargs):
            if entered.is_set(): overlap.set()
            entered.set()
            release.wait(3)
            return original(*args, **kwargs)
        threads=[threading.Thread(target=store.analysis,args=({'start':[str(START)],'end':[str(NOW)]},)) for _ in range(2)]
        try:
            with patch('analytics.report',side_effect=report):
                threads[0].start()
                self.assertTrue(entered.wait(2))
                threads[1].start()
                try:
                    self.assertFalse(overlap.wait(.2))
                finally:
                    release.set()
                    for thread in threads: thread.join(3)
                self.assertTrue(all(not thread.is_alive() for thread in threads))
        finally:
            store.connection.close()

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
