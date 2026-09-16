import json
import csv
import io
import tempfile
import unittest
import urllib.request
import urllib.error
from pathlib import Path

import analytics
import pricing
from server import Store
from test_server import batch
from test_analytics import event, HTTPTestCase, START, NOW


def priced(**changes):
    return analytics.enrich(event(1, model='gpt-5.6-sol', input_tokens=10000,
        cached_input_tokens=4000, cache_write_input_tokens=2000, output_tokens=1000,
        reasoning_output_tokens=700, total_tokens=11000, service_tier='default', **changes))


class PriceTests(unittest.TestCase):
    def test_cache_read_write_and_reasoning_without_double_charge(self):
        e=priced()
        # 4000*4 + 4000*.4 + 2000*5 + 1000*20 = 47600 USD / million.
        self.assertEqual(e['api_cost_usd'],.0476)
        self.assertEqual(e['api_ordinary_input_usd'],.016)
        self.assertEqual(e['api_cache_write_usd'],.01)
        self.assertEqual(e['api_output_usd'],.02)
        self.assertAlmostEqual(sum(e[k] for k in pricing.PARTS), e['api_cost_usd'])

    def test_model_tiers_and_unknown_assumption_are_separate(self):
        raw=event(1, model='gpt-5.6-sol', cache_write_input_tokens=0)
        standard=analytics.enrich({**raw,'service_tier':'default'})
        fast=analytics.enrich({**raw,'service_tier':'priority'})
        flex=analytics.enrich({**raw,'service_tier':'flex'})
        unknown=analytics.enrich({**raw,'service_tier':None})
        self.assertEqual(fast['api_cost_usd'],2*standard['api_cost_usd'])
        self.assertEqual(flex['api_cost_usd'],.5*standard['api_cost_usd'])
        self.assertEqual(unknown['api_cost_usd'],standard['api_cost_usd'])
        self.assertEqual(unknown['tier'],'unknown')
        self.assertTrue(unknown['api_price_assumed_tier'])
        self.assertFalse(fast['api_price_assumed_tier'])
        five=analytics.enrich({**raw,'model':'gpt-5.5','service_tier':'priority'})
        self.assertAlmostEqual(five['api_cost_usd'],(20*12.5+80*1.25+20*75)/1e6)

    def test_context_threshold_and_unpublished_rate(self):
        raw=event(1, model='gpt-6-astra', input_tokens=272000, cached_input_tokens=100000,
            cache_write_input_tokens=20000, output_tokens=1000, reasoning_output_tokens=500, service_tier='default')
        self.assertEqual(analytics.enrich(raw)['api_price_context'],'short')
        long=analytics.enrich({**raw,'input_tokens':272001})
        self.assertEqual(long['api_cost_usd'],(152001*20+100000*2+20000*25+1000*75)/1e6)
        unsupported=analytics.enrich({**raw,'input_tokens':272001,'model':'gpt-5.5','service_tier':'priority'})
        self.assertIsNone(unsupported['api_cost_usd'])
        self.assertEqual(unsupported['api_price_reason'],'unsupported_long_context')

    def test_missing_unknown_and_invalid_counts_stay_unpriced(self):
        raw=event(1,model='gpt-5.6-sol',cache_write_input_tokens=0)
        for changes in [{'model':'new-model'},{'cache_write_input_tokens':None},{'cached_input_tokens':None},{'cache_write_input_tokens':99},{'service_tier':'unpublished-tier'}]:
            self.assertIsNone(analytics.enrich({**raw,**changes})['api_cost_usd'])
        # Older models have no separate write rate and need no write counter.
        self.assertIsNotNone(analytics.enrich({**raw,'model':'gpt-5.4-mini','cache_write_input_tokens':None})['api_cost_usd'])

    def test_price_coverage_and_aggregation(self):
        good=priced();bad=analytics.enrich(event(2,model='new-model'))
        t=analytics.summarize([good,bad])
        self.assertIsNone(t['api_cost_usd'])
        self.assertEqual(t['api_cost_known_usd'],.0476)
        self.assertEqual(t['api_priced_events'],1)
        self.assertEqual(t['api_price_coverage_pct'],50)
        self.assertEqual(t['api_unpriced_reasons'],{'unknown_model':1})
        self.assertAlmostEqual(sum(t['api_cost_breakdown_known_usd'].values()),.0476)
        self.assertIsNone(analytics.summarize([])['api_cost_known_usd'])

    def test_long_session_applies_across_query_boundaries(self):
        store=Store(':memory:')
        for e in [event(-1000,model='gpt-5.5',input_tokens=300000,cached_input_tokens=0),event(1000,model='gpt-5.5',service_tier='default')]:
            store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(e['id'],e['timestamp_ms'],json.dumps(e)))
        store.connection.commit()
        rows=store.read_interval(START,NOW)
        self.assertEqual(len(rows),1)
        estimate=analytics.enrich(rows[0])
        self.assertEqual(estimate['api_price_context'],'long')
        self.assertEqual(estimate['api_cost_usd'],(20*10+80*1+20*45)/1e6)
        store.connection.close()


class VisibilityTests(unittest.TestCase):
    def test_hide_restart_new_event_restore_keep_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'usage.sqlite';store=Store(path);store.ingest(batch())
            before=store.snapshot()['events'];machine=before[0]['machine']
            store.set_machine_visibility(machine,True)
            self.assertTrue(store.activity()[0]['hidden'])
            store.connection.close();store=Store(path);store.ingest(batch())
            self.assertTrue(store.activity()[0]['hidden'])
            self.assertEqual(store.snapshot()['events'],before)
            store.set_machine_visibility(machine,False)
            self.assertFalse(store.activity()[0]['hidden'])
            with self.assertRaises(KeyError):store.set_machine_visibility('not-known',True)
            store.connection.close()


class VisibilityAPITests(HTTPTestCase):

    def test_csv_and_summary_prices_match_the_same_filtered_events(self):
        self.http.store.connection.execute('DELETE FROM usage')
        for i,tier in enumerate(['default','priority',None]):
            raw=event(i,model='gpt-5.6-sol',service_tier=tier,cache_write_input_tokens=0)
            self.http.store.connection.execute('INSERT INTO usage VALUES (?,?,?)',(raw['id'],raw['timestamp_ms'],json.dumps(raw)))
        self.http.store.connection.commit()
        q=self.query+'&machine=mac'
        summary=json.loads(self.get('/api/v1/analytics'+q))
        rows=list(csv.DictReader(io.StringIO(self.get('/api/v1/export.csv'+q).decode())))
        self.assertAlmostEqual(sum(float(r['api_cost_usd']) for r in rows),summary['totals']['api_cost_usd'])
        self.assertEqual(summary['totals']['api_assumed_tier_events'],1)
        self.assertEqual(rows[-1]['tier'],'unknown')
        self.assertEqual(rows[-1]['api_price_tier'],'standard')

    def request(self,payload,headers):
        req=urllib.request.Request(self.base+'/api/v1/machines/visibility',data=json.dumps(payload).encode(),headers=headers)
        try:
            with urllib.request.urlopen(req) as r:return r.status
        except urllib.error.HTTPError as e:
            code=e.code;e.close();return code

    def test_visibility_requires_explicit_same_origin_json_action(self):
        self.http.store.connection.execute('INSERT INTO machine_activity(machine) VALUES (?)',('mac',));self.http.store.connection.commit()
        p={'machine':'mac','hidden':True}
        h={'Content-Type':'application/json','X-Codex-Usage-Action':'visibility'}
        self.assertEqual(self.request(p,{}),403)
        self.assertEqual(self.request(p,{**h,'Origin':'https://evil.example','Sec-Fetch-Site':'cross-site'}),403)
        self.assertEqual(self.request(p,{**h,'Content-Type':'text/plain'}),415)
        self.assertEqual(self.request({**p,'hidden':'yes'},h),400)
        self.assertEqual(self.request(p,{**h,'Origin':'http://127.0.0.1','Sec-Fetch-Site':'same-origin'}),200)
        self.assertTrue(json.loads(self.get('/api/v1/machines'))['machines'][0]['hidden'])
        self.assertEqual(json.loads(self.get('/api/v1/analytics'+self.query))['totals']['total_tokens'],690)
