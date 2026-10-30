import asyncio
import json
import logging
import math
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
from backtest_futures import simulate
from tick_backtest import simulate_ticks
from horizon_test import simulate_long
from research_core import cluster_bootstrap, portfolio
from monitor_core import Config, Monitor, BackoffClient, SeenState, depth_usd, parse_articles, SingleInstance


def candle(t, op=100, high=100, close=100):
    return [t, str(op), str(high), '90', str(close), '10', t+59999]


class ResearchTests(unittest.TestCase):
    def test_tick_gap_fills_at_observed_price(self):
        r = simulate_ticks([(1000, 100), (2000, 105), (300000, 90)], 0, 1, 100, 300, 10)
        self.assertEqual(r['exit'], 105)
        self.assertAlmostEqual(r['net_bps'], -510)

    def test_tick_exit_no_hindsight(self):
        r = simulate_ticks([(1000,100), (299000,90), (300001,95)], 0, 1, 0, 300, 10)
        self.assertEqual(r['exit'],95)
        self.assertEqual(r['exit_t'],300001)

    def test_tick_incomplete_and_invalid(self):
        self.assertEqual(simulate_ticks([(1000,100),(2000,99)],0,1,0,300,10),'incomplete_horizon')
        self.assertEqual(simulate_ticks([(300001,100)],0,1,0,300,10),'no_entry_trade')
        self.assertEqual(simulate_ticks([(2000,100),(1000,99)],0,1,0,300,10),'unordered_ticks')
        self.assertEqual(simulate_ticks([(1000,math.nan)],0,1,0,300,10),'bad_price')

    def test_same_timestamp_preserves_trade_order(self):
        r = simulate_ticks([(1000,100),(1000,105),(1000,90)],0,1,100,300,10)
        self.assertEqual(r['exit'],105)

    def test_minute_no_future_close(self):
        rows=[candle(60000),candle(120000),candle(180000),candle(240000,close=98),candle(300000,close=1)]
        r=simulate(rows,0,1,0,300,10)
        self.assertEqual(r['exit'],98)
        self.assertEqual(r['entry_t'],60000)
        self.assertEqual(r['exit_t'],299999)

    def test_minute_incomplete_and_gap(self):
        self.assertEqual(simulate([candle(60000)],0,1,0,300,10),'incomplete_horizon')
        self.assertEqual(simulate([candle(60000),candle(180000)],0,1,0,300,10),'missing_candles')
        r=simulate([candle(60000),candle(120000,op=105,high=106)],0,1,100,300,10)
        self.assertEqual(r['exit'],105)
        self.assertEqual(simulate_long([candle(60000)],0,1,0,86400),'incomplete_horizon')

    def test_overlapping_positions_not_compounded(self):
        events=[dict(symbol='B',release_ms=0),dict(symbol='A',release_ms=0),dict(symbol='C',release_ms=400)]
        def sim(e):
            return dict(entry_t=e['release_ms']+1,exit_t=e['release_ms']+300,net_bps=100)
        r=portfolio(events,sim,100,10)
        self.assertEqual(r['trades'],2)
        self.assertEqual(r['skipped']['overlapping_position'],1)
        self.assertAlmostEqual(r['final_equity'],1018.26,places=2)

    def test_cluster_bootstrap_reproducible(self):
        rows=[(1,10),(1,20),(2,-10)]
        self.assertEqual(cluster_bootstrap(rows),cluster_bootstrap(rows))
        self.assertIsNone(cluster_bootstrap([(1,10),(1,20)]))


class MonitorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        self.cfg=Config(state_path=root/'seen.json',stop_path=root/'STOP',pid_path=root/'pid',log_path=root/'log')
        self.log=logging.getLogger('regressions');self.log.addHandler(logging.NullHandler())

    async def test_no_spot_fallback(self):
        calls=[]
        def handler(req):
            calls.append(str(req.url));return httpx.Response(400,json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            mon=Monitor(self.cfg,self.log,client=BackoffClient(self.cfg,self.log,client=client))
            result=await mon.check_liquidity(['BTCUSDT'])
        self.assertEqual(len(calls),1)
        self.assertFalse(result[0]['ok'])

    async def test_future_announcement_rejected(self):
        async with httpx.AsyncClient() as client:
            mon=Monitor(self.cfg,self.log,client=BackoffClient(self.cfg,self.log,client=client),
                        depth_probe=lambda _: self.fail('future announcement reached depth probe'))
            await mon.handle(dict(id='x',title='Binance Futures Will Delist BTCUSDT',release_ms=int((time.time()+300)*1000)))
            self.assertEqual(mon.signals,0)

    async def test_stop_interrupts_retry_sleep(self):
        async with httpx.AsyncClient() as client:
            mon=Monitor(self.cfg,self.log,client=BackoffClient(self.cfg,self.log,client=client))
            self.cfg.stop_path.touch()
            with self.assertRaises(asyncio.CancelledError):
                await mon.interruptible_sleep(300)

    async def test_retry_after_not_capped(self):
        delays=[]
        async def sleep(d):delays.append(d)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _:httpx.Response(429,headers={'Retry-After':'600'}))) as client:
            self.cfg.max_attempts=2
            await BackoffClient(self.cfg,self.log,client=client,sleep=sleep).request('https://example.test')
        self.assertEqual(delays,[600])

    def test_depth_fail_closed_and_bid_only(self):
        self.assertEqual(depth_usd({'bids':[[100,1]],'asks':[[101,10000]]},100),100)
        for book in ({'bids':[[100,math.inf]],'asks':[[101,1]]},
                     {'bids':[[102,1]],'asks':[[101,1]]},
                     {'bids':[[100,-1]],'asks':[[101,1]]}):
            self.assertEqual(depth_usd(book,100),0)

    def test_infinite_release_date(self):
        out=parse_articles({'data':{'catalogs':[{'articles':[{'id':1,'releaseDate':math.inf}]}]}})
        self.assertEqual(out[0]['release_ms'],0)

    def test_invalid_env(self):
        with patch.dict('os.environ',{'NEWS_MIN_DEPTH_USD':'inf'}):
            with self.assertRaises(ValueError):Config.from_env()

    def test_second_instance_does_not_truncate_pid(self):
        one,two=SingleInstance(self.cfg.pid_path),SingleInstance(self.cfg.pid_path)
        try:
            self.assertTrue(one.acquire());original=self.cfg.pid_path.read_text()
            self.assertFalse(two.acquire());self.assertEqual(original,self.cfg.pid_path.read_text())
        finally:one.release();two.release()


if __name__=='__main__':unittest.main()
