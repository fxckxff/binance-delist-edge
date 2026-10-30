import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
import forward_study as study
from monitor_core import Config

class ForwardTests(unittest.TestCase):
    def test_visible_size_vwap(self):
        entry=study.fill([[100, .5],[99,2]],notional=100)
        self.assertAlmostEqual(entry['value'],100)
        self.assertLess(entry['price'],100)
        self.assertIsNone(study.fill([[100,.1]],notional=100))
        self.assertEqual(study.fill([[101,2]],quantity=1)['value'],101)

    def test_bad_book(self):
        self.assertFalse(study.valid_book({'bids':[[101,1]],'asks':[[100,1]]}))
        self.assertFalse(study.valid_book({'bids':[[99,1]],'asks':[[100,float('inf')]]}))

    def test_protocol_cannot_change(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'study.db'
            db=study.open_study(path);db.close()
            with patch.dict(study.PROTOCOL,{'horizon_s':100}):
                with self.assertRaises(ValueError):study.open_study(path)

class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_short_stop_uses_asks_and_both_fees(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)
            db=study.open_study(path/'study.db')
            books=iter([{'bids':[[100,10]],'asks':[[100.1,10]]},
                        {'bids':[[102,10]],'asks':[[103,10]]}])
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json=next(books)))) as client:
                with patch.dict(study.PROTOCOL,{'sample_s':0}):
                    await study.observe(db,{'id':'a','symbol':'BTCUSDT'},client,Config(stop_path=path/'STOP'))
            status,payload=db.execute('SELECT status,payload FROM events').fetchone()
            result=json.loads(payload)
            self.assertEqual(status,'closed')
            self.assertAlmostEqual(result['pnl_usd'],-3.1015)
            self.assertEqual(result['reason'],'sampled_stop')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM books').fetchone()[0],2)
            db.close()

    async def test_failed_exit_is_not_profitable_trade(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d);db=study.open_study(path/'study.db')
            books=iter([{'bids':[[100,10]],'asks':[[100.1,10]]}, {'bids':[[90,10]],'asks':[[91,.01]]}])
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json=next(books)))) as client:
                with patch.dict(study.PROTOCOL,{'sample_s':0}):
                    await study.observe(db,{'id':'a','symbol':'BTCUSDT'},client,Config(stop_path=path/'STOP'))
            self.assertEqual(db.execute('SELECT status FROM events').fetchone()[0],'invalid')
            db.close()
