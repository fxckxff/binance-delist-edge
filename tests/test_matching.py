import unittest
from lead_lag import match_announcements
from monitor_core import SeenState
from pathlib import Path
import tempfile

class MatchingTests(unittest.TestCase):
    def test_market_date_and_ambiguity(self):
        b=[dict(title='Delisting of ABCUSDT Perpetual Contract',publish_ms=100000,syms=['ABC'])]
        a=dict(title='Binance Futures Will Delist ABCUSDT',release_ms=100001,catalog=161)
        self.assertEqual(len(match_announcements(b,{'a':a})[0]),1)
        self.assertEqual(match_announcements(b,{'a':a,'b':dict(a,release_ms=100002)})[0],[])
        self.assertEqual(match_announcements(b,{'a':dict(a,title='Binance Margin Will Delist ABCUSDT')})[0],[])
        self.assertEqual(match_announcements(b,{'a':dict(a,release_ms=4000000000)})[0],[])

    def test_corrupt_cursor_bootstraps(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'state.json';p.write_text('{"bootstrapped":true,"ids":null}')
            self.assertFalse(SeenState(p).load().bootstrapped)

if __name__=='__main__':unittest.main()
