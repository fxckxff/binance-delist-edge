#!/usr/bin/env python3
"""Rebuild auditable candidates from saved CMS + minute cache, without network.

Missing cache means unknown, never proof of an absent contract. This does not
replace the historical manifest. Titles alone can omit symbols: candidates need review.
"""
import json
from pathlib import Path
from backtest_futures import symbols_from_title

ROOT = Path(__file__).resolve().parent


def build():
    cms=json.loads((ROOT/'data/cms_articles.json').read_text())
    cache=json.loads((ROOT/'data/fut_klines_1m.json').read_text())
    candidates=[]
    for code, a in cms.items():
        if a.get('catalog') != 161:
            continue
        for symbol in symbols_from_title(a['title']):
            rel=a['release_ms']; key=f'{symbol}:{rel}'
            series=cache.get(key)
            if series is None:
                status='not_fetched'
            elif not series:
                status='unknown_empty_legacy_cache'
            elif not any(int(c[6]) <= rel and float(c[5]) > 0 for c in series):
                status='no_pre_announcement_volume'
            elif not any(int(c[0]) >= rel for c in series):
                status='no_post_announcement_data'
            else:
                status='has_futures_data'
            candidates.append(dict(symbol=symbol,release_ms=rel,article_code=code,
                                   title=a['title'],status=status))
    counts={}
    for c in candidates:counts[c['status']]=counts.get(c['status'],0)+1
    result=dict(source='saved CMS + saved minute cache; title extraction requires review',
                counts=counts,candidates=sorted(candidates,key=lambda c:(c['release_ms'],c['symbol'])))
    out=ROOT/'data/candidate_audit.v2.json'
    out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(counts,ensure_ascii=False,indent=2));print(out)

if __name__=='__main__':build()
