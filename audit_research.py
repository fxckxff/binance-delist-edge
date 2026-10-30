#!/usr/bin/env python3
"""Offline consistency audit and v2 minute recomputation; no API calls."""
import hashlib
import json
from pathlib import Path
import backtest_futures as bt

ROOT = Path(__file__).resolve().parent


def audit():
    def read(name):
        return json.loads((ROOT / 'data' / name).read_text())
    manifest = read('events.json')
    cms = read('cms_articles.json')
    cache = read('fut_klines_1m.json')
    ticks = read('tick_backtest.json')
    issues = []
    keys = [(e['symbol'], e['release_ms']) for e in manifest]
    if len(keys) != len(set(keys)):
        issues.append('duplicate_manifest_events')
    if ticks.get('events') != len(manifest):
        issues.append('legacy_tick_report_does_not_cover_manifest')
    if not ticks.get('bootstrap'):
        issues.append('legacy_tick_report_has_no_confidence_intervals')
    linked = {a['release_ms'] for a in cms.values() if a.get('catalog') == 161}
    unmatched = [e for e in manifest if e['release_ms'] not in linked]
    events = []
    for e in manifest:
        series = cache.get(f"{e['symbol']}:{e['release_ms']}", [])
        if not series:
            issues.append(f"missing_minute_data:{e['symbol']}:{e['release_ms']}")
            continue
        events.append(dict(e, series=series))
    old = read('fut_backtest.json')
    opt = read('fut_backtest_opt.json')
    if old['with_contract'] != opt['with_contract']:
        issues.append('legacy_contract_count_changes_when_cache_is_used')
    tick_v2_path = ROOT / 'data' / 'tick_backtest.v2.json'
    tick_v2 = json.loads(tick_v2_path.read_text()) if tick_v2_path.exists() else None
    result = dict(
        manifest_events=len(manifest), announcement_timestamps=len({e['release_ms'] for e in manifest}),
        unique_symbols=len({e['symbol'] for e in manifest}),
        legacy_tick_events=ticks.get('events'), legacy_tick_ci_rows=len(ticks.get('bootstrap', {})),
        unmatched_manifest_events=unmatched, issues=issues,
        nonempty_minute_cache_entries=sum(bool(v) for v in cache.values()),
        corrected_tick_events=tick_v2.get('events') if tick_v2 else None,
        inputs_sha256={name: hashlib.sha256((ROOT/'data'/name).read_bytes()).hexdigest()
                       for name in ('events.json', 'cms_articles.json', 'fut_klines_1m.json', 'tick_backtest.json')},
        minute_model='v2 closed candles only, gap-aware stops, one position at a time; not executable P&L',
        minute_grid=bt.run_grid(events, 10.),
    )
    out = ROOT / 'data' / 'audit_v2.json'
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('minute_grid', 'inputs_sha256')}, ensure_ascii=False, indent=2))
    print(f'Report: {out}')
    return result


if __name__ == '__main__':
    audit()
