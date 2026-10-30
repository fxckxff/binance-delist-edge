"""Prospective read-only depth experiment. Run alongside monitor.py."""
import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
import time
from pathlib import Path
import httpx
from monitor_core import Config, SingleInstance, categorize, extract_symbols

ROOT = Path(__file__).resolve().parent
PROTOCOL = dict(version=1, market='futures_delist', notional_usd=100,
                stop_bps=100, horizon_s=300, sample_s=1, fee_bps_per_side=5,
                max_detection_age_s=900, max_book_rtt_s=2, max_sampling_gap_s=5,
                final_clusters=30, max_days=180)


def fill(levels, quantity=None, notional=None):
    """VWAP with full visible size; insufficient size is unfilled."""
    if (quantity is None) == (notional is None):
        raise ValueError('Specify quantity OR notional')
    target = quantity if quantity is not None else notional
    if not math.isfinite(target) or target <= 0:
        return None
    total_qty = total_value = 0.
    for level in levels:
        p, q = map(float, level[:2])
        if not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q < 0:
            return None
        take = min(q, max(0, quantity-total_qty) if quantity is not None else max(0, (notional-total_value)/p))
        total_qty += take
        total_value += take*p
        if (quantity is not None and total_qty >= quantity-1e-10) or (notional is not None and total_value >= notional-1e-8):
            return dict(price=total_value/total_qty, quantity=total_qty, value=total_value)
    return None


def valid_book(book):
    try:
        bids = [(float(x[0]),float(x[1])) for x in book['bids']]
        asks = [(float(x[0]),float(x[1])) for x in book['asks']]
        return (bool(bids and asks) and bids[0][0] < asks[0][0]
                and all(math.isfinite(p) and math.isfinite(q) and p > 0 and q >= 0 for p,q in bids+asks)
                and all(bids[i][0] >= bids[i+1][0] for i in range(len(bids)-1))
                and all(asks[i][0] <= asks[i+1][0] for i in range(len(asks)-1)))
    except (KeyError, TypeError, ValueError, IndexError):
        return False


def open_study(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, status TEXT NOT NULL, payload TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS books (event_id TEXT, received_ms INTEGER, rtt_s REAL, payload TEXT)')
    frozen = json.dumps(PROTOCOL, sort_keys=True)
    old = db.execute("SELECT value FROM meta WHERE key='protocol'").fetchone()
    if old and old[0] != frozen:
        db.close()
        raise ValueError('Protocol differs from frozen study; use a new database')
    db.execute("INSERT OR IGNORE INTO meta VALUES ('protocol', ?)",(frozen,))
    db.execute("INSERT OR IGNORE INTO meta VALUES ('started_ms', ?)",(str(int(time.time()*1000)),))
    db.commit()
    return db


def save(db, eid, status, payload):
    db.execute('INSERT OR REPLACE INTO events VALUES (?, ?, ?)',(eid,status,json.dumps(payload,ensure_ascii=False)))
    db.commit()


async def observe(db, event, client, cfg):
    eid = str(event['id'])
    data = dict(event)
    async def book():
        start = time.monotonic()
        response = await client.get('https://fapi.binance.com/fapi/v1/depth',params={'symbol':data['symbol'],'limit':1000})
        response.raise_for_status()
        payload = response.json()
        rtt = time.monotonic()-start
        db.execute('INSERT INTO books VALUES (?, ?, ?, ?)',(eid,int(time.time()*1000),rtt,json.dumps(payload)))
        db.commit()
        if rtt > PROTOCOL['max_book_rtt_s'] or not valid_book(payload):
            raise ValueError('unusable_book')
        return payload
    try:
        entry_book = await book()
        entry = fill(entry_book['bids'],notional=PROTOCOL['notional_usd'])
        if entry is None:
            save(db,eid,'unfilled_entry',data); return
        data.update(entry=entry, entry_ms=int(time.time()*1000))
        save(db,eid,'open',data)
        start = last = time.monotonic()
        while True:
            if cfg.stop_path.exists():
                save(db,eid,'interrupted',data); return
            await asyncio.sleep(PROTOCOL['sample_s'])
            current = await book()
            now = time.monotonic()
            if now-last > PROTOCOL['max_sampling_gap_s']:
                raise ValueError('sampling_gap')
            last = now
            exit_fill = fill(current['asks'],quantity=entry['quantity'])
            if exit_fill is None:
                raise ValueError('insufficient_exit_depth')
            stopped = float(current['asks'][0][0]) >= entry['price']*(1+PROTOCOL['stop_bps']/10000)
            if stopped or now-start >= PROTOCOL['horizon_s']:
                fee = PROTOCOL['fee_bps_per_side']/10000*(entry['value']+exit_fill['value'])
                pnl = entry['value']-exit_fill['value']-fee
                data.update(exit=exit_fill, exit_ms=int(time.time()*1000), pnl_usd=pnl,
                            net_bps=pnl/entry['value']*10000, reason='sampled_stop' if stopped else 'horizon')
                save(db,eid,'closed',data);return
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        data['error'] = str(exc)
        save(db,eid,'invalid',data)
    except asyncio.CancelledError:
        save(db,eid,'interrupted',data)
        raise


async def collect(path):
    cfg=Config.from_env()
    lock=SingleInstance(path.with_suffix('.lock'))
    if not lock.acquire():
        raise SystemExit('Study already running')
    db=open_study(path)
    # No reconstructed fills across a process outage.
    db.execute("UPDATE events SET status='interrupted' WHERE status IN ('pending','open')");db.commit()
    started=int(db.execute("SELECT value FROM meta WHERE key='started_ms'").fetchone()[0])
    tasks=set()
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            while not cfg.stop_path.exists():
                count=db.execute('SELECT COUNT(*) FROM events WHERE status != "excluded"').fetchone()[0]
                if count >= PROTOCOL['final_clusters'] or time.time()*1000-started >= PROTOCOL['max_days']*86400000:
                    break
                if cfg.observations_path.exists():
                    for line in cfg.observations_path.read_text().splitlines():
                        try: event=json.loads(line)
                        except ValueError: continue  # A writer may still be appending the last line.
                        eid=str(event.get('id',''))
                        if not eid or db.execute('SELECT 1 FROM events WHERE id=?',(eid,)).fetchone():continue
                        if event.get('detected_ms',0) < started:continue
                        symbols=sorted(extract_symbols(event.get('title','')))
                        age=time.time()*1000-event.get('detected_ms',0)
                        eligible=(categorize(event.get('title',''))==PROTOCOL['market'] and symbols
                                  and event.get('clock_synced') and 0 <= event.get('detect_lag_s',-1) <= PROTOCOL['max_detection_age_s']
                                  and 0 <= age <= 5000)
                        if not eligible:
                            save(db,eid,'excluded',dict(event,exclusion='market_symbols_clock_or_age'));continue
                        event['symbol']=symbols[0]  # One predeclared alphabetic symbol per publication.
                        save(db,eid,'pending',event)
                        task=asyncio.create_task(observe(db,event,client,cfg));tasks.add(task);task.add_done_callback(tasks.discard)
                        count+=1
                        if count >= PROTOCOL['final_clusters']:break
                await asyncio.sleep(.5)
            if tasks:await asyncio.gather(*tasks)
    finally:
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        db.close();lock.release()


def report(path):
    db=sqlite3.connect(f'file:{path}?mode=ro',uri=True)
    meta=dict(db.execute('SELECT key,value FROM meta'))
    rows=list(db.execute('SELECT status,payload FROM events'))
    counts={}
    for status,_ in rows:counts[status]=counts.get(status,0)+1
    closed=[json.loads(p) for s,p in rows if s=='closed']
    n=sum(v for k,v in counts.items() if k!='excluded')
    protocol=json.loads(meta['protocol'])
    elapsed=(time.time()*1000-int(meta['started_ms']))/86400000
    finished=(n>=protocol['final_clusters'] or elapsed>=protocol['max_days']) and not any(counts.get(k) for k in ('pending','open'))
    result=dict(protocol_sha256=hashlib.sha256(meta['protocol'].encode()).hexdigest(),counts=counts,
                stage='final' if finished else 'interim',
                decision='insufficient_evidence', disclaimer='Displayed depth fills are hypothetical; sampled stops can miss intrasecond spikes.')
    if finished and closed:
        from research_core import cluster_bootstrap
        values=[(x['id'],x['net_bps']) for x in closed]
        result['mean_net_bps']=sum(v for _,v in values)/len(values)
        result['ci95_bps']=cluster_bootstrap(values,n=10000)
        result['coverage']=len(closed)/max(1,n)
        if len(closed)>=protocol['final_clusters'] and result['coverage']==1 and result['ci95_bps'][0]>20:
            result['decision']='candidate_for_independent_execution_review'
    print(json.dumps(result,ensure_ascii=False,indent=2));db.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['collect','report'])
    parser.add_argument('--db',type=Path,default=ROOT/'state/forward.sqlite')
    args=parser.parse_args()
    if args.command=='collect':asyncio.run(collect(args.db.resolve()))
    else:report(args.db.resolve())
