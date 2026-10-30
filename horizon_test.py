#!/usr/bin/env python3
"""Медленный горизонт: делистинг фьючерса — механизм на дни, а не на минуты.

Проверяем гипотезу движения после анонса на часах и днях. Сам делистинг
не гарантирует направление цены. Неполные горизонты отклоняются; settlement,
funding и фактический механизм ликвидации не моделируются.

Что считает:
  * вход: открытие первой минуты после (анонс + лаг);
  * выход: последнее закрытие минуты не позднее release + H, где H из списка;
  * стоп проверяется по high минутных свечей (пессимистично: сработал, если high пробил);
  * только события, у которых был фьючерсный контракт и объём до анонса.

Данные: 1m USDⓈ-M klines, окно release-10m .. release+7d. Кэш отдельный.

Запуск: ./venv/bin/python horizon_test.py
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from research_core import portfolio
from pathlib import Path

import httpx

import backtest_futures as bt

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "fut_klines_long.json"
OUT = ROOT / "data" / "horizon_test.v2.json"

HORIZONS_S = [900, 3600, 14400, 86400, 259200, 604800]
STOPS_BPS = [0, 200, 500]
LAGS_S = [130, 252]
PRE_S = 600
POST_S = 7 * 86400


async def fetch_long(client: httpx.AsyncClient, symbol: str, start_ms: int, end_ms: int):
    out: list[list] = []
    cur = start_ms
    while cur < end_ms:
        r = await client.get(bt.FUT, params={"symbol": symbol, "interval": "1m",
                                             "startTime": cur, "endTime": end_ms, "limit": 1000})
        if r.status_code == 400:
            return [], "no_contract"
        if r.status_code != 200:
            return out, f"http_{r.status_code}"
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < 1000:
            break
        cur = int(batch[-1][0]) + 60_000
        await asyncio.sleep(0.08)
    return out, "ok"


def simulate_long(series: list[list], release_ms: int, lag_s: int, stop_bps: int, horizon_s: int):
    return bt.simulate(series, release_ms, lag_s, stop_bps, horizon_s, bt.COST_BPS)


async def main() -> int:
    cache: dict = {}
    if CACHE.exists():
        cache = json.loads(CACHE.read_text("utf-8"))
        print(f"кэш длинных свечей: {len(cache)}")

    # берём только события, по которым уже известно, что контракт был
    short_cache = json.loads((ROOT / "data" / "fut_klines_1m.json").read_text("utf-8"))
    events = []
    for key, series in short_cache.items():
        if not series:
            continue
        sym, rel = key.split(":")
        rel = int(rel)
        if sum(float(k[5]) for k in series if int(k[6]) <= rel) <= 0:
            continue
        events.append({"symbol": sym, "release_ms": rel})
    events.sort(key=lambda e: e["release_ms"])
    print(f"событий с контрактом и объёмом: {len(events)}")

    async with httpx.AsyncClient(timeout=40.0, headers={"User-Agent": "newsbot-bt/2.0"}) as c:
        sem = asyncio.Semaphore(4)

        async def load(ev: dict) -> None:
            key = f"{ev['symbol']}:{ev['release_ms']}"
            if cache.get(key):
                ev["series"] = cache[key]
                return
            async with sem:
                series, status = await fetch_long(c, ev["symbol"] + "USDT",
                                                  ev["release_ms"] - PRE_S * 1000,
                                                  ev["release_ms"] + POST_S * 1000)
                ev["series"] = series if status == "ok" else []
                if status == "ok" and ev["series"]:
                    cache[key] = ev["series"]
                await asyncio.sleep(0.1)

        for i, ev in enumerate(events, 1):
            await load(ev)
            if i % 20 == 0:
                print(f"  {i}/{len(events)}")
        CACHE.write_text(json.dumps(cache), "utf-8")

    tradable = [e for e in events if e.get("series")]
    print(f"с длинными данными: {len(tradable)}")

    grid = {}
    for lag in LAGS_S:
        for stop in STOPS_BPS:
            for hor in HORIZONS_S:
                row = portfolio(tradable, lambda ev: simulate_long(ev["series"], ev["release_ms"],
                                lag, stop, hor), stop, bt.COST_BPS,
                                equity0=bt.EQUITY0, risk=bt.RISK_PCT, leverage=bt.MAX_LEVERAGE)
                if row:
                    grid[f"lag{lag}_stop{stop}_h{hor}"] = dict(row, lag_s=lag, stop_bps=stop, horizon_s=hor)

    OUT.write_text(json.dumps({"model_version": 2, "generated_at": datetime.now(timezone.utc).isoformat(), "tradable": len(tradable), "grid": grid},
                              ensure_ascii=False, indent=2), "utf-8")

    print(f"\n{'лаг':>4} {'стоп':>5} {'горизонт':>9} {'сделок':>7} {'win%':>6} "
          f"{'мед.бп':>8} {'сред.бп':>9} {'P&L$':>9} {'топ5%':>6} {'DD%':>6} {'слит':>5}")
    print("-" * 88)
    for v in sorted(grid.values(), key=lambda v: (v["lag_s"], v["stop_bps"], v["horizon_s"])):
        h = v["horizon_s"]
        hs = f"{h // 86400}д" if h >= 86400 else f"{h // 60}м"
        print(f"{v['lag_s']:>4} {v['stop_bps']:>5} {hs:>9} {v['trades']:>7} "
              f"{v['win_rate'] * 100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
              f"{v['total_pnl_usd']:>9.2f} {v['top5_share_pct']:>5.0f}% {v['max_dd_pct']:>5.1f}% "
              f"{'ДА' if v.get('account_blown') else '':>5}")
    print(f"\nотчёт: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
