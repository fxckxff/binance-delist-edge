#!/usr/bin/env python3
"""Проверка гипотезы: прибыль даёт только делистинг ФЬЮЧЕРСОВ, а не удаление пар.

Каталог 161 смешивает разные события:
  * «Binance Futures Will Delist X Perpetual Contracts» — механизм жёсткий:
    контракт прекращает торговаться, держатели обязаны закрыться;
  * «Notice of Removal of Spot Trading Pairs» / «Margin Trading Pairs» — спот
    и маржа, фьючерс часто продолжает жить, падения может не быть вовсе.

Скрипт делит торгуемые события по типу анонса и считает сетку для каждого класса.
Сеть нужна только на 9 запросов анонсов; свечи берутся из кэша.

Запуск: ./venv/bin/python split_by_type.py
"""

from __future__ import annotations

import asyncio
import json
import statistics as st
from pathlib import Path

import httpx

import backtest_futures as bt

ROOT = Path(__file__).resolve().parent


async def titles(pages: int = 9) -> dict[str, str]:
    out: dict[str, str] = {}
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "newsbot-bt/2.0"}) as c:
        for page in range(1, pages + 1):
            r = await c.get(bt.ANN, params={"type": 1, "pageNo": page,
                                            "pageSize": 50, "catalogId": 161})
            if r.status_code != 200:
                break
            arts = r.json()["data"]["catalogs"][0]["articles"]
            if not arts:
                break
            for a in arts:
                rel = a.get("releaseDate")
                title = (a.get("title") or "").strip()
                if not rel:
                    continue
                for sym in bt.symbols_from_title(title)[:3]:
                    out[f"{sym}:{int(rel)}"] = title
            await asyncio.sleep(0.4)
    return out


def classify(title: str) -> str:
    t = title.lower()
    if "futures" in t and ("delist" in t or "delivery" in t):
        return "futures_delist"
    if "margin" in t:
        return "margin_removal"
    if "spot" in t and ("pair" in t or "removal" in t):
        return "spot_removal"
    if "alpha" in t:
        return "alpha_removal"
    return "other"


def stats(trades: list[dict], label: str) -> None:
    if not trades:
        print(f"  {label:16s}: нет сделок")
        return
    nets = [t["net_bps"] for t in trades]
    total = sum(t["pnl"] for t in trades)
    ordered = sorted(trades, key=lambda t: t["pnl"], reverse=True)
    top5 = sum(t["pnl"] for t in ordered[:5])
    print(f"  {label:16s}: n={len(trades):3d} win={sum(1 for x in nets if x > 0) / len(nets):4.0%} "
          f"мед={st.median(nets):7.1f} бп сред={st.mean(nets):7.1f} бп "
          f"P&L=${total:8.2f} топ5={top5 / total * 100 if total else 0:6.0f}%")


async def main() -> int:
    tmap = await titles()
    cache = json.loads((ROOT / "data" / "fut_klines_1m.json").read_text("utf-8"))

    events: list[dict] = []
    for key, series in cache.items():
        if not series:
            continue
        sym, rel = key.split(":")
        rel = int(rel)
        pre = [k for k in series if int(k[0]) < rel]
        post = [k for k in series if int(k[0]) >= rel]
        if not pre or not post or sum(float(k[5]) for k in pre) <= 0:
            continue
        events.append({"symbol": sym, "release_ms": rel, "series": series,
                       "title": tmap.get(key, ""), "kind": classify(tmap.get(key, ""))})
    events.sort(key=lambda e: e["release_ms"])

    print(f"торгуемых событий: {len(events)}")
    from collections import Counter
    print("по типу анонса:", dict(Counter(e["kind"] for e in events)))

    for lag, stop, hor in ((130, 100, 300), (130, 200, 300), (60, 100, 300), (1, 100, 300)):
        print(f"\nлаг {lag}s, стоп {stop} бп, горизонт {hor}s")
        for kind in ("futures_delist", "spot_removal", "margin_removal", "alpha_removal", "other"):
            subset = [e for e in events if e["kind"] == kind]
            trades = []
            eq = bt.EQUITY0
            for ev in subset:
                r = bt.simulate(ev["series"], ev["release_ms"], lag, stop, hor, bt.COST_BPS)
                if isinstance(r, str):
                    continue
                notional = min(eq * bt.RISK_PCT / (stop / 10000.0), eq * bt.MAX_LEVERAGE)
                pnl = notional * r["net_bps"] / 10000.0
                trades.append({"symbol": ev["symbol"], "net_bps": r["net_bps"], "pnl": pnl})
                eq += pnl
            stats(trades, kind)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
