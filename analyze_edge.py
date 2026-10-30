#!/usr/bin/env python3
"""Разбор распределения сделок: прибыль — это эдж или лотерея?

Берёт кэш 1m свечей (data/fut_klines_1m.json), пересчитывает сделки для заданной
конфигурации БЕЗ сети и отвечает на три вопроса:
  1. какая доля сделок прибыльна;
  2. сколько прибыли дают 1, 3, 5 лучших сделок;
  3. что остаётся, если выбросить лучшие сделки (устойчивость).

Запуск: ./venv/bin/python analyze_edge.py --lag 130 --stop 100 --horizon 300
"""

from __future__ import annotations

import argparse
import json
import statistics as st
from pathlib import Path

import backtest_futures as bt

ROOT = Path(__file__).resolve().parent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lag", type=int, default=130)
    ap.add_argument("--stop", type=int, default=100)
    ap.add_argument("--horizon", type=int, default=300)
    ap.add_argument("--cost", type=float, default=10.0)
    args = ap.parse_args()

    cache = json.loads((ROOT / "data" / "fut_klines_1m.json").read_text("utf-8"))
    events = []
    for key, series in cache.items():
        sym, rel = key.split(":")
        rel = int(rel)
        if not series:
            continue
        pre = [k for k in series if int(k[0]) < rel]
        post = [k for k in series if int(k[0]) >= rel]
        if not pre or not post:
            continue
        if sum(float(k[5]) for k in pre) <= 0:
            continue
        events.append({"symbol": sym, "release_ms": rel, "series": series})
    events.sort(key=lambda e: e["release_ms"])

    trades = []
    eq = bt.EQUITY0
    for ev in events:
        r = bt.simulate(ev["series"], ev["release_ms"], args.lag, args.stop,
                        args.horizon, args.cost)
        if isinstance(r, str):
            continue
        notional = (min(eq * bt.RISK_PCT / (args.stop / 10000.0), eq * bt.MAX_LEVERAGE)
                    if args.stop else eq * 0.5)
        pnl = notional * r["net_bps"] / 10000.0
        trades.append({"symbol": ev["symbol"], "ts": ev["release_ms"],
                       "net_bps": r["net_bps"], "notional": notional, "pnl": pnl})
        eq += pnl

    if not trades:
        print("сделок нет")
        return 1

    trades.sort(key=lambda t: t["pnl"], reverse=True)
    total = sum(t["pnl"] for t in trades)
    nets = [t["net_bps"] for t in trades]

    print(f"конфигурация: lag={args.lag}s stop={args.stop}bps horizon={args.horizon}s "
          f"cost={args.cost}bps")
    print(f"сделок: {len(trades)} | капитал: ${bt.EQUITY0:.0f} -> ${bt.EQUITY0 + total:.2f} "
          f"({total:+.2f})")
    print(f"прибыльных: {sum(1 for x in nets if x > 0)} ({sum(1 for x in nets if x > 0) / len(nets):.0%})")
    print(f"медиана {st.median(nets):.1f} бп | среднее {st.mean(nets):.1f} бп")

    print(f"\nраспределение net_bps: p10={sorted(nets)[len(nets)//10]:.0f} "
          f"p25={sorted(nets)[len(nets)//4]:.0f} p50={st.median(nets):.0f} "
          f"p75={sorted(nets)[3*len(nets)//4]:.0f} p90={sorted(nets)[9*len(nets)//10]:.0f} "
          f"max={max(nets):.0f} min={min(nets):.0f}")

    print("\nТОП-10 сделок:")
    for t in trades[:10]:
        print(f"  {t['symbol']:10s} net={t['net_bps']:9.1f} бп  P&L=${t['pnl']:9.2f}")

    print("\nконцентрация прибыли:")
    for n in (1, 2, 3, 5, 10):
        top = sum(t["pnl"] for t in trades[:n])
        print(f"  топ-{n:2d} сделок: ${top:9.2f} = {top / total * 100 if total else 0:6.0f}% всего P&L")

    print("\nустойчивость: P&L без лучших сделок")
    for n in (0, 1, 2, 3, 5):
        rest = trades[n:]
        pnl = sum(t["pnl"] for t in rest)
        print(f"  без топ-{n}: ${pnl:9.2f} на {len(rest)} сделках")

    # медианная сделка в деньгах при фиксированной позиции $1000
    med = st.median(nets)
    print(f"\nпри позиции $1000 медианная сделка = ${med * 1000 / 10000:.2f}, "
          f"средняя = ${st.mean(nets) * 1000 / 10000:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
