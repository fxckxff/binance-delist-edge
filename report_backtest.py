#!/usr/bin/env python3
"""Печать результатов бэктеста в читаемом виде + выборка по «реалистичному» лагу.

Читает data/delist_backtest.json (или путь из argv) и печатает:
  * сводку по данным (сколько событий, сколько с фьючерсами);
  * полную сетку;
  * отдельно строки с лагом 60-130 с — это наш реальный диапазон обнаружения;
  * лучшую по медиане конфигурацию среди торгуемых (stop>0).

Запуск: ./venv/bin/python report_backtest.py [путь.json]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "delist_backtest.json"
    d = json.loads(path.read_text("utf-8"))
    grid = d.get("grid") or {}

    print(f"файл: {path}")
    print(f"сгенерирован: {d.get('generated_at')}")
    print(f"анонсов: {d.get('announcements')} | кандидатов символ×анонс: {d.get('candidates')}")
    print(f"с данными: {d.get('with_data')} | фьючерс: {d.get('futures')} | только спот: {d.get('spot_only')}")
    print(f"комиссия round trip: {d.get('cost_bps_roundtrip')} бп | капитал ${d.get('equity0')} "
          f"| риск {d.get('risk_pct', 0) * 100:.0f}% | плечо ≤{d.get('max_leverage')}x")

    if not grid:
        print("\nсетка пуста — прогон не дал сделок")
        return 1

    hdr = (f"{'лаг':>4} {'стоп':>5} {'гориз':>6} {'сделок':>7} {'win%':>6} "
           f"{'мед.бп':>8} {'сред.бп':>9} {'P&L$':>9} {'DD%':>7} {'позиция$':>9}")
    line = "-" * 84

    rows = sorted(grid.values(), key=lambda v: (v["lag_s"], v["stop_bps"], v["horizon_s"]))
    print(f"\nПОЛНАЯ СЕТКА\n{hdr}\n{line}")
    for v in rows:
        print(f"{v['lag_s']:>4} {v['stop_bps']:>5} {v['horizon_s']:>6} {v['trades']:>7} "
              f"{v['win_rate'] * 100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
              f"{v['total_pnl_usd']:>9.2f} {v['max_dd_pct']:>6.1f}% {v['avg_notional']:>9.0f}")

    real = [v for v in rows if 60 <= v["lag_s"] <= 130]
    if real:
        print(f"\nРЕАЛЬНЫЙ ДИАПАЗОН ЛАГА (60-130 с, из-за кэша CloudFront)\n{hdr}\n{line}")
        for v in real:
            print(f"{v['lag_s']:>4} {v['stop_bps']:>5} {v['horizon_s']:>6} {v['trades']:>7} "
                  f"{v['win_rate'] * 100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
                  f"{v['total_pnl_usd']:>9.2f} {v['max_dd_pct']:>6.1f}% {v['avg_notional']:>9.0f}")

    tradable = [v for v in rows if v["stop_bps"] > 0]
    if tradable:
        best = max(tradable, key=lambda v: v["median_net_bps"])
        print(f"\nлучшая по медиане среди конфигураций со стопом: "
              f"lag={best['lag_s']}s stop={best['stop_bps']}bps horizon={best['horizon_s']}s "
              f"-> медиана {best['median_net_bps']:.1f} бп, P&L ${best['total_pnl_usd']:.2f}, "
              f"DD {best['max_dd_pct']:.1f}%")
        pos = [v for v in tradable if v["total_pnl_usd"] > 0]
        print(f"конфигураций со стопом и положительным P&L: {len(pos)} из {len(tradable)}")
        print(f"конфигураций со стопом и медианой > 0: "
              f"{sum(1 for v in tradable if v['median_net_bps'] > 0)} из {len(tradable)}")

    cav = d.get("caveats") or []
    if cav:
        print("\nОГРАНИЧЕНИЯ (из отчёта):")
        for c in cav:
            print(f"  - {c}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
