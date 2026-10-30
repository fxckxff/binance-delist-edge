#!/usr/bin/env python3
"""Сводка по фьючерсному бэктесту: пессимистичная и оптимистичная границы рядом.

Читает data/fut_backtest.json (консервативно: стоп может сработать в минуту входа)
и data/fut_backtest_opt.json (оптимистично: стоп со следующей минуты).

Печатает по каждой конфигурации медиану net bps и P&L в обоих вариантах,
чтобы было видно, где правда: если знак совпадает — вывод устойчив,
если расходится — данных не хватает.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def load(name: str) -> dict | None:
    p = DATA / name
    if not p.exists():
        return None
    return json.loads(p.read_text("utf-8"))


def main() -> int:
    cons = load("fut_backtest.json")
    opt = load("fut_backtest_opt.json")
    if cons is None:
        print("нет data/fut_backtest.json — сначала прогони backtest_futures.py")
        return 1

    print("ДАННЫЕ")
    for label, d in (("консервативно", cons), ("оптимистично", opt)):
        if not d:
            continue
        print(f"  {label:14s}: анонсов {d['announcements']}, кандидатов {d['candidates']}, "
              f"контракт был {d['with_contract']}, без объёма до анонса {d['no_pre_volume']}, "
              f"торгуемо {d['tradable']}")
    print(f"  комиссия round trip: {cons['cost_bps_roundtrip']} бп | капитал ${cons['equity0']} "
          f"| риск {cons['risk_pct'] * 100:.0f}% | плечо ≤{cons['max_leverage']}x")

    cg = cons["grid"]
    og = (opt or {}).get("grid") or {}

    print(f"\n{'лаг':>4} {'стоп':>5} {'гориз':>6} | {'сделок':>6} {'мед.бп':>8} {'P&L$':>9} "
          f"{'DD%':>6} | {'мед.бп':>8} {'P&L$':>9} {'DD%':>6}")
    print(f"{'':>16} | {'--- консервативно ---':^26} | {'--- оптимистично ---':^26}")
    print("-" * 84)

    rows = sorted(cg.values(), key=lambda v: (v["lag_s"], v["stop_bps"], v["horizon_s"]))
    for v in rows:
        key = f"lag{v['lag_s']}_stop{v['stop_bps']}_h{v['horizon_s']}"
        o = og.get(key)
        line = (f"{v['lag_s']:>4} {v['stop_bps']:>5} {v['horizon_s']:>6} | "
                f"{v['trades']:>6} {v['median_net_bps']:>8.1f} {v['total_pnl_usd']:>9.2f} "
                f"{v['max_dd_pct']:>5.1f}%")
        if o:
            line += f" | {o['median_net_bps']:>8.1f} {o['total_pnl_usd']:>9.2f} {o['max_dd_pct']:>5.1f}%"
        print(line)

    # устойчивость: где обе границы положительны
    both_pos = []
    for v in rows:
        key = f"lag{v['lag_s']}_stop{v['stop_bps']}_h{v['horizon_s']}"
        o = og.get(key)
        if o and v["total_pnl_usd"] > 0 and o["total_pnl_usd"] > 0 and v["stop_bps"] > 0:
            both_pos.append((v, o))
    print(f"\nконфигураций со стопом, где P&L > 0 в ОБОИХ вариантах: {len(both_pos)}")
    for v, o in both_pos:
        print(f"  lag={v['lag_s']:3d}s stop={v['stop_bps']:3d} h={v['horizon_s']:3d}s: "
              f"консерв ${v['total_pnl_usd']:8.2f} (мед {v['median_net_bps']:7.1f} бп), "
              f"оптим ${o['total_pnl_usd']:8.2f} (мед {o['median_net_bps']:7.1f} бп)")

    print("\nОГРАНИЧЕНИЯ")
    for c in cons.get("caveats") or []:
        print(f"  - {c}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
