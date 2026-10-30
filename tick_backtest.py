#!/usr/bin/env python3
"""Тиковый бэктест: снимает неопределённость минутных свечей.

Проблема, которую решает
------------------------
На минутных свечах неизвестно, что было раньше внутри минуты — стоп или вход.
Отсюда разрыв между пессимистичной и оптимистичной оценкой в 6.6x, и по таким
данным нельзя ответить, есть ли эдж.

Решение: реальные сделки (aggTrades) с data.binance.vision — официальный архив
Binance, бесплатный, с точностью до миллисекунды и в правильном порядке.
Для делистингованных пар файлы сохранились (проверено: ALPACAUSDT 24.04.2025).

Что делает
----------
1. Берёт торгуемые события (фьючерс был, объём до анонса был).
2. Качает дневной архив aggTrades за дату анонса (+следующий день при переходе
   через полночь), вырезает окно ±10 минут и выбрасывает остальное.
3. Вход: цена первой реальной сделки после (анонс + лаг).
4. Стоп: проверяется по КАЖДОЙ сделке в хронологическом порядке — без догадок.
5. Выход: цена последней сделки в пределах горизонта.
6. Сетка по лагу, стопу и горизонту + бутстрэп ДИ.

Запуск:
    ./venv/bin/python tick_backtest.py --limit 5      # проверка на пяти
    ./venv/bin/python tick_backtest.py                # все события
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import random
import statistics as st
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
BASE = "https://data.binance.vision/data/futures/um/daily/aggTrades"
CACHE = ROOT / "data" / "tick_windows.json"
OUT = ROOT / "data" / "tick_backtest.json"

LAGS_S = [1, 5, 15, 30, 60, 111, 130, 252]
STOPS_BPS = [0, 100, 200, 500]
HORIZONS_S = [300, 900]
COST_BPS = 10.0
EQUITY0 = 1000.0
RISK_PCT = 0.01
MAX_LEVERAGE = 3.0
PRE_MS = 600_000
PAD_MS = 120_000


def day_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


async def download_day(client: httpx.AsyncClient, symbol: str, date: str) -> list[tuple[int, float]]:
    """Скачивает дневной aggTrades и возвращает [(ts_ms, price), ...]."""
    url = f"{BASE}/{symbol}/{symbol}-aggTrades-{date}.zip"
    r = await client.get(url)
    if r.status_code != 200:
        return []
    rows: list[tuple[int, float]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                for raw in io.TextIOWrapper(f, encoding="utf-8"):
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) < 6:
                        continue
                    try:
                        price = float(parts[1])
                        ts = int(parts[5])
                    except ValueError:
                        continue          # строка заголовка
                    rows.append((ts, price))
    except zipfile.BadZipFile:
        return []
    return rows


def simulate_ticks(ticks: list[tuple[int, float]], release_ms: int, lag_s: int,
                   stop_bps: int, horizon_s: int, cost_bps: float):
    """Точная симуляция на реальных сделках. Возвращает dict или причину пропуска."""
    entry_t = release_ms + lag_s * 1000
    entry_i = None
    for i, (ts, _) in enumerate(ticks):
        if ts >= entry_t:
            entry_i = i
            break
    if entry_i is None:
        return "no_entry_trade"
    entry = ticks[entry_i][1]
    if entry <= 0:
        return "bad_price"

    deadline = release_ms + horizon_s * 1000
    stop_price = entry * (1 + stop_bps / 10000.0) if stop_bps else None

    exit_price = None
    exit_ts = None
    for ts, price in ticks[entry_i:]:
        if ts > deadline:
            break
        if stop_price is not None and price >= stop_price:
            exit_price = stop_price       # стоп-маркет: исполняем по цене стопа
            exit_ts = ts
            break
        exit_price = price
        exit_ts = ts
    if exit_price is None:
        return "no_exit"

    gross = (entry - exit_price) / entry * 10000.0
    return {"entry": entry, "exit": exit_price, "gross_bps": gross,
            "net_bps": gross - cost_bps, "hold_s": (exit_ts - entry_t) / 1000.0}


def run_grid(events: list[dict], cost_bps: float) -> dict:
    results = {}
    ordered = sorted(events, key=lambda e: e["release_ms"])
    for lag in LAGS_S:
        for stop in STOPS_BPS:
            for hor in HORIZONS_S:
                trades = []
                eq = EQUITY0
                peak = eq
                dd = 0.0
                blown = False
                skipped: dict[str, int] = {}
                for ev in ordered:
                    if blown:
                        break
                    r = simulate_ticks(ev["ticks"], ev["release_ms"], lag, stop, hor, cost_bps)
                    if isinstance(r, str):
                        skipped[r] = skipped.get(r, 0) + 1
                        continue
                    notional = (min(eq * RISK_PCT / (stop / 10000.0), eq * MAX_LEVERAGE)
                                if stop else eq * 0.5)
                    pnl = notional * r["net_bps"] / 10000.0
                    if eq + pnl <= 0:
                        pnl = -eq
                        blown = True
                    trades.append({**r, "symbol": ev["symbol"], "notional": notional, "pnl": pnl})
                    eq += pnl
                    peak = max(peak, eq)
                    dd = max(dd, (peak - eq) / peak if peak > 0 else 0.0)
                if not trades:
                    continue
                nets = [t["net_bps"] for t in trades]
                ordered_pnl = sorted(trades, key=lambda t: t["pnl"], reverse=True)
                total = sum(t["pnl"] for t in trades)
                results[f"lag{lag}_stop{stop}_h{hor}"] = {
                    "lag_s": lag, "stop_bps": stop, "horizon_s": hor,
                    "trades": len(trades),
                    "win_rate": sum(1 for x in nets if x > 0) / len(nets),
                    "median_net_bps": st.median(nets),
                    "mean_net_bps": st.mean(nets),
                    "total_pnl_usd": round(total, 2),
                    "max_dd_pct": round(dd * 100, 1),
                    "top5_share_pct": round(sum(t["pnl"] for t in ordered_pnl[:5]) / total * 100, 0) if total else None,
                    "account_blown": blown,
                    "skipped": skipped,
                }
    return results


def bootstrap_ci(vals: list[float], n: int = 10000) -> tuple[float, float]:
    random.seed(42)
    means = sorted(st.mean(random.choices(vals, k=len(vals))) for _ in range(n))
    return means[int(0.025 * n)], means[int(0.975 * n)]


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="только первые N событий (проверка)")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    # торгуемые события: читаем манифест data/events.json, если есть, иначе
    # выводим из длинного минутного кэша (не обязателен для воспроизведения).
    manifest = ROOT / "data" / "events.json"
    if manifest.exists():
        events = [{"symbol": e["symbol"] + "USDT", "release_ms": int(e["release_ms"])}
                  for e in json.loads(manifest.read_text("utf-8"))]
    else:
        src = json.loads((ROOT / "data" / "fut_klines_long.json").read_text("utf-8"))
        events = []
        for key, series in src.items():
            if not series:
                continue
            sym, rel = key.split(":")
            rel = int(rel)
            if sum(float(k[5]) for k in series if int(k[0]) < rel) <= 0:
                continue
            events.append({"symbol": sym + "USDT", "release_ms": rel})
    events.sort(key=lambda e: e["release_ms"])
    if args.limit:
        events = events[: args.limit]
    print(f"событий: {len(events)} | период: {day_str(events[0]['release_ms'])} .. "
          f"{day_str(events[-1]['release_ms'])}")

    cache: dict = {}
    if CACHE.exists() and not args.refresh:
        cache = json.loads(CACHE.read_text("utf-8"))
        print(f"кэш окон: {len(cache)}")

    missing = 0
    async with httpx.AsyncClient(timeout=120.0, headers={"User-Agent": "newsbot-tick/1.0"},
                                 follow_redirects=True) as c:
        sem = asyncio.Semaphore(4)

        async def load(ev: dict) -> None:
            nonlocal missing
            key = f"{ev['symbol']}:{ev['release_ms']}"
            if key in cache:
                ev["ticks"] = [tuple(x) for x in cache[key]]
                return
            async with sem:
                lo = ev["release_ms"] - PRE_MS
                hi = ev["release_ms"] + max(HORIZONS_S) * 1000 + PAD_MS
                days = {day_str(lo), day_str(hi)}
                ticks: list[tuple[int, float]] = []
                for d in sorted(days):
                    got = await download_day(c, ev["symbol"], d)
                    ticks.extend(x for x in got if lo <= x[0] <= hi)
                ticks.sort()
                ev["ticks"] = ticks
                cache[key] = [list(x) for x in ticks]
                if not ticks:
                    missing += 1
                await asyncio.sleep(0.1)

        for i, ev in enumerate(events, 1):
            await load(ev)
            if i % 10 == 0 or i == len(events):
                print(f"  {i}/{len(events)} | без данных: {missing}")
        CACHE.write_text(json.dumps(cache), "utf-8")

    events = [e for e in events if e.get("ticks")]
    print(f"\nс тиковыми данными: {len(events)} | без данных: {len(events) and missing}")
    if not events:
        return 1

    grid = run_grid(events, COST_BPS)

    print(f"\n{'лаг':>4} {'стоп':>5} {'гориз':>6} {'сделок':>7} {'win%':>6} "
          f"{'мед.бп':>8} {'сред.бп':>9} {'P&L$':>9} {'топ5%':>6} {'DD%':>6} {'слит':>5}")
    print("-" * 90)
    for v in sorted(grid.values(), key=lambda v: (v["lag_s"], v["stop_bps"], v["horizon_s"])):
        print(f"{v['lag_s']:>4} {v['stop_bps']:>5} {v['horizon_s']:>6} {v['trades']:>7} "
              f"{v['win_rate'] * 100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
              f"{v['total_pnl_usd']:>9.2f} {v['top5_share_pct']:>5.0f}% {v['max_dd_pct']:>5.1f}% "
              f"{'ДА' if v['account_blown'] else '':>5}")

    # бутстрэп по каждой конфигурации
    print(f"\n{'лаг':>4} {'стоп':>5} {'гориз':>6} {'n':>4} {'среднее':>9} {'95% ДИ':>24} {'значим':>8}")
    print("-" * 70)
    ci_rows = {}
    for lag in LAGS_S:
        for stop in STOPS_BPS:
            for hor in HORIZONS_S:
                vals = []
                for ev in sorted(events, key=lambda e: e["release_ms"]):
                    r = simulate_ticks(ev["ticks"], ev["release_ms"], lag, stop, hor, COST_BPS)
                    if not isinstance(r, str):
                        vals.append(r["net_bps"])
                if len(vals) < 10:
                    continue
                lo, hi = bootstrap_ci(vals)
                sig = "ДА" if lo > 0 else ("нет" if hi > 0 else "убыток")
                ci_rows[f"lag{lag}_stop{stop}_h{hor}"] = {
                    "n": len(vals), "mean": st.mean(vals), "ci_low": lo, "ci_high": hi,
                    "significant": sig,
                }
                print(f"{lag:>4} {stop:>5} {hor:>6} {len(vals):>4} {st.mean(vals):>9.1f} "
                      f"[{lo:>9.1f}; {hi:>9.1f}] {sig:>8}")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "data.binance.vision futures/um daily aggTrades (tick-level)",
        "events": len(events),
        "missing_files": missing,
        "cost_bps_roundtrip": COST_BPS,
        "equity0": EQUITY0,
        "grid": grid,
        "bootstrap": ci_rows,
        "caveats": [
            "вход по цене первой сделки после лага; проскальзывание сверх 10 бп не моделируется",
            "стоп исполняется по цене стопа, реальный проскок на гэпе будет хуже",
            "объём в окне может быть микроскопическим: цена реальна, но исполнимость под вопросом",
            "115 событий — вся история каталога 161; подбор параметров = переобучение",
        ],
    }
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), "utf-8")
    print(f"\nотчёт: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
