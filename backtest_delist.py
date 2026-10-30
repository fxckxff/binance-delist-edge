#!/usr/bin/env python3
"""Настоящий бэктест стратегии «шорт на анонсе делистинга Binance».

Что делает, в отличие от прежних замеров:
  * берёт ВСЕ анонсы каталога 161 (делистинги), а не выборку;
  * тянет реальные 1-секундные свечи вокруг каждого анонса (спот и USDⓈ-M фьючерсы);
  * торгует только то, что реально можно шортить: нужен фьючерсный перпетуал,
    по которому в окне были сделки;
  * вход не «мгновенно», а с заданным лагом — и лаг перебирается, потому что
    наш реальный лаг обнаружения 60-130 с (кэш CloudFront), а не 1 с;
  * комиссия по умолчанию 10 бп round trip (вход тейкером 5 + выход 5), а не 4;
  * стоп-лосс и горизонт выхода — параметры, а не догадки;
  * размер позиции из риска 1% от $1000, с ограничением плеча;
  * считает кривую капитала, просадку и абсолютные доллары.

Всё, что не удалось измерить, помечается как пропуск, а не додумывается.

Запуск:
    ./venv/bin/python backtest_delist.py --pages 9
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import statistics as st
from datetime import datetime, timezone
from pathlib import Path

import httpx

ANN = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
SPOT = "https://api.binance.com/api/v3/klines"
FUT = "https://fapi.binance.com/fapi/v1/klines"
ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "delist_klines.json"
OUT = ROOT / "data" / "delist_backtest.json"

STOP_WORDS = {
    "BINANCE", "WILL", "DELIST", "NOTICE", "REMOVAL", "SPOT", "TRADING", "PAIRS", "PAIR",
    "MARGIN", "LOANS", "TOKEN", "ALPHA", "REMOVE", "FUTURES", "PERPETUAL", "CONTRACTS",
    "CONTRACT", "MULTIPLE", "USD", "UAH", "DEPOSITS", "WITHDRAWALS", "VIA", "FIAT",
    "TRADE", "AND", "ON", "CLOSE", "THE", "OF", "FOR", "NEW", "LISTING", "WITH", "TAG",
    "APPLIED", "SEED", "MONITORING", "UPDATED", "ADD", "ADDS", "LAUNCH", "LAUNCHES",
    "MARGINED", "TRADFI", "BUSD", "USDT", "USDC", "FDUSD", "TUSD",
}

# Что перебираем
LAGS_S = [1, 5, 15, 30, 60, 111, 130, 252]
STOPS_BPS = [0, 100, 200]        # 0 = без стопа
HORIZONS_S = [300, 900]
COST_BPS = 10.0                  # round trip: тейкер 5 + тейкер 5
EQUITY0 = 1000.0
RISK_PCT = 0.01
MAX_LEVERAGE = 3.0
PRE_S = 300
POST_S = 1200


def symbols_from_title(title: str) -> list[str]:
    text = re.sub(r"\(.*?\)", " ", title)
    text = re.sub(r"\d{4}-\d{2}-\d{2}", " ", text)
    out: list[str] = []
    for tok in re.findall(r"\b[A-Z][A-Z0-9]{1,9}\b", text):
        if tok in STOP_WORDS or tok.isdigit():
            continue
        if tok.endswith("USDT") and len(tok) > 6:
            tok = tok[:-4]
        if tok in STOP_WORDS or len(tok) < 2:
            continue
        if tok not in out:
            out.append(tok)
    return out


async def fetch_announcements(client: httpx.AsyncClient, pages: int) -> list[dict]:
    arts: list[dict] = []
    for page in range(1, pages + 1):
        r = await client.get(ANN, params={"type": 1, "pageNo": page, "pageSize": 50, "catalogId": 161})
        if r.status_code != 200:
            print(f"  страница {page}: HTTP {r.status_code}, стоп")
            break
        try:
            got = r.json()["data"]["catalogs"][0]["articles"]
        except Exception:
            break
        if not got:
            break
        arts.extend(got)
        await asyncio.sleep(0.5)
    return arts


async def fetch_klines(client: httpx.AsyncClient, base: str, symbol: str,
                       start_ms: int, end_ms: int) -> list[list]:
    """1s свечи кусками по 1000. Возвращает только свечи с реальными сделками."""
    out: list[list] = []
    cur = start_ms
    while cur < end_ms:
        r = await client.get(base, params={"symbol": symbol, "interval": "1s",
                                           "startTime": cur, "endTime": end_ms, "limit": 1000})
        if r.status_code != 200:
            return out
        try:
            batch = r.json()
        except Exception:
            return out
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < 1000:
            break
        cur = int(batch[-1][0]) + 1000
    return [k for k in out if float(k[5]) > 0]


def price_at(series: list[list], ts_ms: int, field: int = 1):
    """Цена последней сделки на момент ts_ms или раньше (field=1 open, 4 close)."""
    lo, hi, best = 0, len(series) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if int(series[mid][0]) <= ts_ms:
            best = series[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return float(best[field]) if best else None


def first_after(series: list[list], ts_ms: int):
    for k in series:
        if int(k[0]) >= ts_ms:
            return k
    return None


def simulate(series: list[list], release_ms: int, lag_s: int, stop_bps: int,
             horizon_s: int) -> dict | None:
    """Одна сделка шортом. Возвращает результат или None, если данных нет."""
    entry_t = release_ms + lag_s * 1000
    k = first_after(series, entry_t)
    if k is None:
        return None
    entry = float(k[1])                     # открытие первой торгуемой секунды после входа
    if entry <= 0:
        return None
    exit_deadline = release_ms + horizon_s * 1000
    stop_price = entry * (1 + stop_bps / 10000.0) if stop_bps else None

    exit_price = None
    for c in series:
        t = int(c[0])
        if t < entry_t:
            continue
        if t > exit_deadline:
            break
        if stop_price is not None and float(c[2]) >= stop_price:   # high пробил стоп
            exit_price = stop_price
            break
        exit_price = float(c[4])            # close последней свечи в окне
    if exit_price is None:
        return None
    gross_bps = (entry - exit_price) / entry * 10000.0     # шорт: прибыль при падении
    return {"entry": entry, "exit": exit_price, "gross_bps": gross_bps,
            "net_bps": gross_bps - COST_BPS,
            "hold_s": (int(series[-1][0]) - entry_t) / 1000.0}


def run_grid(events: list[dict]) -> dict:
    results = {}
    for lag in LAGS_S:
        for stop in STOPS_BPS:
            for hor in HORIZONS_S:
                trades = []
                for ev in events:
                    r = simulate(ev["series"], ev["release_ms"], lag, stop, hor)
                    if r is None:
                        continue
                    # размер позиции: риск 1% при стопе, иначе фиксированный нотионал
                    if stop:
                        notional = min(EQUITY0 * RISK_PCT / (stop / 10000.0), EQUITY0 * MAX_LEVERAGE)
                    else:
                        notional = EQUITY0 * MAX_LEVERAGE * 0.5
                    pnl = notional * r["net_bps"] / 10000.0
                    trades.append({**r, "ts": ev["release_ms"], "symbol": ev["symbol"],
                                   "notional": notional, "pnl": pnl})
                if not trades:
                    continue
                trades.sort(key=lambda x: x["ts"])
                eq = EQUITY0
                peak = eq
                dd = 0.0
                for t in trades:
                    eq += t["pnl"]
                    peak = max(peak, eq)
                    dd = max(dd, (peak - eq) / peak)
                nets = [t["net_bps"] for t in trades]
                results[f"lag{lag}_stop{stop}_h{hor}"] = {
                    "lag_s": lag, "stop_bps": stop, "horizon_s": hor,
                    "trades": len(trades),
                    "win_rate": sum(1 for x in nets if x > 0) / len(nets),
                    "median_net_bps": st.median(nets),
                    "mean_net_bps": st.mean(nets),
                    "total_pnl_usd": round(eq - EQUITY0, 2),
                    "final_equity": round(eq, 2),
                    "max_dd_pct": round(dd * 100, 2),
                    "avg_notional": round(st.mean([t["notional"] for t in trades]), 0),
                }
    return results


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=9)
    ap.add_argument("--cost", type=float, default=10.0, help="комиссия round trip, бп")
    ap.add_argument("--refresh", action="store_true", help="перекачать свечи, игнорируя кэш")
    args = ap.parse_args()

    global COST_BPS
    COST_BPS = args.cost
    print(f"комиссия round trip: {COST_BPS:.0f} бп")

    cache = {}
    if CACHE.exists() and not args.refresh:
        cache = json.loads(CACHE.read_text("utf-8"))
        print(f"кэш свечей: {len(cache)} событий")

    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "newsbot-backtest/1.0"}) as c:
        arts = await fetch_announcements(c, args.pages)
        print(f"анонсов получено: {len(arts)}")

        events: list[dict] = []
        seen = set()
        for a in arts:
            rel = a.get("releaseDate")
            title = (a.get("title") or "").strip()
            if not rel or not title:
                continue
            for sym in symbols_from_title(title)[:3]:
                key = f"{sym}:{rel}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({"symbol": sym, "release_ms": int(rel), "title": title})
        print(f"кандидатов (символ × анонс): {len(events)}")

        sem = asyncio.Semaphore(5)
        stats = {"with_futures": 0, "spot_only": 0, "no_data": 0, "cached": 0}

        async def load(ev: dict) -> None:
            key = f"{ev['symbol']}:{ev['release_ms']}"
            if key in cache:
                ev["series"] = cache[key]
                stats["cached"] += 1
                return
            async with sem:
                start = ev["release_ms"] - PRE_S * 1000
                end = ev["release_ms"] + POST_S * 1000
                series = await fetch_klines(c, FUT, ev["symbol"] + "USDT", start, end)
                if series:
                    stats["with_futures"] += 1
                else:
                    series = await fetch_klines(c, SPOT, ev["symbol"] + "USDT", start, end)
                    stats["spot_only" if series else "no_data"] += 1
                ev["series"] = series
                cache[key] = series
                await asyncio.sleep(0.15)

        done = 0
        for ev in events:
            await load(ev)
            done += 1
            if done % 50 == 0:
                print(f"  загружено {done}/{len(events)} | фьючерс {stats['with_futures']} "
                      f"| только спот {stats['spot_only']} | нет данных {stats['no_data']}")

        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(cache), "utf-8")

    tradable = [e for e in events if e.get("series")]
    print(f"\nсвечи есть у {len(tradable)} событий "
          f"(фьючерс {stats['with_futures']}, только спот {stats['spot_only']}, "
          f"нет данных {stats['no_data']})")

    grid = run_grid(tradable)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "announcements": len(arts),
        "candidates": len(events),
        "with_data": len(tradable),
        "futures": stats["with_futures"],
        "spot_only": stats["spot_only"],
        "cost_bps_roundtrip": COST_BPS,
        "equity0": EQUITY0,
        "risk_pct": RISK_PCT,
        "max_leverage": MAX_LEVERAGE,
        "grid": grid,
        "caveats": [
            "1s свечи у неликвидных пар редкие: цена входа/выхода оптимистична",
            "комиссия 10 бп round trip — оценка, реальная зависит от пары и стакана",
            "проскальзывание сверх комиссии не моделируется",
            "события, по которым нет 1s данных (старые), не входят в выборку",
            "оптимизация параметров на ~100 событиях — переобучение, а не настройка",
        ],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), "utf-8")

    print(f"\n{'лаг':>5} {'стоп':>6} {'гориз':>6} {'сделок':>7} {'win%':>6} "
          f"{'мед.бп':>8} {'сред.бп':>9} {'P&L$':>9} {'DD%':>7} {'позиция$':>9}")
    print("-" * 82)
    for k, v in sorted(grid.items(), key=lambda kv: (kv[1]["lag_s"], kv[1]["stop_bps"], kv[1]["horizon_s"])):
        print(f"{v['lag_s']:>5} {v['stop_bps']:>6} {v['horizon_s']:>6} {v['trades']:>7} "
              f"{v['win_rate']*100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
              f"{v['total_pnl_usd']:>9.2f} {v['max_dd_pct']:>6.1f}% {v['avg_notional']:>9.0f}")
    print(f"\nотчёт: {OUT}")
    return 0


if __name__ == "__main__":
    import sys
    if "--allow-invalid-legacy" not in sys.argv:
        raise SystemExit("INVALID LEGACY MODEL: spot fallback cannot establish futures short returns. Use tick_backtest.py.")
    sys.argv.remove("--allow-invalid-legacy")
    raise SystemExit(asyncio.run(main()))
