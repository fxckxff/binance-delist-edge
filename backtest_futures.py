#!/usr/bin/env python3
"""Честный бэктест шорта на делистинге: ТОЛЬКО то, что реально можно шортить.

Почему переписан
----------------
Прошлый прогон (backtest_delist.py) запрашивал у USDⓈ-M фьючерсов свечи с
interval=1s. Такого интервала у фьючерсов Binance нет (HTTP 400, code -1120),
код молча падал в спот — и весь результат оказался посчитан по спот-ценам.
Спот шортить нельзя (нет займа), поэтому тот отчёт неприменим.

Что делает этот файл
--------------------
1. Берёт анонсы каталога 161 (делистинги).
2. Для каждого символа запрашивает 1-МИНУТНЫЕ свечи USDⓈ-M фьючерсов вокруг анонса.
   Если контракта не было — 400, событие выбрасывается (шорт невозможен).
3. Требует, чтобы до анонса был реальный объём: иначе «цена» — это пустая книга.
4. Вход: открытие первой минутной свечи после (анонс + лаг). Лаг перебирается.
   Из-за минутной сетки фактический лаг = lag + до 60 с. Это в минус, не в плюс.
5. Стоп проверяется по high свечи, исполняется по цене стопа.
6. Выход: закрытие последней свечи в пределах горизонта.
7. Размер позиции считается от ТЕКУЩЕГО капитала (компаундинг), с потолком плеча.
8. Комиссия round trip по умолчанию 10 бп (тейкер 4+4 на фьючерсах + запас).

Запуск:
    ./venv/bin/python backtest_futures.py --pages 9
    ./venv/bin/python backtest_futures.py --pages 9 --cost 20
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
FUT = "https://fapi.binance.com/fapi/v1/klines"
ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "fut_klines_1m.json"
OUT = ROOT / "data" / "fut_backtest.json"

STOP_WORDS = {
    "BINANCE", "WILL", "DELIST", "NOTICE", "REMOVAL", "SPOT", "TRADING", "PAIRS", "PAIR",
    "MARGIN", "LOANS", "TOKEN", "ALPHA", "REMOVE", "FUTURES", "PERPETUAL", "CONTRACTS",
    "CONTRACT", "MULTIPLE", "USD", "UAH", "DEPOSITS", "WITHDRAWALS", "VIA", "FIAT",
    "TRADE", "AND", "ON", "CLOSE", "THE", "OF", "FOR", "NEW", "LISTING", "WITH", "TAG",
    "APPLIED", "SEED", "MONITORING", "UPDATED", "ADD", "ADDS", "LAUNCH", "LAUNCHES",
    "MARGINED", "TRADFI", "BUSD", "USDT", "USDC", "FDUSD", "TUSD",
}

LAGS_S = [1, 5, 15, 30, 60, 111, 130, 252]
STOPS_BPS = [0, 100, 200]
HORIZONS_S = [300, 900]
COST_BPS = 10.0
EQUITY0 = 1000.0
RISK_PCT = 0.01
MAX_LEVERAGE = 3.0
PRE_S = 600
POST_S = 1800


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


async def fetch_1m(client: httpx.AsyncClient, symbol: str, start_ms: int, end_ms: int):
    """1-минутные свечи фьючерса. Возвращает (список, статус).

    статус: 'ok' | 'no_contract' (400) | 'http_<code>'
    """
    out: list[list] = []
    cur = start_ms
    while cur < end_ms:
        r = await client.get(FUT, params={"symbol": symbol, "interval": "1m",
                                          "startTime": cur, "endTime": end_ms, "limit": 500})
        if r.status_code == 400:
            return [], "no_contract"
        if r.status_code != 200:
            return [], f"http_{r.status_code}"
        try:
            batch = r.json()
        except Exception:
            return [], "bad_json"
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < 500:
            break
        cur = int(batch[-1][0]) + 60_000
        await asyncio.sleep(0.1)
    return out, "ok"


def simulate(series: list[list], release_ms: int, lag_s: int, stop_bps: int,
             horizon_s: int, cost_bps: float, stop_from_next: bool = False):
    """Один шорт. Возвращает dict или строку-причину пропуска.

    stop_from_next=False — пессимистично: high минуты входа может сбить стоп
    (внутри минуты порядок неизвестен, считаем худшее).
    stop_from_next=True  — оптимистично: стоп начинает действовать со следующей минуты.
    """
    entry_t = release_ms + lag_s * 1000
    entry_candle = None
    for c in series:
        if int(c[0]) >= entry_t:
            entry_candle = c
            break
    if entry_candle is None:
        return "no_entry_candle"

    entry = float(entry_candle[1])          # open первой минуты после входа
    if entry <= 0:
        return "bad_price"

    exit_deadline = release_ms + horizon_s * 1000
    stop_price = entry * (1 + stop_bps / 10000.0) if stop_bps else None
    entry_open_ms = int(entry_candle[0])

    exit_price = None
    exit_t = None
    for c in series:
        t = int(c[0])
        if t < entry_open_ms:
            continue
        if t > exit_deadline:
            break
        stop_active = stop_price is not None and (not stop_from_next or t > entry_open_ms)
        if stop_active and float(c[2]) >= stop_price:   # high пробил стоп
            exit_price = stop_price
            exit_t = t
            break
        exit_price = float(c[4])
        exit_t = t
    if exit_price is None:
        return "no_exit"

    gross_bps = (entry - exit_price) / entry * 10000.0
    return {
        "entry": entry,
        "exit": exit_price,
        "gross_bps": gross_bps,
        "net_bps": gross_bps - cost_bps,
        "hold_s": (exit_t - entry_t) / 1000.0,
        "entry_t": entry_t,
    }


def run_grid(events: list[dict], cost_bps: float, stop_from_next: bool = False) -> dict:
    """Полный перебор. Капитал компаундится: размер позиции берётся от equity ДО сделки."""
    results = {}
    ordered = sorted(events, key=lambda e: e["release_ms"])
    for lag in LAGS_S:
        for stop in STOPS_BPS:
            for hor in HORIZONS_S:
                trades = []
                skipped: dict[str, int] = {}
                eq = EQUITY0
                peak = eq
                dd = 0.0
                blown = False
                for ev in ordered:
                    if blown:
                        skipped["account_blown"] = skipped.get("account_blown", 0) + 1
                        continue
                    r = simulate(ev["series"], ev["release_ms"], lag, stop, hor, cost_bps,
                                 stop_from_next=stop_from_next)
                    if isinstance(r, str):
                        skipped[r] = skipped.get(r, 0) + 1
                        continue
                    # риск 1% от ТЕКУЩЕГО капитала; без стопа риск не ограничен -> полкапитала
                    if stop:
                        notional = min(eq * RISK_PCT / (stop / 10000.0), eq * MAX_LEVERAGE)
                    else:
                        notional = eq * 0.5
                    pnl = notional * r["net_bps"] / 10000.0
                    # ликвидация: убыток больше капитала = счёт умер, а не ушёл в минус
                    if eq + pnl <= 0:
                        pnl = -eq
                        blown = True
                    trades.append({**r, "ts": ev["release_ms"], "symbol": ev["symbol"],
                                   "notional": notional, "pnl": pnl, "equity_before": eq})
                    eq += pnl
                    peak = max(peak, eq)
                    dd = max(dd, (peak - eq) / peak if peak > 0 else 0.0)
                if not trades:
                    continue
                nets = [t["net_bps"] for t in trades]
                results[f"lag{lag}_stop{stop}_h{hor}"] = {
                    "lag_s": lag, "stop_bps": stop, "horizon_s": hor,
                    "trades": len(trades),
                    "skipped": skipped,
                    "win_rate": sum(1 for x in nets if x > 0) / len(nets),
                    "median_net_bps": st.median(nets),
                    "mean_net_bps": st.mean(nets),
                    "total_pnl_usd": round(eq - EQUITY0, 2),
                    "final_equity": round(eq, 2),
                    "max_dd_pct": round(dd * 100, 2),
                    "avg_notional": round(st.mean([t["notional"] for t in trades]), 0),
                    "account_blown": blown,
                }
    return results


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=9)
    ap.add_argument("--cost", type=float, default=10.0, help="комиссия round trip, бп")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--stop-from-next", action="store_true",
                    help="оптимистичная граница: стоп действует со следующей минуты")
    args = ap.parse_args()

    global COST_BPS
    COST_BPS = args.cost
    print(f"комиссия round trip: {COST_BPS:.0f} бп")

    cache: dict = {}
    if CACHE.exists() and not args.refresh:
        cache = json.loads(CACHE.read_text("utf-8"))
        print(f"кэш 1m свечей: {len(cache)} символов-событий")

    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "newsbot-bt/2.0"}) as c:
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
        stats = {"ok": 0, "no_contract": 0, "other": 0, "cached": 0, "no_pre_volume": 0}

        async def load(ev: dict) -> None:
            key = f"{ev['symbol']}:{ev['release_ms']}"
            if key in cache:
                ev["series"] = cache[key]
                stats["cached"] += 1
                return
            async with sem:
                start = ev["release_ms"] - PRE_S * 1000
                end = ev["release_ms"] + POST_S * 1000
                series, status = await fetch_1m(c, ev["symbol"] + "USDT", start, end)
                if status == "no_contract":
                    stats["no_contract"] += 1
                    ev["series"] = []
                elif status != "ok":
                    stats["other"] += 1
                    ev["series"] = []
                else:
                    stats["ok"] += 1
                    ev["series"] = series
                cache[key] = ev["series"]
                await asyncio.sleep(0.12)

        done = 0
        for ev in events:
            await load(ev)
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(events)} | контракт есть {stats['ok']} | 400 {stats['no_contract']} "
                      f"| кэш {stats['cached']} | прочее {stats['other']}")

        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(cache), "utf-8")

    # фильтр торгуемости: был реальный объём ДО анонса
    tradable: list[dict] = []
    for ev in events:
        series = ev.get("series") or []
        pre = [k for k in series if int(k[0]) < ev["release_ms"]]
        post = [k for k in series if int(k[0]) >= ev["release_ms"]]
        if not series:
            continue
        pre_vol = sum(float(k[5]) for k in pre)
        if pre_vol <= 0 or not post:
            stats["no_pre_volume"] += 1
            continue
        ev["pre_vol_usd"] = pre_vol
        tradable.append(ev)

    print(f"\nконтракт найден: {stats['ok'] + stats['cached']} | без контракта (400): {stats['no_contract']} "
          f"| нет объёма до анонса: {stats['no_pre_volume']} | торгуемо: {len(tradable)}")

    if not tradable:
        print("нет торгуемых событий — сетка не считается")
        return 1

    # компаундинг считается внутри run_grid от текущего капитала

    grid = run_grid(tradable, COST_BPS, stop_from_next=args.stop_from_next)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance CMS catalog 161 + USD-M futures 1m klines (shortable only)",
        "announcements": len(arts),
        "candidates": len(events),
        "with_contract": stats["ok"] + stats["cached"],
        "no_contract": stats["no_contract"],
        "no_pre_volume": stats["no_pre_volume"],
        "tradable": len(tradable),
        "cost_bps_roundtrip": COST_BPS,
        "stop_assumption": "next_candle" if args.stop_from_next else "entry_candle_conservative",
        "equity0": EQUITY0,
        "risk_pct": RISK_PCT,
        "max_leverage": MAX_LEVERAGE,
        "grid": grid,
        "caveats": [
            "1-минутные свечи: фактический вход = лаг + до 60 с (в минус, не в плюс)",
            "стоп исполняется по цене стопа; реальный гэп может дать хуже",
            "проскальзывание сверх комиссии не моделируется отдельно",
            "ликвидность проверена только по объёму до анонса, не по глубине стакана",
            "параметры подбираются на ~100 событиях — это переобучение, а не настройка",
            "события коррелированы: один символ может давать 2-3 анонса подряд",
        ],
    }
    out_path = OUT.with_name(OUT.stem + ("_opt" if args.stop_from_next else "") + ".json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), "utf-8")

    print(f"\n{'лаг':>4} {'стоп':>5} {'гориз':>6} {'сделок':>7} {'win%':>6} "
          f"{'мед.бп':>8} {'сред.бп':>9} {'P&L$':>9} {'DD%':>7} {'позиция$':>9}")
    print("-" * 84)
    for k, v in sorted(grid.items(), key=lambda kv: (kv[1]["lag_s"], kv[1]["stop_bps"], kv[1]["horizon_s"])):
        print(f"{v['lag_s']:>4} {v['stop_bps']:>5} {v['horizon_s']:>6} {v['trades']:>7} "
              f"{v['win_rate'] * 100:>5.0f}% {v['median_net_bps']:>8.1f} {v['mean_net_bps']:>9.1f} "
              f"{v['total_pnl_usd']:>9.2f} {v['max_dd_pct']:>6.1f}% {v['avg_notional']:>9.0f}")
    print(f"\nотчёт: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
