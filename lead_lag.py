#!/usr/bin/env python3
"""Lead-lag: кто объявляет делистинг первым — Binance или другие биржи.

Логика
------
Если Bybit/OKX/KuCoin объявляют делистинг токена РАНЬШЕ Binance, то, шортя по их
свежему (без CDN-кэша) анонсу, мы забираем падение до того, как Binance вообще
опубликует новость. Это новый источник эджа, не зависящий от кэша Binance.

Что делает
----------
1. Собирает все анонсы типа `delistings` с Bybit (type.key=delistings).
2. Берёт каталог 161 Binance (кэш data/cms_articles.json).
3. Извлекает символы из заголовков, сопоставляет по токену.
4. Считает lag = publishTime(Bybit) - releaseDate(Binance) по каждому совпадению.
   lag < 0 => Bybit раньше. lag > 0 => Bybit позже.

Запуск: ./venv/bin/python lead_lag.py
"""

from __future__ import annotations

import json
import re
import statistics as st
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "data" / "lead_lag.json"
CMS_CACHE = ROOT / "data" / "cms_articles.json"

BYBIT = "https://api.bybit.com/v5/announcements/index"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

STOP = {"DELISTING", "OF", "PERPETUAL", "CONTRACT", "TO", "WILL", "DELIST", "TOKENS",
        "TOKEN", "ON", "AND", "SPOT", "PAIR", "PAIRS", "NOTICE", "FUTURES", "BINANCE"}


def symbols(title: str) -> list[str]:
    t = re.sub(r"\(.*?\)", " ", title)
    out: list[str] = []
    for tok in re.findall(r"\b[A-Z0-9]{2,12}\b", t):
        if tok in STOP:
            continue
        if tok.endswith("USDT") and len(tok) > 6:
            tok = tok[:-4]
        if tok in STOP or len(tok) < 2:
            continue
        if tok not in out:
            out.append(tok)
    return out


def main() -> int:
    # --- Bybit delistings ---
    bybit: list[dict] = []
    page = 1
    with httpx.Client(timeout=25.0, headers=UA) as c:
        while True:
            r = c.get(BYBIT, params={"locale": "en-US", "type": "delistings",
                                     "page": page, "limit": 200})
            if r.status_code != 200:
                break
            d = r.json().get("result", {})
            lst = d.get("list") or []
            if not lst:
                break
            for a in lst:
                bybit.append({
                    "title": a.get("title", ""),
                    "publish_ms": a.get("publishTime"),
                    "syms": symbols(a.get("title", "")),
                })
            total = d.get("total", 0)
            if page * 200 >= total:
                break
            page += 1
            time.sleep(0.2)
    print(f"Bybit делистингов собрано: {len(bybit)}")

    # --- Binance catalog 161 ---
    cms = json.loads(CMS_CACHE.read_text("utf-8")) if CMS_CACHE.exists() else {}
    print(f"Binance статей в кэше: {len(cms)}")

    # --- сопоставление ---
    pairs = []
    for b in bybit:
        if not b["publish_ms"]:
            continue
        for sym in b["syms"]:
            for code, a in cms.items():
                if a.get("catalog") != 161:
                    continue
                rel = a.get("release_ms")
                if not rel:
                    continue
                if sym in symbols(a.get("title", "")):
                    lag = b["publish_ms"] / 1000.0 - rel / 1000.0
                    pairs.append({"symbol": sym, "bybit_ms": b["publish_ms"],
                                  "binance_ms": rel, "lag_s": lag,
                                  "bybit_title": b["title"], "binance_title": a["title"]})
                    break
    print(f"совпадений по токену: {len(pairs)}")

    if pairs:
        # дедуп: одна пара (символ, binance_ms)
        seen = set()
        uniq = []
        for p in pairs:
            k = (p["symbol"], p["binance_ms"])
            if k not in seen:
                seen.add(k)
                uniq.append(p)
        pairs = uniq
        lags = [p["lag_s"] for p in pairs]
        faster = sum(1 for x in lags if x < 0)
        print(f"\nуникальных пар: {len(pairs)}")
        print(f"Bybit раньше Binance: {faster}/{len(pairs)} ({faster/len(pairs):.0%})")
        print(f"медиана lag (Bybit - Binance): {st.median(lags)/3600:+.1f} ч")
        print(f"  мин {min(lags)/3600:+.1f} ч | макс {max(lags)/3600:+.1f} ч")
        import statistics
        hrs = sorted(lags)
        q = lambda p: hrs[min(len(hrs)-1, int(p*len(hrs)))]/3600
        print(f"  p05 {q(.05):+.1f}ч | p25 {q(.25):+.1f}ч | p50 {q(.5):+.1f}ч | "
              f"p75 {q(.75):+.1f}ч | p95 {q(.95):+.1f}ч")
        print("\nпервые 15 по времени:")
        for p in sorted(pairs, key=lambda x: x["lag_s"])[:15]:
            print(f"  {p['lag_s']/3600:+8.1f} ч | {p['symbol']:10s} | Bybit: {p['bybit_title'][:40]}")
        print("\nпоследние 5:")
        for p in sorted(pairs, key=lambda x: x["lag_s"])[-5:]:
            print(f"  {p['lag_s']/3600:+8.1f} ч | {p['symbol']:10s} | Bybit: {p['bybit_title'][:40]}")

        OUT.write_text(json.dumps({
            "bybit_delistings": len(bybit), "pairs": len(pairs),
            "faster_share": faster / len(pairs), "median_lag_s": st.median(lags),
            "details": sorted(pairs, key=lambda x: x["lag_s"]),
        }, ensure_ascii=False, indent=2), "utf-8")
        print(f"\nотчёт: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
