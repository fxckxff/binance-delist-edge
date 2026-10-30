#!/usr/bin/env python3
"""Историческое измерение лага Telegram против CMS Binance.

Зачем
-----
Лаг обнаружения — свойство канала, а не рынка, поэтому рыночные данные его не
содержат. Но у Telegram есть публичный архив всех постов с точным временем, а
каждый пост содержит ссылку на статью (`/announcement/detail/<code>`). CMS отдаёт
тот же `code`. Значит сопоставление ТОЧНОЕ, без угадывания заголовков — и лаг
можно измерить на сотнях прошедших событий, не ожидая недель.

Что делает
----------
1. Листает t.me/s/binance_announcements назад через ?before=<id>, собирает
   (post_id, время поста, code статьи).
2. Забирает каталог 161 (делистинги) и 48 (листинги) из CMS: code -> releaseDate.
3. Сопоставляет по code и считает lag = tg_time - cms_releaseDate.
4. Печатает распределение и долю постов, опубликованных РАНЬШЕ CMS.

Запуск:
    ./venv/bin/python tg_lag.py --pages 150
"""

from __future__ import annotations

import argparse
import json
import re
import statistics as st
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
TG_CACHE = ROOT / "data" / "tg_archive.json"
CMS_CACHE = ROOT / "data" / "cms_articles.json"
OUT = ROOT / "data" / "tg_lag.json"

TG = "https://t.me/s/binance_announcements"
CMS = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
UA_TG = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
UA_CMS = {"User-Agent": "newsbot-probe/1.0"}


def parse_page(html: str) -> list[dict]:
    """Вытаскивает посты: id, время, code статьи, текст."""
    posts: list[dict] = []
    # делим по маркерам постов
    chunks = re.split(r'data-post="binance_announcements/(\d+)"', html)
    # chunks: [pre, id1, body1, id2, body2, ...]
    for i in range(1, len(chunks) - 1, 2):
        pid = int(chunks[i])
        body = chunks[i + 1]
        tm = re.search(r'<time datetime="([^"]+)"', body)
        code = re.search(r'/announcement/detail/([0-9a-f]{32})', body)
        text = re.search(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', body, re.S)
        title = ""
        if text:
            title = re.sub(r"<[^>]+>", "", text.group(1)).replace("&amp;", "&").strip()
        posts.append({
            "post_id": pid,
            "time": tm.group(1) if tm else None,
            "code": code.group(1) if code else None,
            "title": title[:120],
        })
    return posts


def collect_tg(pages: int) -> dict[int, dict]:
    archive: dict[int, dict] = {}
    if TG_CACHE.exists():
        raw = json.loads(TG_CACHE.read_text("utf-8"))
        archive = {int(k): v for k, v in raw.items()}
        print(f"архив Telegram в кэше: {len(archive)} постов")

    before = min(archive) if archive else None
    with httpx.Client(timeout=25.0, headers=UA_TG, follow_redirects=True) as c:
        for i in range(pages):
            url = TG if before is None else f"{TG}?before={before}"
            try:
                r = c.get(url)
            except httpx.HTTPError as exc:
                print(f"  страница {i+1}: сеть {type(exc).__name__}")
                time.sleep(2)
                continue
            if r.status_code != 200:
                print(f"  страница {i+1}: HTTP {r.status_code} — стоп")
                break
            got = [p for p in parse_page(r.text) if p["time"]]
            if not got:
                print(f"  страница {i+1}: постов нет — архив кончился")
                break
            new = [p for p in got if p["post_id"] not in archive]
            for p in new:
                archive[p["post_id"]] = p
            before = min(p["post_id"] for p in got)
            if (i + 1) % 10 == 0 or not new:
                oldest = min(got, key=lambda p: p["post_id"])
                print(f"  {i+1} страниц | всего {len(archive)} | самый старый {oldest['time']}")
            if not new:
                break
            time.sleep(0.25)

    TG_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TG_CACHE.write_text(json.dumps({str(k): v for k, v in archive.items()}, ensure_ascii=False), "utf-8")
    return archive


def collect_cms() -> dict[str, dict]:
    if CMS_CACHE.exists():
        return json.loads(CMS_CACHE.read_text("utf-8"))
    arts: dict[str, dict] = {}
    with httpx.Client(timeout=25.0, headers=UA_CMS) as c:
        for cat in (161, 48):
            for page in range(1, 11):
                r = c.get(CMS, params={"type": 1, "pageNo": page, "pageSize": 50, "catalogId": cat})
                if r.status_code != 200:
                    break
                try:
                    got = r.json()["data"]["catalogs"][0]["articles"]
                except Exception:
                    break
                if not got:
                    break
                for a in got:
                    if a.get("code"):
                        arts[a["code"]] = {"title": a.get("title"), "release_ms": a.get("releaseDate"),
                                           "catalog": cat}
                time.sleep(0.35)
    CMS_CACHE.write_text(json.dumps(arts, ensure_ascii=False), "utf-8")
    return arts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=150)
    args = ap.parse_args()

    archive = collect_tg(args.pages)
    cms = collect_cms()
    print(f"статей CMS: {len(cms)}")

    from datetime import datetime, timezone
    pairs = []
    for p in archive.values():
        code = p.get("code")
        if not code or code not in cms:
            continue
        rel = cms[code].get("release_ms")
        if not rel:
            continue
        tg_dt = datetime.fromisoformat(p["time"].replace("Z", "+00:00"))
        lag = tg_dt.timestamp() - rel / 1000.0
        pairs.append({"post_id": p["post_id"], "code": code, "lag_s": lag,
                      "title": cms[code]["title"], "catalog": cms[code]["catalog"],
                      "tg_time": p["time"]})

    if not pairs:
        print("совпадений нет")
        return 1

    pairs.sort(key=lambda x: x["lag_s"])
    lags = [p["lag_s"] for p in pairs]
    print(f"\nсовпало по code: {len(pairs)} из {len(cms)} статей CMS")
    print(f"лаг Telegram относительно CMS, секунды:")
    print(f"  медиана {st.median(lags):+.1f} | среднее {st.mean(lags):+.1f}")
    q = lambda p: sorted(lags)[min(len(lags) - 1, int(p * len(lags)))]
    print(f"  p05 {q(.05):+.0f} | p25 {q(.25):+.0f} | p75 {q(.75):+.0f} | p95 {q(.95):+.0f}")
    print(f"  минимум {min(lags):+.0f} | максимум {max(lags):+.0f}")
    faster = sum(1 for x in lags if x < 0)
    within5 = sum(1 for x in lags if -5 <= x <= 5)
    print(f"\nTelegram раньше CMS: {faster}/{len(lags)} ({faster / len(lags):.0%})")
    print(f"в пределах ±5 с: {within5}/{len(lags)} ({within5 / len(lags):.0%})")

    print("\n10 самых быстрых:")
    for p in pairs[:10]:
        print(f"  {p['lag_s']:+8.1f} с | cat{p['catalog']} | {p['title'][:64]}")
    print("\n10 самых медленных:")
    for p in pairs[-10:]:
        print(f"  {p['lag_s']:+8.1f} с | cat{p['catalog']} | {p['title'][:64]}")

    OUT.write_text(json.dumps({
        "pairs": len(pairs), "median_lag_s": st.median(lags), "mean_lag_s": st.mean(lags),
        "faster_share": faster / len(lags), "within_5s_share": within5 / len(lags),
        "details": pairs,
    }, ensure_ascii=False, indent=2), "utf-8")
    print(f"\nотчёт: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
