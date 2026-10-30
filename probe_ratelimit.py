#!/usr/bin/env python3
"""Измерение реального лимита запросов к CMS-эндпоинту анонсов Binance.

Разгоняется по шагам и ОСТАНАВЛИВАЕТСЯ на первом 429, чтобы не получить бан.
Пишет результат в data/ratelimit_probe.json.

Запуск: ./venv/bin/python probe_ratelimit.py
"""

import asyncio
import json
import statistics
import time
from pathlib import Path

import httpx

URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
PARAMS = {"type": 1, "pageNo": 1, "pageSize": 1, "catalogId": 161}
OUT = Path(__file__).resolve().parent / "data" / "ratelimit_probe.json"

# (интервал между запросами, сколько запросов)
PHASES = [(1.0, 60), (0.5, 40), (0.25, 30), (0.1, 20)]


async def main() -> int:
    report = {"url": URL, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "phases": [], "verdict": None}
    async with httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "newsbot-probe/1.0"}) as c:
        etag = None
        first = await c.get(URL, params=PARAMS)
        etag = first.headers.get("etag")
        report["etag_initial"] = etag
        print(f"стартовый etag: {etag}")

        stopped = False
        for interval, count in PHASES:
            if stopped:
                break
            phase = {"interval_s": interval, "planned": count, "sent": 0,
                     "status": {}, "cache": {}, "lat_ms": [], "first_429_at": None}
            print(f"\n--- фаза: {count} запросов с интервалом {interval}s "
                  f"({60/interval:.0f} req/min) ---")
            for i in range(count):
                t0 = time.time()
                headers = {"If-None-Match": etag} if etag else {}
                try:
                    r = await c.get(URL, params=PARAMS, headers=headers)
                    dt = (time.time() - t0) * 1000
                    phase["lat_ms"].append(round(dt, 1))
                    phase["status"][str(r.status_code)] = phase["status"].get(str(r.status_code), 0) + 1
                    cache = r.headers.get("x-cache", "?")
                    phase["cache"][cache] = phase["cache"].get(cache, 0) + 1
                    if r.status_code == 429:
                        phase["first_429_at"] = i + 1
                        stopped = True
                        print(f"  429 на запросе {i+1} -> СТОП (Retry-After={r.headers.get('Retry-After')})")
                        break
                except httpx.HTTPError as exc:
                    phase["status"]["error"] = phase["status"].get("error", 0) + 1
                    print(f"  сетевая ошибка на {i+1}: {type(exc).__name__}")
                phase["sent"] += 1
                await asyncio.sleep(interval)
            lat = phase["lat_ms"]
            if lat:
                phase["lat_median_ms"] = statistics.median(lat)
                phase["lat_p95_ms"] = sorted(lat)[int(0.95 * (len(lat) - 1))]
            report["phases"].append(phase)
            print(f"  отправлено {phase['sent']} | статусы {phase['status']} | "
                  f"кэш {phase['cache']} | медиана {phase.get('lat_median_ms', 0):.0f} ms")

        ok = all("429" not in p["status"] for p in report["phases"])
        fastest = min((p["interval_s"] for p in report["phases"]
                       if "429" not in p["status"]), default=None)
        report["verdict"] = {
            "no_429_up_to": (1.0 / fastest) * 60 if fastest else 0,
            "clean_through_all_phases": ok,
            "note": "req/min = 60 / интервал, при котором 429 ещё не было",
        }
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), "utf-8")
        print(f"\nитог: без 429 держит до {report['verdict']['no_429_up_to']:.0f} req/min")
        print(f"отчёт: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
