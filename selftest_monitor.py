#!/usr/bin/env python3
"""Проверка пяти защит монитора на реальном коде, без заглушек в логике.

Что проверяется:
  1. kill switch (файл STOP)
  2. backoff на 429 / 5xx / сетевые ошибки
  3. устойчивость парсера к кривым ответам
  4. проверка ликвидности (fail closed)
  5. синхронизация времени и отсечка старых анонсов
  + персистентный курсор (нет фантомных сигналов после рестарта)
  + single instance
  + два живых запроса в Binance (время и стакан)

Запуск:  ./venv/bin/python selftest_monitor.py
"""

import asyncio
import io
import json
import logging
import random
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx  # noqa: E402

from monitor_core import (  # noqa: E402
    BINANCE_FAPI_TIME,
    Config,
    Monitor,
    BackoffClient,
    SeenState,
    SingleInstance,
    depth_usd,
    extract_symbols,
    parse_articles,
)

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))


def capture_logger() -> tuple[logging.Logger, io.StringIO]:
    buf = io.StringIO()
    log = logging.getLogger(f"selftest-{time.time_ns()}")
    log.setLevel(logging.DEBUG)
    log.propagate = False
    h = logging.StreamHandler(buf)
    h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    log.addHandler(h)
    return log, buf


def make_transport(feed: list[dict], *, depth_book=None, list_status=200):
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "article/list" in url:
            return httpx.Response(
                list_status,
                json={"data": {"catalogs": [{"articles": list(feed)}]}},
            )
        if "fapi/v1/time" in url:
            return httpx.Response(200, json={"serverTime": int(time.time() * 1000)})
        if "depth" in url:
            if depth_book is None:
                return httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
            return httpx.Response(200, json=depth_book)
        return httpx.Response(404)
    return httpx.MockTransport(handler)


def art(aid: str, title: str, age_s: float = 60.0) -> dict:
    return {"id": aid, "title": title,
            "releaseDate": int((time.time() - age_s) * 1000), "code": "x"}


DEEP_BOOK = {
    "bids": [[100.0, 500.0], [99.9, 500.0]],
    "asks": [[100.1, 500.0], [100.2, 500.0]],
}


def tmp_cfg(tmp: Path) -> Config:
    return Config(
        state_path=tmp / "seen.json",
        pid_path=tmp / "monitor.pid",
        stop_path=tmp / "STOP",
        log_path=tmp / "bot.log",
        poll_interval_s=0.0,
        heartbeat_every_s=0.0,
        clock_resync_s=10_000.0,
    )


def bootstrap_state(cfg: Config) -> None:
    """Помечает курсор как уже инициализированный — чтобы тест проверял обработку, а не bootstrap."""
    cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.state_path.write_text(json.dumps({"bootstrapped": True, "ids": []}), "utf-8")


# --------------------------------------------------------------------------- #
# 3. парсер
# --------------------------------------------------------------------------- #

def test_parser() -> None:
    bad_payloads = [
        None, [], "строка", {}, {"data": None}, {"data": []},
        {"data": {"catalogs": "нет"}}, {"data": {"catalogs": [None, 5]}},
        {"data": {"catalogs": [{"articles": "нет"}]}},
        {"data": {"catalogs": [{"articles": [None, 1, "x"]}]}},
    ]
    ok = True
    detail = ""
    for p in bad_payloads:
        try:
            out = parse_articles(p)
            if out != []:
                ok, detail = False, f"ожидался пустой список для {p!r}, получено {out!r}"
                break
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"исключение {type(exc).__name__} на {p!r}"
            break
    check("парсер: 10 битых ответов не роняют процесс", ok, detail)

    got = parse_articles({"data": {"catalogs": [{"articles": [
        {"id": 1, "title": None, "releaseDate": "мусор"},
        {"id": 2, "title": "  Ok title  ", "releaseDate": "1700000000000"},
    ]}]}})
    ok = (len(got) == 2 and got[0]["title"] == "" and got[0]["release_ms"] == 0
          and got[1]["title"] == "Ok title" and got[1]["release_ms"] == 1700000000000)
    check("парсер: отсутствующий title/битая дата -> пустые значения, без падения",
          ok, f"{got}")

    got = parse_articles({"data": {"catalogs": [{"articles": [{"id": 7, "title": "T", "releaseDate": 1}]}]}})
    check("парсер: нормальный ответ разобран", len(got) == 1 and got[0]["id"] == "7")


def test_symbols() -> None:
    cases = {
        "Binance Futures Will Delist PTBUSDT and MITOUSDT USDⓈ-M Perpetual Contracts (2026-10-23)":
            ["PTBUSDT", "MITOUSDT"],
        "Binance Will Delist EPIC, QUICK on 2026-10-28": ["EPICUSDT", "QUICKUSDT"],
        "Binance Margin And Loans Token Delist on 2026-10-02": [],
        "Binance Will Add XYUSDC Trading Pairs": ["XYUSDC"],
    }
    ok = True
    detail = ""
    for title, expected in cases.items():
        got = extract_symbols(title)
        if got != expected:
            ok, detail = False, f"{title[:40]}... -> {got}, ожидалось {expected}"
            break
    check("символы: разбор тикеров из заголовков", ok, detail)


def test_depth() -> None:
    book = {"bids": [[100.0, 1.0], [99.0, 5.0]], "asks": [[101.0, 1.0], [102.0, 5.0]]}
    got = depth_usd(book, 50.0)
    check("ликвидность: расчёт глубины в полосе ±50 бп", abs(got - 201.0) < 0.01, f"{got}")
    check("ликвидность: пустой/битый стакан -> 0", depth_usd({}, 50.0) == 0.0
          and depth_usd("мусор", 50.0) == 0.0 and depth_usd({"bids": [["x", "y"]], "asks": []}, 50.0) == 0.0)


# --------------------------------------------------------------------------- #
# 2. backoff
# --------------------------------------------------------------------------- #

async def test_backoff() -> None:
    cfg = Config(backoff_base_s=2.0, backoff_max_s=300.0, max_attempts=6)
    log, _ = capture_logger()

    # 429 с Retry-After -> пауза ровно по заголовку, затем успех
    calls = {"n": 0}

    def handler_429(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 2:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json={"ok": True})

    slept: list[float] = []

    async def fake_sleep(d):
        slept.append(d)

    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=httpx.MockTransport(handler_429)),
                           sleep=fake_sleep, rng=random.Random(0))
    res = await client.get_json("https://example.test/x")
    check("backoff: Retry-After соблюдён (2 паузы по 7 с, потом успех)",
          res == {"ok": True} and slept == [7.0, 7.0] and client.count_429 == 2,
          f"slept={slept}, 429={client.count_429}")

    # постоянные 429 -> сдаётся, паузы растут
    def handler_always_429(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    slept = []
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=httpx.MockTransport(handler_always_429)),
                           sleep=fake_sleep, rng=random.Random(0))
    res = await client.get_json("https://example.test/x")
    growing = all(b >= a for a, b in zip(slept, slept[1:])) and len(slept) == 5
    check("backoff: постоянные 429 -> 5 пауз, рост задержки, без падения",
          res is None and growing and client.count_429 == 6, f"slept={[round(x, 1) for x in slept]}")

    # 500 и сетевая ошибка тоже ретраятся
    def handler_500(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    slept = []
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=httpx.MockTransport(handler_500)),
                           sleep=fake_sleep, rng=random.Random(0))
    res = await client.get_json("https://example.test/x")
    check("backoff: 503 ретраится и сдаётся без исключения", res is None and len(slept) == 5)

    def handler_boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("нет сети")

    slept = []
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=httpx.MockTransport(handler_boom)),
                           sleep=fake_sleep, rng=random.Random(0))
    res = await client.get_json("https://example.test/x")
    check("backoff: сетевая ошибка ретраится, возвращается None", res is None and len(slept) == 5)

    # битый JSON
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(
                               transport=httpx.MockTransport(lambda r: httpx.Response(200, text="<html>"))),
                           sleep=fake_sleep, rng=random.Random(0))
    res = await client.get_json("https://example.test/x")
    check("backoff: не-JSON ответ -> None, без падения", res is None)


# --------------------------------------------------------------------------- #
# 1, 5, 4, 6. поведение монитора
# --------------------------------------------------------------------------- #

async def test_kill_switch(tmp: Path) -> None:
    cfg = tmp_cfg(tmp)
    log, buf = capture_logger()
    cfg.stop_path.write_text("stop")
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport([])),
                           sleep=lambda d: asyncio.sleep(0))
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0})
    rc = await mon.run()
    check("kill switch: файл STOP останавливает цикл с кодом 0",
          rc == 0 and "KILL SWITCH" in buf.getvalue(), f"rc={rc}")


async def test_bootstrap_and_dedup(tmp: Path) -> None:
    cfg = tmp_cfg(tmp)
    log, buf = capture_logger()
    feed = [art("1", "Binance Futures Will Delist AAUSDT Perpetual Contracts"),
            art("2", "Binance Will Delist BB on 2026-10-28")]
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed)),
                           sleep=lambda d: asyncio.sleep(0))
    depth_ok = lambda sym: {"symbol": sym, "market": "futures", "depth_usd": 100000.0, "ok": True}

    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=depth_ok, max_loops=1)
    await mon.run()
    text = buf.getvalue()
    check("курсор: первый запуск = bootstrap, 0 сигналов (нет фантомов)",
          "bootstrap" in text and "SIGNAL" not in text and mon.signals == 0, text.strip().splitlines()[-1][:80])

    # рестарт на том же state -> те же анонсы не должны дать сигналов
    buf.truncate(0), buf.seek(0)
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed)),
                           sleep=lambda d: asyncio.sleep(0))
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=depth_ok, max_loops=1)
    await mon.run()
    check("курсор: рестарт не повторяет старые анонсы", mon.signals == 0
          and "SIGNAL" not in buf.getvalue(), f"signals={mon.signals}")

    # новый анонс -> ровно один сигнал
    buf.truncate(0), buf.seek(0)
    feed.append(art("3", "Binance Futures Will Delist CCUSDT Perpetual Contracts", age_s=5))
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed)),
                           sleep=lambda d: asyncio.sleep(0))
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=depth_ok, max_loops=1)
    await mon.run()
    check("курсор: новый анонс даёт ровно один сигнал", mon.signals == 1
          and "SIGNAL PAPER" in buf.getvalue(), f"signals={mon.signals}")


async def test_staleness(tmp: Path) -> None:
    cfg = tmp_cfg(tmp)
    bootstrap_state(cfg)
    log, buf = capture_logger()
    feed = [art("10", "Binance Futures Will Delist DDUSDT Perpetual Contracts", age_s=7200)]
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed)),
                           sleep=lambda d: asyncio.sleep(0))
    depth_ok = lambda sym: {"symbol": sym, "market": "futures", "depth_usd": 100000.0, "ok": True}
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=depth_ok, max_loops=1)
    await mon.run()
    text = buf.getvalue()
    check("время: анонс двухчасовой давности отброшен как STALE",
          "STALE" in text and mon.signals == 0, f"signals={mon.signals}")

    # свежий анонс при измеренном смещении часов проходит в обработку
    buf.truncate(0), buf.seek(0)
    feed2 = [art("11", "Binance Futures Will Delist FFUSDT Perpetual Contracts", age_s=5)]
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed2)),
                           sleep=lambda d: asyncio.sleep(0))
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 54.0, "rtt_ms": 12.0},
                  depth_probe=depth_ok, max_loops=1)
    await mon.run()
    check("время: свежий анонс проходит при измеренном смещении часов",
          mon.signals == 1 and mon.clock_offset_ms == 54.0, f"signals={mon.signals}")


async def test_liquidity_gate(tmp: Path) -> None:
    cfg = tmp_cfg(tmp)
    bootstrap_state(cfg)
    log, buf = capture_logger()
    feed = [art("20", "Binance Futures Will Delist EEUSDT Perpetual Contracts", age_s=5)]
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed)),
                           sleep=lambda d: asyncio.sleep(0))
    thin = lambda sym: {"symbol": sym, "market": "futures", "depth_usd": 12.0, "ok": False}
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=thin, max_loops=1)
    await mon.run()
    text = buf.getvalue()
    check("ликвидность: пустой стакан -> сигнал отклонён (fail closed)",
          "SIGNAL REJECTED" in text and mon.signals == 0, f"signals={mon.signals}")

    # реальная ветка: символа нет ни на фьючерсах, ни на споте
    buf.truncate(0), buf.seek(0)
    feed2 = [art("21", "Binance Futures Will Delist GGUSDT Perpetual Contracts", age_s=5)]
    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=make_transport(feed2, depth_book=None)),
                           sleep=lambda d: asyncio.sleep(0))
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0}, max_loops=1)
    await mon.run()
    check("ликвидность: несуществующий символ -> отказ, без падения",
          mon.signals == 0 and "SIGNAL REJECTED" in buf.getvalue())


def test_single_instance(tmp: Path) -> None:
    path = tmp / "lock.pid"
    a, b = SingleInstance(path), SingleInstance(path)
    first = a.acquire()
    second = b.acquire()
    a.release()
    third = SingleInstance(path).acquire()
    check("single instance: второй процесс не стартует, после освобождения — можно",
          first and not second and third, f"{first}/{second}/{third}")


async def test_conditional_and_lag(tmp: Path) -> None:
    """Условные запросы: 304 не должен ломать цикл, а лаг обнаружения должен попадать в лог."""
    cfg = tmp_cfg(tmp)
    bootstrap_state(cfg)
    log, buf = capture_logger()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("if-none-match"):
            return httpx.Response(304, headers={"etag": 'W/"abc"', "x-cache": "Hit from cloudfront"})
        return httpx.Response(
            200,
            headers={"etag": 'W/"abc"', "x-cache": "Miss from cloudfront"},
            json={"data": {"catalogs": [{"articles": [
                art("30", "Binance Futures Will Delist HHUSDT Perpetual Contracts", age_s=3)
            ]}]}},
        )

    client = BackoffClient(cfg, log,
                           client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                           sleep=lambda d: asyncio.sleep(0))
    depth_ok = lambda sym: {"symbol": sym, "market": "futures", "depth_usd": 100000.0, "ok": True}
    mon = Monitor(cfg, log, client=client, sleep=lambda d: asyncio.sleep(0),
                  time_probe=lambda: {"offset_ms": 0.0, "rtt_ms": 1.0},
                  depth_probe=depth_ok, max_loops=2)
    await mon.run()
    text = buf.getvalue()
    check("условные запросы: 304 обработан, цикл не упал, сигнал ровно один",
          mon.loops == 2 and mon.signals == 1 and "unexpected HTTP 304" not in text,
          f"loops={mon.loops} signals={mon.signals}")
    check("лаг обнаружения пишется в лог (detect_lag)",
          "detect_lag=" in text and 0 <= mon.lags[0] < 30, f"lag={mon.lags[0]:.1f}s")


def test_state_file(tmp: Path) -> None:
    p = tmp / "state.json"
    s = SeenState(p, max_ids=3).load()
    for i in range(5):
        s.add(str(i))
    s.bootstrapped = True
    s.save()
    s2 = SeenState(p, max_ids=3).load()
    check("курсор: состояние переживает рестарт, размер ограничен",
          s2.bootstrapped and s2.seen("4") and not s2.seen("0") and len(s2.ids) == 3,
          f"ids={s2.ids}")


# --------------------------------------------------------------------------- #
# живые запросы
# --------------------------------------------------------------------------- #

async def test_live() -> None:
    cfg = Config()
    log, buf = capture_logger()
    mon = Monitor(cfg, log, sleep=lambda d: asyncio.sleep(0))
    try:
        ok = await mon.sync_clock()
        check("живой запрос: время Binance, смещение измерено",
              ok and abs(mon.clock_offset_ms) < 5000,
              f"offset={mon.clock_offset_ms:.0f} ms, rtt={mon.clock_rtt_ms:.0f} ms")
    except Exception as exc:  # noqa: BLE001
        check("живой запрос: время Binance", False, f"{type(exc).__name__}: {exc}")
    try:
        liq = await mon.check_liquidity(["BTCUSDT"])
        check("живой запрос: стакан BTCUSDT, глубина > порога",
              liq and liq[0]["ok"] and liq[0]["depth_usd"] > cfg.min_depth_usd,
              f"{liq[0]['market']} {liq[0]['depth_usd']:.0f} USD")
    except Exception as exc:  # noqa: BLE001
        check("живой запрос: стакан BTCUSDT", False, f"{type(exc).__name__}: {exc}")
    try:
        liq = await mon.check_liquidity(["ЭТОГОНЕТUSDT"])
        check("живой запрос: несуществующий символ -> ok=False, без исключения",
              liq and liq[0]["ok"] is False, f"{liq[0]}")
    except Exception as exc:  # noqa: BLE001
        check("живой запрос: несуществующий символ", False, f"{type(exc).__name__}: {exc}")
    finally:
        try:
            await mon.client.client.aclose()
        except Exception:
            pass


# --------------------------------------------------------------------------- #

async def main() -> int:
    test_parser()
    test_symbols()
    test_depth()
    await test_backoff()
    test_single_instance(Path(tempfile.mkdtemp()))
    test_state_file(Path(tempfile.mkdtemp()))

    for fn in (test_kill_switch, test_bootstrap_and_dedup, test_staleness,
               test_liquidity_gate, test_conditional_and_lag):
        await fn(Path(tempfile.mkdtemp()))

    await test_live()

    failed = [r for r in RESULTS if not r[1]]
    print(f"\nитого: {len(RESULTS) - len(failed)}/{len(RESULTS)} проверок пройдено")
    if failed:
        for name, _, detail in failed:
            print(f"  FAIL: {name} {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
