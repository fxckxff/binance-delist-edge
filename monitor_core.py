"""Ядро монитора анонсов Binance.

Без побочных эффектов при импорте: ничего не запускается, не пишет в лог,
не ходит в сеть, пока не создан Monitor и не вызван run().

Защиты:
  1. Kill switch  — файл STOP, проверяется перед каждым шагом цикла.
  2. Backoff      — 429/418/5xx/сеть: экспоненциальная пауза + джиттер,
                    уважается Retry-After, есть предел попыток.
  3. Парсинг      — любая кривая структура ответа не роняет процесс.
  4. Ликвидность  — глубина стакана в USD в полосе ±N бп от середины,
                    сигнал без ликвидности отклоняется (fail closed).
  5. Время        — смещение часов относительно сервера Binance,
                    по нему считается возраст анонса; старое отбрасывается.

Плюс две вещи, без которых первые пять бесполезны:
  6. Персистентный курсор — рестарт не порождает фантомных сигналов.
  7. Single instance      — второй процесс на том же каталоге не стартует.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import math
from email.utils import parsedate_to_datetime
import os
import random
import re
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

import httpx

BINANCE_LIST = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BINANCE_FAPI_TIME = "https://fapi.binance.com/fapi/v1/time"
BINANCE_FAPI_DEPTH = "https://fapi.binance.com/fapi/v1/depth"
BINANCE_SPOT_DEPTH = "https://api.binance.com/api/v3/depth"

ROOT = Path(__file__).resolve().parent


@dataclass
class Config:
    catalog_id: int = 161            # 161 = Delisting
    page_size: int = 10
    poll_interval_s: float = 15.0
    stale_after_s: float = 900.0     # анонс старше 15 минут не торгуем
    min_depth_usd: float = 5000.0    # минимум ликвидности в стакане
    depth_band_bps: float = 50.0     # полоса вокруг середины
    backoff_base_s: float = 2.0
    backoff_max_s: float = 300.0
    max_attempts: int = 6
    heartbeat_every_s: float = 1200.0
    clock_resync_s: float = 600.0
    use_conditional: bool = False    # по умолчанию ВЫКЛ: 304 может гасить ревалидацию кэша
                                     # (см. PROTECTIONS.md, раздел про TTL)
    state_path: Path = ROOT / "state" / "seen.json"
    pid_path: Path = ROOT / "state" / "monitor.pid"
    stop_path: Path = ROOT / "STOP"
    log_path: Path = ROOT / "logs" / "bot.log"
    observations_path: Path | None = None

    @classmethod
    def from_env(cls) -> "Config":
        """Настройки без правки кода: NEWS_POLL_INTERVAL, NEWS_STALE_AFTER, NEWS_MIN_DEPTH_USD."""
        cfg = cls()
        env = os.environ
        root = Path(env.get("NEWS_ROOT", str(ROOT))).expanduser().resolve()
        cfg.state_path = root / "state" / "seen.json"
        cfg.pid_path = root / "state" / "monitor.pid"
        cfg.stop_path = root / "STOP"
        cfg.log_path = root / "logs" / "bot.log"
        cfg.observations_path = root / "logs" / "observations.jsonl"
        for key, attr, lo, cast in (
            ("NEWS_POLL_INTERVAL", "poll_interval_s", 0.2, float),
            ("NEWS_STALE_AFTER", "stale_after_s", 1.0, float),
            ("NEWS_MIN_DEPTH_USD", "min_depth_usd", 0.0, float),
        ):
            raw = env.get(key)
            if raw is None:
                continue
            try:
                value = cast(raw)
                if not math.isfinite(value) or value < lo:
                    raise ValueError(f"{key} must be finite and >= {lo}")
                setattr(cfg, attr, value)
            except ValueError as exc:
                raise ValueError(f"Invalid {key}: {raw!r}") from exc
        return cfg


# --------------------------------------------------------------------------- #
# логирование
# --------------------------------------------------------------------------- #

def setup_logging(log_path: Path, level: int = logging.INFO) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("newsbot")
    logger.setLevel(level)
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger


# --------------------------------------------------------------------------- #
# парсинг (защита 3)
# --------------------------------------------------------------------------- #

def parse_articles(payload: Any) -> list[dict]:
    """Достаёт список анонсов из любого ответа. Никогда не бросает исключение."""
    out: list[dict] = []
    if not isinstance(payload, dict):
        return out
    data = payload.get("data")
    if not isinstance(data, dict):
        return out
    catalogs = data.get("catalogs")
    if not isinstance(catalogs, list):
        return out
    for cat in catalogs:
        if not isinstance(cat, dict):
            continue
        arts = cat.get("articles")
        if not isinstance(arts, list):
            continue
        for a in arts:
            if not isinstance(a, dict):
                continue
            aid = a.get("id")
            if aid is None or isinstance(aid, (dict, list)):
                continue
            title = a.get("title")
            if not isinstance(title, str):
                title = ""
            rel = a.get("releaseDate")
            try:
                rel_ms = int(rel)
            except (TypeError, ValueError, OverflowError):
                rel_ms = 0
            code = a.get("code")
            out.append(
                {
                    "id": str(aid),
                    "title": title.strip(),
                    "release_ms": rel_ms,
                    "code": code if isinstance(code, str) else "",
                }
            )
    return out


NOISE = {
    "BINANCE", "FUTURES", "SPOT", "MARGIN", "LOANS", "TOKEN", "ALPHA", "WILL",
    "DELIST", "DELISTING", "REMOVE", "REMOVAL", "TRADING", "PAIRS", "PAIR",
    "PERPETUAL", "CONTRACTS", "CONTRACT", "MULTIPLE", "NOTICE", "THE", "AND",
    "ON", "OF", "FOR", "WITH", "USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD",
    "NFT", "NEW", "LIST", "LAUNCH", "UPDATED", "API",
}

PAIR_RE = re.compile(r"\b([A-Z0-9]{2,15})(USDT|USDC|BUSD|FDUSD|TUSD)\b")
BARE_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,9})\b")


def extract_symbols(title: str) -> list[str]:
    """Возвращает торговые символы из заголовка. Сначала ищет готовые пары."""
    if not isinstance(title, str):
        return []
    pairs: list[str] = []
    for m in PAIR_RE.finditer(title):
        s = m.group(1) + m.group(2)
        if s not in pairs:
            pairs.append(s)
    if pairs:
        return pairs
    out: list[str] = []
    for m in BARE_RE.finditer(title):
        t = m.group(1)
        if t in NOISE or t.isdigit():
            continue
        s = t + "USDT"
        if s not in out:
            out.append(s)
    return out


def categorize(title: str) -> str:
    t = (title or "").lower()
    if "futures" in t and "delist" in t:
        return "futures_delist"
    if "delist" in t:
        return "spot_delist"
    if "monitoring" in t:
        return "monitoring_tag"
    return "other"


def depth_usd(book: Any, band_bps: float) -> float:
    """Номинал bids для входа в шорт в полосе от mid. Невалидный стакан → 0."""
    try:
        if not isinstance(book, dict):
            return 0.0
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            return 0.0
        if not math.isfinite(band_bps) or band_bps <= 0:
            return 0.0
        bids = [(float(p), float(q)) for p, q, *_ in bids]
        asks = [(float(p), float(q)) for p, q, *_ in asks]
        if any(not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q < 0
               for p, q in bids + asks):
            return 0.0
        if any(bids[i][0] < bids[i+1][0] for i in range(len(bids)-1)):
            return 0.0
        if any(asks[i][0] > asks[i+1][0] for i in range(len(asks)-1)):
            return 0.0
        best_bid, best_ask = bids[0][0], asks[0][0]
        if best_bid >= best_ask:
            return 0.0
        mid = (best_bid + best_ask) / 2.0
        lo = mid * (1.0 - band_bps / 10_000.0)
        hi = mid * (1.0 + band_bps / 10_000.0)
        # A short sells into bids; asks do not supply entry liquidity.
        total = sum(p * q for p, q in bids if lo <= p <= hi)
        if not math.isfinite(total):
            return 0.0
        return total
    except Exception:
        return 0.0


# --------------------------------------------------------------------------- #
# HTTP с backoff (защита 2)
# --------------------------------------------------------------------------- #

class BackoffClient:
    def __init__(self, cfg: Config, logger: logging.Logger, client: Optional[httpx.AsyncClient] = None,
                 sleep=None, rng: Optional[random.Random] = None):
        self.cfg = cfg
        self.log = logger
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"User-Agent": "newsbot-monitor/2.0"},
        )
        self._sleep = sleep or asyncio.sleep
        self._rng = rng or random.Random()
        self.attempt = 0
        self.count_429 = 0
        self.last_delays: list[float] = []

    def _delay(self) -> float:
        base = self.cfg.backoff_base_s * (2 ** max(0, self.attempt - 1))
        return min(base, self.cfg.backoff_max_s) + self._rng.uniform(0, 1)

    @staticmethod
    def _retry_after(resp: httpx.Response) -> Optional[float]:
        raw = resp.headers.get("Retry-After")
        if not raw:
            return None
        try:
            value = float(raw)
            return max(0.0, value) if math.isfinite(value) else None
        except (TypeError, ValueError):
            try:
                return max(0.0, parsedate_to_datetime(raw).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                return None

    async def request(self, url: str, params: Optional[dict] = None,
                      headers: Optional[dict] = None) -> Optional[httpx.Response]:
        """GET с ретраями. Возвращает ответ только для 200/304, иначе None."""
        self.attempt = 0
        while True:
            try:
                resp = await self.client.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                self.attempt += 1
                if self.attempt >= self.cfg.max_attempts:
                    self.log.error("HTTP failed after %d attempts (%s): %s", self.attempt, url, exc)
                    return None
                delay = self._delay()
                self.last_delays.append(delay)
                self.log.warning("network error %s — retry in %.1fs (attempt %d/%d)",
                                 type(exc).__name__, delay, self.attempt, self.cfg.max_attempts)
                await self._sleep(delay)
                continue

            code = resp.status_code

            if code in (429, 418):
                self.count_429 += 1
                self.attempt += 1
                if self.attempt >= self.cfg.max_attempts:
                    self.log.error("HTTP %d: giving up after %d attempts", code, self.attempt)
                    return None
                retry_after = self._retry_after(resp)
                delay = retry_after if retry_after is not None else self._delay()
                self.last_delays.append(delay)
                self.log.warning("HTTP %d rate limit — sleep %.1fs (attempt %d/%d)",
                                 code, delay, self.attempt, self.cfg.max_attempts)
                await self._sleep(delay)
                continue

            if code >= 500:
                self.attempt += 1
                if self.attempt >= self.cfg.max_attempts:
                    self.log.error("HTTP %d: giving up after %d attempts", code, self.attempt)
                    return None
                delay = self._delay()
                self.last_delays.append(delay)
                self.log.warning("HTTP %d server error — sleep %.1fs (attempt %d/%d)",
                                 code, delay, self.attempt, self.cfg.max_attempts)
                await self._sleep(delay)
                continue

            self.attempt = 0
            if code not in (200, 304):
                self.log.warning("unexpected HTTP %d from %s", code, url)
                return None
            return resp

    async def get_json(self, url: str, params: Optional[dict] = None) -> Any:
        resp = await self.request(url, params)
        if resp is None or resp.status_code != 200:
            return None
        try:
            return resp.json()
        except ValueError:
            self.log.error("invalid JSON from %s", url)
            return None


# --------------------------------------------------------------------------- #
# состояние (защита 6)
# --------------------------------------------------------------------------- #

class SeenState:
    def __init__(self, path: Path, max_ids: int = 2000):
        self.path = path
        self.max_ids = max_ids
        self.ids: list[str] = []
        self._set: set[str] = set()
        self.bootstrapped = False

    def load(self) -> "SeenState":
        try:
            raw = json.loads(self.path.read_text("utf-8"))
            if (not isinstance(raw, dict) or not isinstance(raw.get("ids"), list)
                    or type(raw.get("bootstrapped")) is not bool
                    or any(not isinstance(i, str) for i in raw["ids"])):
                raise ValueError("invalid cursor schema")
            self.ids = list(dict.fromkeys(raw["ids"]))[-self.max_ids:]
            self._set = set(self.ids)
            self.bootstrapped = raw["bootstrapped"]
        except FileNotFoundError:
            pass
        except (ValueError, TypeError):
            # Invalid state must bootstrap again, never process the whole old feed.
            self.ids, self._set, self.bootstrapped = [], set(), False
        return self

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        payload = {"bootstrapped": self.bootstrapped, "ids": self.ids[-self.max_ids:]}
        tmp.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
        os.replace(tmp, self.path)

    def seen(self, aid: str) -> bool:
        return aid in self._set

    def add(self, aid: str) -> None:
        if aid in self._set:
            return
        self._set.add(aid)
        self.ids.append(aid)
        if len(self.ids) > self.max_ids:
            drop = self.ids[: -self.max_ids]
            self.ids = self.ids[-self.max_ids:]
            for d in drop:
                self._set.discard(d)


class SingleInstance:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a+")
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fh.close()
            self._fh = None
            return False
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(str(os.getpid()))
        self._fh.flush()
        return True

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        except OSError:
            pass
        self._fh.close()
        self._fh = None


# --------------------------------------------------------------------------- #
# монитор
# --------------------------------------------------------------------------- #

class Monitor:
    def __init__(self, cfg: Config, logger: logging.Logger, *,
                 client: Optional[BackoffClient] = None,
                 sleep=None,
                 depth_probe=None,
                 time_probe=None,
                 max_loops: Optional[int] = None):
        self.cfg = cfg
        self.log = logger
        self._sleep = sleep or self.interruptible_sleep
        self.client = client or BackoffClient(cfg, logger, sleep=self._sleep)
        self.state = SeenState(cfg.state_path).load()
        self.clock_offset_ms = 0.0
        self.clock_rtt_ms = 0.0
        self.loops = 0
        self.signals = 0
        self.max_loops = max_loops
        self._depth_probe = depth_probe
        self._time_probe = time_probe
        self._stop = False
        self._last_sync = 0.0
        self._last_heartbeat = 0.0
        self._etag: Optional[str] = None
        self._last_cache: Optional[str] = None
        self._last_good_feed = time.monotonic()
        self.lags: list[float] = []

    async def interruptible_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.stop_requested():
                raise asyncio.CancelledError
            await asyncio.sleep(min(0.25, max(0, deadline - time.monotonic())))

    # --- время (защита 5) -------------------------------------------------- #

    def server_now(self) -> float:
        return time.time() + self.clock_offset_ms / 1000.0

    async def sync_clock(self) -> bool:
        if self._time_probe is not None:
            res = self._time_probe()
            if asyncio.iscoroutine(res):
                res = await res
            if not isinstance(res, dict):
                self.log.error("clock sync: probe returned %r", res)
                return False
        else:
            t0 = time.time()
            payload = await self.client.get_json(BINANCE_FAPI_TIME)
            t1 = time.time()
            if not isinstance(payload, dict):
                self.log.error("clock sync failed: no response from %s", BINANCE_FAPI_TIME)
                return False
            try:
                server_ms = int(payload["serverTime"])
            except (KeyError, TypeError, ValueError):
                self.log.error("clock sync failed: no serverTime field")
                return False
            res = {"offset_ms": server_ms - (t0 + t1) / 2.0 * 1000.0,
                   "rtt_ms": (t1 - t0) * 1000.0}
        self.clock_offset_ms = float(res.get("offset_ms", 0.0))
        self.clock_rtt_ms = float(res.get("rtt_ms", 0.0))
        self._last_sync = time.time()
        self.log.info("clock sync: offset=%.0f ms rtt=%.0f ms",
                      self.clock_offset_ms, self.clock_rtt_ms)
        return True

    # --- ликвидность (защита 4) ------------------------------------------- #

    async def check_liquidity(self, symbols: list[str]) -> list[dict]:
        results: list[dict] = []
        for sym in symbols:
            if self._depth_probe is not None:
                res = self._depth_probe(sym)
                if asyncio.iscoroutine(res):
                    res = await res
                results.append(res)
                continue

            book = await self.client.get_json(BINANCE_FAPI_DEPTH, {"symbol": sym, "limit": 50})
            market = "futures"
            usd = depth_usd(book, self.cfg.depth_band_bps)
            results.append({
                "symbol": sym,
                "market": market if usd > 0 else None,
                "depth_usd": usd,
                "ok": usd > 0 and usd >= self.cfg.min_depth_usd,
            })
        return results

    # --- kill switch (защита 1) ------------------------------------------- #

    def stop_requested(self) -> bool:
        return self._stop or self.cfg.stop_path.exists()

    # --- основной цикл ----------------------------------------------------- #

    async def run(self) -> int:
        inst = SingleInstance(self.cfg.pid_path)
        if not inst.acquire():
            self.log.error("another instance is running (lock %s) — exit", self.cfg.pid_path)
            await self.client.client.aclose()
            return 2
        try:
            self.state = SeenState(self.cfg.state_path).load()
            if self.stop_requested():
                self.log.critical("KILL SWITCH before startup — graceful stop")
                return 0
            if not self.state.bootstrapped:
                self.log.info("state: no cursor yet, will bootstrap on first poll")
            await self.sync_clock()
            self.log.info(
                "monitor start | poll=%.0fs stale=%.0fs min_depth=%.0fUSD band=%.0fbps catalog=%d",
                self.cfg.poll_interval_s, self.cfg.stale_after_s,
                self.cfg.min_depth_usd, self.cfg.depth_band_bps, self.cfg.catalog_id,
            )
            while True:
                if self.stop_requested():
                    self.log.critical("KILL SWITCH: %s — graceful stop", self.cfg.stop_path)
                    return 0
                await self.tick()
                silent = time.monotonic() - self._last_good_feed
                if silent > self.cfg.stale_after_s:
                    self.log.error("FEED UNHEALTHY: no valid announcement response for %.0fs", silent)
                self.loops += 1
                if self.max_loops is not None and self.loops >= self.max_loops:
                    self.log.info("max_loops=%d reached — stop", self.max_loops)
                    return 0
                await self._sleep(self.cfg.poll_interval_s)
        except asyncio.CancelledError:
            if not self.stop_requested():
                raise
            return 0
        finally:
            await self.client.client.aclose()
            inst.release()

    async def tick(self) -> None:
        if time.time() - self._last_sync > self.cfg.clock_resync_s:
            await self.sync_clock()

        headers = None
        if self.cfg.use_conditional and self._etag:
            headers = {"If-None-Match": self._etag}

        resp = await self.client.request(
            BINANCE_LIST,
            {"type": 1, "pageNo": 1, "pageSize": self.cfg.page_size,
             "catalogId": self.cfg.catalog_id},
            headers=headers,
        )
        if resp is None:
            self.log.warning("tick: no data from announcements API")
            return

        cache = (resp.headers.get("x-cache") or "").split(" ")[0]
        if cache and cache != self._last_cache:
            self.log.info("cdn cache: %s | etag=%s | poll=%.1fs",
                          cache, resp.headers.get("etag", ""), self.cfg.poll_interval_s)
            self._last_cache = cache

        etag = resp.headers.get("etag")
        if resp.status_code == 304:
            self._last_good_feed = time.monotonic()
            self.log.debug("tick: 304 not modified")
            return

        try:
            payload = resp.json()
        except ValueError:
            self.log.error("tick: invalid JSON from announcements API")
            return

        arts = parse_articles(payload)
        if not arts:
            self.log.warning("tick: announcement list empty or unparsable")
            return

        if etag:
            self._etag = etag
        self._last_good_feed = time.monotonic()
        detected_ms = int(self.server_now() * 1000)
        for art in arts:
            art["detected_ms"] = detected_ms
        if not self.state.bootstrapped:
            for a in arts:
                self.state.add(a["id"])
            self.state.bootstrapped = True
            self.state.save()
            self.log.info(
                "bootstrap: recorded %d announcements, 0 signals "
                "(prevents phantom signals after restart)", len(arts),
            )
            return

        new = [a for a in arts if not self.state.seen(a["id"])]
        new.sort(key=lambda a: a["release_ms"])
        for art in new:
            await self.handle(art)
            self.state.add(art["id"])
            self.state.save()
        if time.time() - self._last_heartbeat > self.cfg.heartbeat_every_s:
            self._last_heartbeat = time.time()
            med = f"{sorted(self.lags)[len(self.lags) // 2]:.1f}s" if self.lags else "n/a"
            self.log.info(
                "heartbeat | loops=%d signals=%d http_429=%d clock_offset=%.0fms rtt=%.0fms "
                "detect_lag_median=%s (n=%d)",
                self.loops, self.signals, getattr(self.client, "count_429", 0),
                self.clock_offset_ms, self.clock_rtt_ms, med, len(self.lags),
            )

    async def handle(self, art: dict) -> None:
        title = art.get("title") or ""
        if not title:
            self.log.warning("MALFORMED id=%s: no title — skipped", art.get("id"))
            return

        rel_ms = int(art.get("release_ms") or 0)
        if rel_ms <= 0:
            self.log.warning("STALE id=%s: no releaseDate — skipped | %s",
                             art.get("id"), title[:90])
            return

        detected_ms = art.get("detected_ms", int(self.server_now() * 1000))
        age = (detected_ms - rel_ms) / 1000.0
        if self.cfg.observations_path is not None:
            path = self.cfg.observations_path
            path.parent.mkdir(parents=True, exist_ok=True)
            observation = dict(id=art.get("id"), code=art.get("code"), title=title,
                               release_ms=rel_ms, detected_ms=detected_ms, detect_lag_s=age,
                               clock_offset_ms=self.clock_offset_ms, clock_rtt_ms=self.clock_rtt_ms,
                               clock_synced=self._last_sync > 0)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(observation, ensure_ascii=False) + "\n")
        self.lags.append(age)
        self.lags = self.lags[-50:]
        self.log.info("NEW id=%s detect_lag=%.1fs | %s", art.get("id"), age, title[:90])
        if age > self.cfg.stale_after_s:
            self.log.info("STALE age=%.0fs (>%.0fs) — skipped | %s",
                          age, self.cfg.stale_after_s, title[:90])
            return
        if age < -120:
            self.log.warning("CLOCK SKEW: announcement %.0fs in the future, offset=%.0fms | %s",
                             -age, self.clock_offset_ms, title[:90])
            return

        cat = categorize(title)
        if cat == "other":
            self.log.info("IGNORED | %s", title[:110])
            return

        symbols = extract_symbols(title)
        if not symbols:
            self.log.warning("SIGNAL %s but no symbols parsed | %s", cat, title[:110])
            return

        if self.stop_requested():
            self.log.critical("KILL SWITCH during signal handling — abort")
            return

        liq = await self.check_liquidity(symbols)
        if self.stop_requested():
            self.log.critical("KILL SWITCH after liquidity check — abort")
            return
        ok = [x for x in liq if x.get("ok") and x.get("market") == "futures"]
        summary = ", ".join(
            f"{x['symbol']}[{x.get('market') or 'no-market'}]={x.get('depth_usd', 0.0):.0f}USD"
            for x in liq
        )
        if not ok:
            self.log.warning(
                "SIGNAL REJECTED: liquidity < %.0f USD | cat=%s | %s | age=%.0fs",
                self.cfg.min_depth_usd, cat, summary, age,
            )
            return

        self.signals += 1
        self.log.info(
            "SIGNAL PAPER (order NOT sent) | cat=%s | %s | age=%.0fs | clock_offset=%.0fms | id=%s | %s",
            cat, summary, age, self.clock_offset_ms, art.get("id"), title[:110],
        )
