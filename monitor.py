#!/usr/bin/env python3
"""Точка входа монитора. Вся логика — в monitor_core.py."""

import asyncio
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from monitor_core import Config, Monitor, setup_logging  # noqa: E402


async def amain(cfg: Config, monitor: Monitor) -> int:
    loop = asyncio.get_running_loop()

    def _request_stop(*_args) -> None:
        monitor._stop = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    return await monitor.run()


def main() -> int:
    cfg = Config.from_env()
    logger = setup_logging(cfg.log_path)
    monitor = Monitor(cfg, logger)
    try:
        return asyncio.run(amain(cfg, monitor))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
