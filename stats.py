"""
Real-time stats tracker using Redis atomic counters.
Shared across all worker processes.
"""

import asyncio
import time
from dataclasses import dataclass

import aioredis

from config import Config


@dataclass
class Stats:
    sent: int = 0
    failed: int = 0
    total: int = 0
    elapsed_sec: float = 0.0
    rate_per_sec: float = 0.0
    eta_sec: float = 0.0


class StatsTracker:
    def __init__(self, redis: aioredis.Redis, total_tokens: int):
        self._redis = redis
        self._total = total_tokens
        self._start_time = time.time()
        self._key_sent = f"{Config.REDIS_STATS_KEY}:sent"
        self._key_failed = f"{Config.REDIS_STATS_KEY}:failed"

    async def reset(self):
        await self._redis.delete(self._key_sent, self._key_failed)
        self._start_time = time.time()

    async def increment(self, success: int, failed: int):
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.incrby(self._key_sent, success)
            pipe.incrby(self._key_failed, failed)
            await pipe.execute()

    async def snapshot(self) -> Stats:
        sent, failed = await self._redis.mget(self._key_sent, self._key_failed)
        sent = int(sent or 0)
        failed = int(failed or 0)
        elapsed = time.time() - self._start_time
        rate = sent / elapsed if elapsed > 0 else 0
        remaining = self._total - sent - failed
        eta = remaining / rate if rate > 0 else float("inf")
        return Stats(
            sent=sent,
            failed=failed,
            total=self._total,
            elapsed_sec=elapsed,
            rate_per_sec=rate,
            eta_sec=eta,
        )


async def print_progress(tracker: StatsTracker, interval: float = 2.0):
    """Continuously print progress every *interval* seconds."""
    while True:
        s = await tracker.snapshot()
        pct = (s.sent + s.failed) / s.total * 100 if s.total else 0
        eta_str = f"{s.eta_sec:.0f}s" if s.eta_sec != float("inf") else "---"
        print(
            f"\r[{s.elapsed_sec:6.1f}s] "
            f"Sent: {s.sent:>10,}  Failed: {s.failed:>8,}  "
            f"Rate: {s.rate_per_sec:>8,.0f}/s  "
            f"Progress: {pct:5.1f}%  ETA: {eta_str}",
            end="",
            flush=True,
        )
        if s.sent + s.failed >= s.total:
            print()  # newline at end
            break
        await asyncio.sleep(interval)
