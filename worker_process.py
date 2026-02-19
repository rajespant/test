"""
Worker Process
Each OS process runs its own asyncio event loop with CONCURRENT_WORKERS async workers.
Spawned by main.py via multiprocessing.

Using uvloop (if available) for faster I/O than the default asyncio event loop.
"""

import asyncio
import logging

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # Fall back to default event loop

import aioredis

from bulk_sender import run_workers
from config import Config
from fcm_client import NotificationPayload
from stats import StatsTracker

logging.basicConfig(
    level=logging.WARNING,  # Keep quiet during bulk run
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def run_process(
    process_id: int,
    total_tokens: int,
    title: str,
    body: str,
    image: str,
    data: dict,
):
    """Entry point for each worker process. Runs the asyncio event loop."""

    async def _main():
        redis = aioredis.from_url(
            f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
            encoding="utf-8",
            decode_responses=True,
            max_connections=Config.CONCURRENT_WORKERS + 10,
        )
        notification = NotificationPayload(
            title=title,
            body=body,
            image=image,
            data=data,
        )
        tracker = StatsTracker(redis, total_tokens)

        await run_workers(
            redis=redis,
            notification=notification,
            tracker=tracker,
            num_workers=Config.CONCURRENT_WORKERS,
        )
        await redis.close()

    asyncio.run(_main())
