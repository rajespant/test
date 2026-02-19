"""
Bulk Sender
Async worker pool that drains the Redis token queue and fires
FCM multicast requests at maximum throughput.

Math for 20M users in 60 seconds:
  - 20_000_000 / 500 (batch size)  = 40,000 batches
  - 40,000 / 60s                   = ~667 batches/second
  - With 500 concurrent workers,
    each request only needs to take < 750ms on average to hit the target.
"""

import asyncio
import logging
import time
from typing import Optional

import aioredis

from config import Config
from fcm_client import FCMClient, NotificationPayload
from stats import StatsTracker
from token_manager import pop_batch

logger = logging.getLogger(__name__)


async def worker(
    worker_id: int,
    redis: aioredis.Redis,
    client: FCMClient,
    notification: NotificationPayload,
    tracker: StatsTracker,
    semaphore: asyncio.Semaphore,
    stop_event: asyncio.Event,
):
    """
    A single async worker that:
    1. Pops a batch of tokens from Redis
    2. Sends multicast FCM request
    3. Updates stats
    4. Retries failed tokens once
    """
    while not stop_event.is_set():
        async with semaphore:
            tokens = await pop_batch(redis, Config.BATCH_SIZE)
            if not tokens:
                # Queue is empty, signal stop after brief pause
                await asyncio.sleep(0.05)
                stop_event.set()
                return

            # Send with retry
            result = None
            for attempt in range(Config.MAX_RETRIES):
                result = await client.send_multicast(tokens, notification)
                if result.failure_count == 0 or attempt == Config.MAX_RETRIES - 1:
                    break
                # Retry only failed tokens
                tokens = result.failed_tokens
                await asyncio.sleep(Config.RETRY_BACKOFF_BASE * (2 ** attempt))

            await tracker.increment(result.success_count, result.failure_count)

            # Store persistently-failed tokens for later analysis
            if result.failed_tokens:
                async with redis.pipeline(transaction=False) as pipe:
                    for t in result.failed_tokens:
                        pipe.rpush(Config.REDIS_FAILED_QUEUE_KEY, t)
                    await pipe.execute()

            logger.debug(
                "Worker %d: sent=%d failed=%d in %.0fms",
                worker_id,
                result.success_count,
                result.failure_count,
                result.duration_ms,
            )


async def run_workers(
    redis: aioredis.Redis,
    notification: NotificationPayload,
    tracker: StatsTracker,
    num_workers: int = Config.CONCURRENT_WORKERS,
):
    """
    Spin up *num_workers* async workers sharing a single FCMClient session.
    Uses a semaphore to cap concurrency and an event to signal completion.
    """
    client = FCMClient()
    await client.start()

    semaphore = asyncio.Semaphore(num_workers)
    stop_event = asyncio.Event()

    tasks = [
        asyncio.create_task(
            worker(i, redis, client, notification, tracker, semaphore, stop_event)
        )
        for i in range(num_workers)
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        await client.close()
