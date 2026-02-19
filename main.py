"""
FCM Bulk Notification System
=============================
Sends push notifications to 20 million users in ~60 seconds using:
  - FCM HTTP v1 Batch API (500 tokens/request)
  - asyncio with 500 concurrent workers per process
  - Multiple OS processes (default: 4) for CPU parallelism
  - Redis as the token queue and stats store

Usage:
    python main.py --tokens tokens.txt --title "Hello!" --body "New update available"

    # Or with custom concurrency:
    python main.py --tokens tokens.txt --processes 8 --workers 600

Required env vars (or .env file):
    FIREBASE_CREDENTIALS_PATH  - Path to service account JSON
    FIREBASE_PROJECT_ID        - Firebase project ID
    REDIS_HOST                 - Redis host (default: localhost)
    REDIS_PORT                 - Redis port (default: 6379)
"""

import argparse
import asyncio
import logging
import multiprocessing
import sys
import time

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import aioredis

from config import Config
from stats import StatsTracker, print_progress
from token_manager import enqueue_tokens, get_queue_length
from worker_process import run_process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send FCM push notifications to millions of users."
    )
    parser.add_argument(
        "--tokens",
        required=True,
        help="Path to file containing FCM device tokens (one per line)",
    )
    parser.add_argument("--title", default=Config.DEFAULT_TITLE, help="Notification title")
    parser.add_argument("--body", default=Config.DEFAULT_BODY, help="Notification body")
    parser.add_argument("--image", default=Config.DEFAULT_IMAGE, help="Notification image URL")
    parser.add_argument(
        "--data",
        nargs="*",
        metavar="KEY=VALUE",
        default=[],
        help="Custom data payload as key=value pairs",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=Config.NUM_PROCESSES,
        help=f"Number of worker processes (default: {Config.NUM_PROCESSES})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=Config.CONCURRENT_WORKERS,
        help=f"Async workers per process (default: {Config.CONCURRENT_WORKERS})",
    )
    parser.add_argument(
        "--skip-enqueue",
        action="store_true",
        help="Skip loading tokens into Redis (assume already loaded)",
    )
    return parser.parse_args()


def parse_data_args(data_args: list[str]) -> dict:
    result = {}
    for item in data_args:
        if "=" in item:
            k, v = item.split("=", 1)
            result[k] = v
    return result


async def setup_and_enqueue(token_file: str) -> int:
    """Connect to Redis, clear old state, enqueue tokens. Returns total count."""
    redis = aioredis.from_url(
        f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )
    # Clean up previous run
    await redis.delete(
        Config.REDIS_TOKEN_QUEUE_KEY,
        Config.REDIS_FAILED_QUEUE_KEY,
        f"{Config.REDIS_STATS_KEY}:sent",
        f"{Config.REDIS_STATS_KEY}:failed",
    )
    logger.info("Enqueueing tokens from %s ...", token_file)
    total = await enqueue_tokens(redis, token_file)
    await redis.aclose()
    return total


async def monitor_until_done(total: int):
    """Attach to Redis stats and print live progress until all tokens are processed."""
    redis = aioredis.from_url(
        f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )
    tracker = StatsTracker(redis, total)
    await print_progress(tracker, interval=1.0)
    snap = await tracker.snapshot()
    await redis.aclose()
    return snap


def main():
    args = parse_args()
    data = parse_data_args(args.data)

    # Override config from CLI
    Config.CONCURRENT_WORKERS = args.workers
    Config.NUM_PROCESSES = args.processes

    # ── Step 1: Enqueue tokens into Redis ──────────────────────────────────────
    if not args.skip_enqueue:
        total = asyncio.run(setup_and_enqueue(args.tokens))
    else:
        total = asyncio.run(_get_queue_len())
    logger.info("Total tokens to send: %d", total)

    # ── Step 2: Spawn worker processes ─────────────────────────────────────────
    logger.info(
        "Starting %d processes × %d workers = %d concurrent requests",
        args.processes,
        args.workers,
        args.processes * args.workers,
    )
    start = time.time()
    processes = []
    for pid in range(args.processes):
        p = multiprocessing.Process(
            target=run_process,
            args=(pid, total, args.title, args.body, args.image, data),
            daemon=True,
        )
        p.start()
        processes.append(p)

    # ── Step 3: Live progress monitoring ───────────────────────────────────────
    snap = asyncio.run(monitor_until_done(total))

    for p in processes:
        p.join(timeout=5)

    elapsed = time.time() - start

    # ── Step 4: Final report ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  FCM Bulk Send Complete")
    print("=" * 60)
    print(f"  Total tokens   : {total:>12,}")
    print(f"  Sent (success) : {snap.sent:>12,}")
    print(f"  Failed         : {snap.failed:>12,}")
    print(f"  Elapsed        : {elapsed:>11.2f}s")
    print(f"  Avg rate       : {snap.sent / elapsed:>10,.0f} /s")
    print(f"  Success rate   : {snap.sent / total * 100:>10.2f}%")
    print("=" * 60)

    if snap.failed > 0:
        print(f"\nFailed tokens saved to Redis key: {Config.REDIS_FAILED_QUEUE_KEY}")
        print("Run `redis-cli lrange fcm:failed_tokens 0 -1` to inspect them.")

    return 0 if snap.failed == 0 else 1


async def _get_queue_len() -> int:
    redis = aioredis.from_url(
        f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )
    n = await get_queue_length(redis)
    await redis.aclose()
    return n


if __name__ == "__main__":
    sys.exit(main())
