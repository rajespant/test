"""
Token Manager
Loads device tokens from various sources and enqueues them into Redis
for parallel workers to consume.

Sources supported:
  - CSV / text file (one token per line)
  - Redis SET (pre-populated)
  - PostgreSQL / MySQL query (via DB_URL env var)
"""

import asyncio
import logging
from typing import AsyncIterator

import aioredis

from config import Config

logger = logging.getLogger(__name__)


async def stream_tokens_from_file(path: str) -> AsyncIterator[str]:
    """Yield tokens one-by-one from a flat text file (one FCM token per line)."""
    with open(path, "r") as f:
        for line in f:
            token = line.strip()
            if token:
                yield token


async def enqueue_tokens(redis: aioredis.Redis, token_source: str) -> int:
    """
    Read tokens from *token_source* (file path) and push them into Redis
    in pipeline batches for high throughput.

    Returns total number of tokens enqueued.
    """
    count = 0
    pipeline_batch: list[str] = []
    PIPELINE_SIZE = 5_000  # flush Redis pipeline every N tokens

    async for token in stream_tokens_from_file(token_source):
        pipeline_batch.append(token)
        count += 1

        if len(pipeline_batch) >= PIPELINE_SIZE:
            async with redis.pipeline(transaction=False) as pipe:
                for t in pipeline_batch:
                    pipe.rpush(Config.REDIS_TOKEN_QUEUE_KEY, t)
                await pipe.execute()
            pipeline_batch.clear()
            logger.info("Enqueued %d tokens so far...", count)

    # Flush remaining
    if pipeline_batch:
        async with redis.pipeline(transaction=False) as pipe:
            for t in pipeline_batch:
                pipe.rpush(Config.REDIS_TOKEN_QUEUE_KEY, t)
            await pipe.execute()

    logger.info("Total tokens enqueued: %d", count)
    return count


async def get_queue_length(redis: aioredis.Redis) -> int:
    return await redis.llen(Config.REDIS_TOKEN_QUEUE_KEY)


async def pop_batch(redis: aioredis.Redis, size: int) -> list[str]:
    """
    Atomically pop *size* tokens from the left of the Redis list.
    Uses a Lua script for atomicity so multiple workers don't overlap.
    """
    lua_script = """
    local tokens = {}
    for i = 1, tonumber(ARGV[1]) do
        local val = redis.call('LPOP', KEYS[1])
        if val == false then break end
        tokens[#tokens + 1] = val
    end
    return tokens
    """
    result = await redis.eval(lua_script, 1, Config.REDIS_TOKEN_QUEUE_KEY, size)
    return result or []
