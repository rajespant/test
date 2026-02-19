/**
 * Token Manager
 * Reads device tokens from a file (one per line) and loads them
 * into a Redis list so worker threads can pop batches concurrently.
 */

import fs from 'fs';
import readline from 'readline';
import { Config } from './config.js';

/**
 * Stream tokens from a file and push them into Redis in pipeline batches.
 * @param {import('ioredis').Redis} redis
 * @param {string} filePath - path to file with one FCM token per line
 * @returns {Promise<number>} total tokens enqueued
 */
export async function enqueueTokens(redis, filePath) {
  const PIPELINE_SIZE = 5_000; // flush Redis pipeline every N tokens

  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let batch = [];
  let total = 0;

  for await (const line of rl) {
    const token = line.trim();
    if (!token) continue;

    batch.push(token);
    total++;

    if (batch.length >= PIPELINE_SIZE) {
      const pipeline = redis.pipeline();
      for (const t of batch) pipeline.rpush(Config.REDIS_TOKEN_QUEUE_KEY, t);
      await pipeline.exec();
      batch = [];
      process.stdout.write(`\r  Enqueued ${total.toLocaleString()} tokens...`);
    }
  }

  // Flush remaining tokens
  if (batch.length > 0) {
    const pipeline = redis.pipeline();
    for (const t of batch) pipeline.rpush(Config.REDIS_TOKEN_QUEUE_KEY, t);
    await pipeline.exec();
  }

  console.log(`\r  Enqueued ${total.toLocaleString()} tokens total.   `);
  return total;
}

/**
 * Atomically pop up to `size` tokens from the Redis queue using a Lua script.
 * This prevents multiple workers from getting the same tokens.
 * @param {import('ioredis').Redis} redis
 * @param {number} size
 * @returns {Promise<string[]>}
 */
export async function popBatch(redis, size) {
  const luaScript = `
    local tokens = {}
    for i = 1, tonumber(ARGV[1]) do
      local val = redis.call('LPOP', KEYS[1])
      if val == false then break end
      tokens[#tokens + 1] = val
    end
    return tokens
  `;
  const result = await redis.eval(luaScript, 1, Config.REDIS_TOKEN_QUEUE_KEY, size);
  return result || [];
}

/**
 * Returns the current number of tokens still in the queue.
 */
export async function getQueueLength(redis) {
  return redis.llen(Config.REDIS_TOKEN_QUEUE_KEY);
}
