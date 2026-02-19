/**
 * Bulk Sender
 * Runs a pool of concurrent "workers" (Promises, not threads).
 * Each worker loops: pop a batch → send FCM → update stats → repeat.
 *
 * Concurrency math for 20M users in 60 seconds:
 *   20,000,000 / 500 tokens     = 40,000 batches
 *   40,000 / 60s                = ~667 batches/second needed
 *   500 concurrent × 150ms avg  = 3,333 batches/second capacity per thread
 *   4 threads × 3,333           = ~13,000 batches/second (20× headroom)
 */

import Redis from 'ioredis';
import { Config }        from './config.js';
import { initFirebase, sendMulticast } from './fcmClient.js';
import { StatsTracker }  from './stats.js';
import { popBatch }      from './tokenManager.js';

/**
 * @typedef {Object} NotificationPayload
 * @property {string} title
 * @property {string} body
 * @property {string} [image]
 * @property {Record<string,string>} [data]
 */

/**
 * Run one "worker" coroutine that continuously pops batches and sends them.
 * Stops when the queue is empty.
 *
 * @param {import('ioredis').Redis} redis
 * @param {NotificationPayload} notification
 * @param {StatsTracker} tracker
 */
async function worker(redis, notification, tracker) {
  while (true) {
    const tokens = await popBatch(redis, Config.BATCH_SIZE);

    // Empty queue → this worker is done
    if (tokens.length === 0) return;

    const result = await sendMulticast(tokens, notification);

    // Record stats
    await tracker.increment(result.successCount, result.failureCount);

    // Persist failed tokens for later retry
    if (result.failedTokens.length > 0) {
      const pipeline = redis.pipeline();
      for (const t of result.failedTokens) {
        pipeline.rpush(Config.REDIS_FAILED_QUEUE_KEY, t);
      }
      await pipeline.exec();
    }
  }
}

/**
 * Spin up `numWorkers` concurrent worker coroutines in this thread.
 * All workers share a single Redis connection and Firebase app.
 *
 * @param {NotificationPayload} notification
 * @param {number} totalTokens
 * @param {string} firebaseAppName - unique name per worker thread
 */
export async function runWorkers(notification, totalTokens, firebaseAppName) {
  // Each worker thread gets its own Firebase + Redis instance
  initFirebase(firebaseAppName);

  const redis = new Redis({
    host:            Config.REDIS_HOST,
    port:            Config.REDIS_PORT,
    db:              Config.REDIS_DB,
    maxRetriesPerRequest: 3,
    lazyConnect:     false,
  });

  const tracker = new StatsTracker(redis, totalTokens);

  // Launch CONCURRENT_WORKERS promises in parallel
  const workers = Array.from(
    { length: Config.CONCURRENT_WORKERS },
    () => worker(redis, notification, tracker)
  );

  await Promise.all(workers);
  await redis.quit();
}
