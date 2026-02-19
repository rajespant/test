/**
 * Stats Tracker
 * Uses Redis INCRBY for atomic counters shared across all worker threads.
 * Displays a live progress line updated every second.
 */

import { Config } from './config.js';

const KEY_SENT   = `${Config.REDIS_STATS_KEY}:sent`;
const KEY_FAILED = `${Config.REDIS_STATS_KEY}:failed`;

export class StatsTracker {
  /**
   * @param {import('ioredis').Redis} redis
   * @param {number} totalTokens - total number of tokens being sent
   */
  constructor(redis, totalTokens) {
    this.redis      = redis;
    this.total      = totalTokens;
    this.startTime  = Date.now();
  }

  /** Reset counters for a fresh run. */
  async reset() {
    await this.redis.del(KEY_SENT, KEY_FAILED);
    this.startTime = Date.now();
  }

  /**
   * Atomically increment success/failure counters.
   * @param {number} success
   * @param {number} failed
   */
  async increment(success, failed) {
    const pipeline = this.redis.pipeline();
    if (success > 0) pipeline.incrby(KEY_SENT,   success);
    if (failed  > 0) pipeline.incrby(KEY_FAILED,  failed);
    await pipeline.exec();
  }

  /** Take a snapshot of current stats. */
  async snapshot() {
    const [sent, failed] = await this.redis.mget(KEY_SENT, KEY_FAILED);
    const s          = parseInt(sent   || '0');
    const f          = parseInt(failed || '0');
    const elapsedSec = (Date.now() - this.startTime) / 1000;
    const rate       = elapsedSec > 0 ? s / elapsedSec : 0;
    const remaining  = this.total - s - f;
    const eta        = rate > 0 ? remaining / rate : Infinity;

    return { sent: s, failed: f, total: this.total, elapsedSec, rate, eta };
  }
}

/**
 * Prints a live progress line every `intervalMs` milliseconds.
 * Resolves when all tokens have been processed.
 *
 * @param {StatsTracker} tracker
 * @param {number} [intervalMs=1000]
 * @returns {Promise<object>} final snapshot
 */
export async function printProgress(tracker, intervalMs = 1000) {
  return new Promise((resolve) => {
    const timer = setInterval(async () => {
      const s   = await tracker.snapshot();
      const pct = s.total > 0 ? ((s.sent + s.failed) / s.total * 100).toFixed(1) : '0.0';
      const eta = s.eta === Infinity ? '---' : `${s.eta.toFixed(0)}s`;

      process.stdout.write(
        `\r[${s.elapsedSec.toFixed(1).padStart(6)}s] ` +
        `Sent: ${s.sent.toLocaleString().padStart(12)}  ` +
        `Failed: ${s.failed.toLocaleString().padStart(8)}  ` +
        `Rate: ${Math.round(s.rate).toLocaleString().padStart(9)}/s  ` +
        `Progress: ${pct.padStart(5)}%  ` +
        `ETA: ${eta}`
      );

      if (s.sent + s.failed >= s.total) {
        clearInterval(timer);
        process.stdout.write('\n');
        resolve(s);
      }
    }, intervalMs);
  });
}
