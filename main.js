#!/usr/bin/env node
/**
 * FCM Bulk Notification System — Main Entry Point
 * ================================================
 * Usage:
 *   node main.js --tokens tokens.txt --title "Hello!" --body "Check this out"
 *
 * Required env vars (or .env file):
 *   FIREBASE_CREDENTIALS_PATH  — path to serviceAccountKey.json
 *   FIREBASE_PROJECT_ID        — your Firebase project ID
 *   REDIS_HOST                 — Redis host (default: localhost)
 *   REDIS_PORT                 — Redis port (default: 6379)
 */

import { Worker }    from 'worker_threads';
import { fileURLToPath } from 'url';
import path          from 'path';
import Redis         from 'ioredis';
import yargs         from 'yargs';
import { hideBin }   from 'yargs/helpers';

import { Config }           from './config.js';
import { enqueueTokens, getQueueLength } from './tokenManager.js';
import { StatsTracker, printProgress }   from './stats.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── CLI arguments ────────────────────────────────────────────────────────────
const argv = yargs(hideBin(process.argv))
  .usage('Usage: $0 --tokens <file> [options]')
  .option('tokens',       { type: 'string',  demandOption: true,  describe: 'File with one FCM token per line' })
  .option('title',        { type: 'string',  default: Config.DEFAULT_TITLE,  describe: 'Notification title' })
  .option('body',         { type: 'string',  default: Config.DEFAULT_BODY,   describe: 'Notification body' })
  .option('image',        { type: 'string',  default: Config.DEFAULT_IMAGE,  describe: 'Notification image URL' })
  .option('data',         { type: 'array',   default: [],         describe: 'Extra data as key=value pairs' })
  .option('threads',      { type: 'number',  default: Config.NUM_THREADS,    describe: 'Number of worker threads' })
  .option('workers',      { type: 'number',  default: Config.CONCURRENT_WORKERS, describe: 'Concurrent requests per thread' })
  .option('skip-enqueue', { type: 'boolean', default: false,      describe: 'Skip loading tokens (already in Redis)' })
  .help()
  .parseSync();

/** Parse "key=value" pairs into a plain object */
function parseDataArgs(dataArgs) {
  const result = {};
  for (const item of dataArgs) {
    const eqIdx = item.indexOf('=');
    if (eqIdx !== -1) {
      result[item.slice(0, eqIdx)] = item.slice(eqIdx + 1);
    }
  }
  return result;
}

function createRedis() {
  return new Redis({
    host: Config.REDIS_HOST,
    port: Config.REDIS_PORT,
    db:   Config.REDIS_DB,
    maxRetriesPerRequest: 3,
  });
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  // Override config from CLI
  Config.NUM_THREADS         = argv.threads;
  Config.CONCURRENT_WORKERS  = argv.workers;

  const notification = {
    title: argv.title,
    body:  argv.body,
    image: argv.image,
    data:  parseDataArgs(argv.data),
  };

  const redis = createRedis();

  // ── Step 1: Load tokens into Redis ──────────────────────────────────────
  let totalTokens;

  if (!argv['skip-enqueue']) {
    // Clear any leftover state from a previous run
    await redis.del(
      Config.REDIS_TOKEN_QUEUE_KEY,
      Config.REDIS_FAILED_QUEUE_KEY,
      `${Config.REDIS_STATS_KEY}:sent`,
      `${Config.REDIS_STATS_KEY}:failed`,
    );

    console.log(`\nLoading tokens from "${argv.tokens}" into Redis...`);
    totalTokens = await enqueueTokens(redis, argv.tokens);
  } else {
    totalTokens = await getQueueLength(redis);
    console.log(`\nSkipping enqueue. Tokens already in Redis: ${totalTokens.toLocaleString()}`);
  }

  if (totalTokens === 0) {
    console.error('No tokens found. Exiting.');
    process.exit(1);
  }

  // Reset stats counters
  const tracker = new StatsTracker(redis, totalTokens);
  await tracker.reset();

  // ── Step 2: Spawn worker threads ─────────────────────────────────────────
  console.log(
    `\nStarting ${argv.threads} worker thread(s) × ${argv.workers} concurrent workers` +
    ` = ${(argv.threads * argv.workers).toLocaleString()} parallel requests\n`
  );

  const startTime = Date.now();
  const workerFile = path.join(__dirname, 'workerThread.js');
  const workers = [];

  for (let i = 0; i < argv.threads; i++) {
    const w = new Worker(workerFile, {
      workerData: { threadId: i, totalTokens, notification },
    });
    w.on('message', (msg) => {
      if (msg.status === 'error') {
        console.error(`\nThread ${msg.threadId} error: ${msg.message}`);
      }
    });
    w.on('error', (err) => console.error(`\nWorker error: ${err.message}`));
    workers.push(w);
  }

  // ── Step 3: Live progress display ────────────────────────────────────────
  const finalSnap = await printProgress(tracker, 1000);

  // Wait for all threads to finish
  await Promise.all(workers.map((w) => new Promise((res) => w.on('exit', res))));

  const elapsedSec = (Date.now() - startTime) / 1000;

  // ── Step 4: Final report ─────────────────────────────────────────────────
  console.log('\n' + '='.repeat(60));
  console.log('  FCM Bulk Send Complete');
  console.log('='.repeat(60));
  console.log(`  Total tokens   : ${totalTokens.toLocaleString().padStart(14)}`);
  console.log(`  Sent (success) : ${finalSnap.sent.toLocaleString().padStart(14)}`);
  console.log(`  Failed         : ${finalSnap.failed.toLocaleString().padStart(14)}`);
  console.log(`  Elapsed        : ${elapsedSec.toFixed(2).padStart(13)}s`);
  console.log(`  Avg rate       : ${Math.round(finalSnap.sent / elapsedSec).toLocaleString().padStart(12)}/s`);
  console.log(`  Success rate   : ${(finalSnap.sent / totalTokens * 100).toFixed(2).padStart(12)}%`);
  console.log('='.repeat(60));

  if (finalSnap.failed > 0) {
    console.log(`\nFailed tokens stored in Redis key: "${Config.REDIS_FAILED_QUEUE_KEY}"`);
    console.log('Retry with:  node main.js --tokens failed.txt  (after exporting them)');
  }

  await redis.quit();
  process.exit(finalSnap.failed === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
