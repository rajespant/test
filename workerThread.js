/**
 * Worker Thread Entry Point
 * Node.js spawns this file as a worker thread via worker_threads.
 * Each thread runs its own event loop with CONCURRENT_WORKERS async workers.
 *
 * Receives { threadId, totalTokens, notification } via workerData.
 * Reports completion back to the main thread via parentPort.
 */

import { workerData, parentPort } from 'worker_threads';
import { runWorkers } from './bulkSender.js';

const { threadId, totalTokens, notification } = workerData;

// Each thread uses a unique Firebase app name to avoid "app already exists" error
const appName = `fcm-thread-${threadId}`;

runWorkers(notification, totalTokens, appName)
  .then(() => {
    parentPort.postMessage({ status: 'done', threadId });
  })
  .catch((err) => {
    parentPort.postMessage({ status: 'error', threadId, message: err.message });
    process.exit(1);
  });
