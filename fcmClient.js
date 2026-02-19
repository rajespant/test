/**
 * FCM Client
 * Wraps the Firebase Admin SDK to send multicast messages.
 * Each worker thread initializes its own Firebase app instance.
 *
 * firebase-admin's sendEachForMulticast() sends to up to 500 tokens
 * per call and returns per-token success/failure results.
 */

import { readFileSync } from 'fs';
import admin from 'firebase-admin';
import { Config } from './config.js';

let app = null;

/**
 * Initialize Firebase Admin SDK (once per process / worker thread).
 * Pass a unique `appName` when running multiple instances in the same process.
 * @param {string} [appName='default']
 */
export function initFirebase(appName = 'default') {
  if (app) return; // already initialized

  const serviceAccount = JSON.parse(readFileSync(Config.FIREBASE_CREDENTIALS_PATH, 'utf8'));

  // Use project ID from env or fall back to service account file
  const projectId = Config.FIREBASE_PROJECT_ID || serviceAccount.project_id;

  app = admin.initializeApp(
    {
      credential: admin.credential.cert(serviceAccount),
      projectId,
    },
    appName,
  );
}

/**
 * Result of a single multicast batch send.
 * @typedef {Object} BatchResult
 * @property {number} successCount
 * @property {number} failureCount
 * @property {string[]} failedTokens
 * @property {number} durationMs
 */

/**
 * Send a notification to up to 500 tokens in a single FCM multicast call.
 * Retries the failed tokens up to MAX_RETRIES times with exponential backoff.
 *
 * @param {string[]} tokens
 * @param {{ title: string, body: string, image?: string, data?: Record<string,string> }} notification
 * @returns {Promise<BatchResult>}
 */
export async function sendMulticast(tokens, notification) {
  const start = Date.now();

  const message = {
    tokens,
    notification: {
      title: notification.title,
      body:  notification.body,
      ...(notification.image ? { imageUrl: notification.image } : {}),
    },
    ...(notification.data && Object.keys(notification.data).length > 0
      ? { data: notification.data }
      : {}),
  };

  let currentTokens = tokens;
  let totalSuccess = 0;
  let totalFailed = 0;
  let failedTokens = [];

  for (let attempt = 0; attempt < Config.MAX_RETRIES; attempt++) {
    const response = await admin.messaging().sendEachForMulticast({ ...message, tokens: currentTokens });

    totalSuccess += response.successCount;
    totalFailed  = response.failureCount;
    failedTokens = [];

    // Collect tokens that failed so we can retry them
    response.responses.forEach((res, idx) => {
      if (!res.success) {
        failedTokens.push(currentTokens[idx]);
      }
    });

    if (failedTokens.length === 0 || attempt === Config.MAX_RETRIES - 1) break;

    // Exponential backoff before retrying failed tokens only
    const delay = Config.RETRY_BACKOFF_MS * Math.pow(2, attempt);
    await sleep(delay);
    currentTokens = failedTokens;
  }

  return {
    successCount: totalSuccess,
    failureCount: failedTokens.length,
    failedTokens,
    durationMs: Date.now() - start,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
