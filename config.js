import 'dotenv/config';

export const Config = {
  // ── Firebase ──────────────────────────────────────────────────────────────
  FIREBASE_CREDENTIALS_PATH: process.env.FIREBASE_CREDENTIALS_PATH || 'serviceAccountKey.json',
  FIREBASE_PROJECT_ID:       process.env.FIREBASE_PROJECT_ID || '',

  // ── Redis (token queue + stats store) ────────────────────────────────────
  REDIS_HOST:             process.env.REDIS_HOST || 'localhost',
  REDIS_PORT:             parseInt(process.env.REDIS_PORT || '6379'),
  REDIS_DB:               parseInt(process.env.REDIS_DB   || '0'),
  REDIS_TOKEN_QUEUE_KEY:  'fcm:token_queue',
  REDIS_FAILED_QUEUE_KEY: 'fcm:failed_tokens',
  REDIS_STATS_KEY:        'fcm:stats',

  // ── Performance ───────────────────────────────────────────────────────────
  BATCH_SIZE:         500,   // FCM multicast max tokens per request
  CONCURRENT_WORKERS: 500,   // concurrent requests per worker thread
  NUM_THREADS:        parseInt(process.env.NUM_THREADS || '4'),
  MAX_RETRIES:        3,
  RETRY_BACKOFF_MS:   1000,  // base delay for exponential backoff

  // ── Notification defaults ─────────────────────────────────────────────────
  DEFAULT_TITLE: process.env.NOTIF_TITLE || 'New Notification',
  DEFAULT_BODY:  process.env.NOTIF_BODY  || 'You have a new message',
  DEFAULT_IMAGE: process.env.NOTIF_IMAGE || '',
};
