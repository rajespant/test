# FCM Bulk Notification System (Node.js)

Send push notifications to **20 million users in ~60 seconds** using Firebase Cloud Messaging and Node.js.

## How it works

```
tokens.txt  ──►  Redis Queue  (40,000 batches of 500 tokens)
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
     [Thread 0]  [Thread 1]  [Thread N]
     500 async   500 async   500 async
     workers     workers     workers
          │           │           │
          └───────────┼───────────┘
                      ▼
               FCM Batch API
          (500 tokens per request)
```

**Why it's fast:**

| Technique | Benefit |
|-----------|---------|
| FCM multicast | 500 tokens per HTTP call → only 40,000 calls for 20M users |
| `worker_threads` | True parallelism, bypasses Node.js single-thread limit |
| 500 async workers/thread | Zero idle time waiting for FCM responses |
| Redis pipeline | Load 20M tokens into queue in seconds |
| Connection pooling | Reuse TCP sockets to FCM |

**Throughput math:**
```
Target : 20,000,000 users / 60 seconds = 333,333 users/sec
Batches: 20,000,000 / 500 = 40,000 requests
Rate   : 40,000 / 60 ≈ 667 req/sec needed

Capacity per thread (500 workers × avg 150ms latency) = 3,333 req/sec
4 threads × 3,333 = ~13,000 req/sec  →  20× more than needed ✓
```

---

## Setup

### 1. Requirements
- Node.js 18+
- Redis (local or remote)
- Firebase project with Cloud Messaging enabled
- Service account JSON key (with `firebase.messaging` permission)

### 2. Install dependencies
```bash
npm install
```

### 3. Configure environment
```bash
cp .env.example .env
```

Edit `.env`:
```env
FIREBASE_CREDENTIALS_PATH=serviceAccountKey.json
FIREBASE_PROJECT_ID=your-project-id
REDIS_HOST=localhost
```

### 4. Prepare your tokens file
One FCM device token per line:
```
fXx9z2abc...:APA91bHp...
dYm3k1xyz...:APA91bKq...
...
```

Generate 20M test tokens (for testing only):
```bash
node generateTestTokens.js --count 20000000 --output tokens.txt
```

### 5. Run
```bash
node main.js --tokens tokens.txt --title "Big Sale!" --body "50% off today only"
```

With custom data payload:
```bash
node main.js \
  --tokens tokens.txt \
  --title "Flash Sale" \
  --body "Ends in 1 hour!" \
  --image https://example.com/banner.jpg \
  --data screen=sale promo_code=FLASH50
```

---

## All options

```
--tokens        Path to device token file         (required)
--title         Notification title                (default: from .env)
--body          Notification body text            (default: from .env)
--image         Notification image URL            (optional)
--data          Custom key=value data pairs       (optional, multiple)
--threads       Worker threads to spawn           (default: 4)
--workers       Async workers per thread          (default: 500)
--skip-enqueue  Skip loading tokens into Redis    (if already loaded)
```

---

## Live output

```
[   2.1s] Sent:    1,234,000  Failed:          0  Rate:  590,000/s  Progress:  6.2%  ETA: 32s
[   4.2s] Sent:    2,468,000  Failed:          0  Rate:  588,000/s  Progress: 12.3%  ETA: 30s
...
============================================================
  FCM Bulk Send Complete
============================================================
  Total tokens   :     20,000,000
  Sent (success) :     19,987,432
  Failed         :         12,568
  Elapsed        :         58.71s
  Avg rate       :    340,442/s
  Success rate   :         99.94%
============================================================
```

---

## Retrying failed tokens

Failed tokens are saved to Redis key `fcm:failed_tokens`:
```bash
# Export failed tokens
redis-cli lrange fcm:failed_tokens 0 -1 > failed.txt

# Retry
node main.js --tokens failed.txt
```

---

## Scaling up

| Setup | Config |
|-------|--------|
| 8-core server | `--threads 8 --workers 600` |
| Multiple servers | Each server runs independently, sharing the same Redis |
| 100M+ users | Partition tokens.txt across servers, each has its own Redis queue |

---

## File overview

```
├── main.js               # Entry point: CLI → enqueue → spawn threads → report
├── workerThread.js       # Worker thread entry point (one per thread)
├── bulkSender.js         # Async worker pool (500 concurrent FCM requests)
├── fcmClient.js          # Firebase Admin SDK wrapper for multicast
├── tokenManager.js       # Load tokens file → Redis queue
├── stats.js              # Shared Redis counters + live progress display
├── config.js             # All settings from environment variables
├── generateTestTokens.js # Generate fake tokens for load testing
├── package.json
└── .env.example
```
