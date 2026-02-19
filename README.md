# FCM Bulk Notification System

Send push notifications to **20 million users in ~60 seconds** using Firebase Cloud Messaging (FCM).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        main.py                              │
│  1. Load tokens.txt → Redis queue (40,000 batches of 500)   │
│  2. Spawn N worker processes                                │
│  3. Monitor live stats via Redis counters                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ multiprocessing
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   [Process 0]       [Process 1]  ...  [Process N]
   500 async         500 async         500 async
   workers           workers           workers
         │                 │                 │
         └─────────────────┼─────────────────┘
                           │ HTTP/2 keep-alive
                           ▼
                    FCM Batch API
               (500 tokens/request)
```

### Why this is fast

| Factor | Detail |
|--------|--------|
| **FCM Multicast** | 500 tokens per HTTP request → only 40,000 requests for 20M users |
| **asyncio** | 500 concurrent requests per process, no thread overhead |
| **uvloop** | C-based event loop, ~2× faster than default asyncio |
| **Multi-process** | Bypass Python GIL; 4 processes × 500 workers = 2,000 concurrent requests |
| **Connection pooling** | Reuse TCP connections to FCM, avoid TLS handshake per request |
| **Redis pipeline** | Atomic token pop in batches; zero lock contention between workers |

### Throughput math

```
Target   : 20,000,000 users in 60 seconds
Batches  : 20,000,000 / 500 = 40,000 requests
Rate     : 40,000 / 60 ≈ 667 requests/second

Per process (500 workers):
  If avg FCM latency = 150ms → 500 / 0.15 = 3,333 req/s per process
  4 processes → ~13,000 req/s  (well above the 667 req/s needed)
```

## Setup

### 1. Prerequisites

- Python 3.11+
- Redis (local or remote)
- Firebase project with FCM enabled
- Service account JSON with `firebase.messaging` scope

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env with your Firebase credentials and Redis config
```

### 4. Prepare device tokens

Tokens file: one FCM device token per line.

```bash
# Generate 20M test tokens (for load testing only):
python generate_test_tokens.py --count 20000000 --output tokens.txt
```

### 5. Run

```bash
python main.py \
  --tokens tokens.txt \
  --title "Big Sale!" \
  --body "50% off everything today only" \
  --data "screen=sale" "promo_code=SAVE50"
```

#### Options

```
--tokens       Path to device token file (required)
--title        Notification title
--body         Notification body
--image        Notification image URL
--data         Custom key=value data pairs (multiple allowed)
--processes    Number of OS processes (default: 4)
--workers      Async workers per process (default: 500)
--skip-enqueue Skip loading tokens (if already in Redis)
```

### 6. Live output

```
[   2.1s] Sent:    1,234,000  Failed:          0  Rate:  587,619/s  Progress:  6.2%  ETA: 32s
[   4.2s] Sent:    2,468,000  Failed:          0  Rate:  588,095/s  Progress: 12.3%  ETA: 30s
...
============================================================
  FCM Bulk Send Complete
============================================================
  Total tokens   :   20,000,000
  Sent (success) :   19,987,432
  Failed         :       12,568
  Elapsed        :      58.71s
  Avg rate       :    340,442 /s
  Success rate   :       99.94%
============================================================
```

## Handling failures

Failed tokens are saved to Redis key `fcm:failed_tokens`. Retry them with:

```bash
python main.py --tokens /dev/stdin --skip-enqueue \
  <<< ""  # tokens already in Redis
```

Or export them:

```bash
redis-cli lrange fcm:failed_tokens 0 -1 > failed_tokens.txt
python main.py --tokens failed_tokens.txt --skip-enqueue
```

## Scaling beyond 20M

| Scenario | Config |
|----------|--------|
| Single powerful server (32 cores) | `--processes 16 --workers 800` |
| Multi-server (Kubernetes) | Run `worker_process.py` on each pod, share Redis |
| > 100M users | Partition tokens across multiple Redis queues + multiple clusters |

## FCM Quotas

FCM does **not** publish a hard global rate limit for server-side sends, but:
- Stay below **600,000 messages/second** per project to avoid automatic throttling
- Use **exponential backoff** on `429` / `QUOTA_EXCEEDED` responses (built-in, 3 retries)
- Contact Google to raise quotas for very high-volume use cases

## File overview

```
├── main.py                  # Orchestrator: enqueue → spawn → monitor → report
├── worker_process.py        # OS process entry point (asyncio event loop per process)
├── bulk_sender.py           # Async worker pool draining Redis queue
├── fcm_client.py            # FCM HTTP v1 batch API client (aiohttp)
├── token_manager.py         # Token file → Redis queue loader
├── stats.py                 # Shared Redis-based stats + live progress printer
├── config.py                # All configuration (env vars + defaults)
├── generate_test_tokens.py  # Generate fake tokens for load testing
├── requirements.txt
└── .env.example
```
