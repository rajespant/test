import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Firebase
    FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "serviceAccountKey.json")
    FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")

    # Redis (token queue)
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_TOKEN_QUEUE_KEY = "fcm:token_queue"
    REDIS_FAILED_QUEUE_KEY = "fcm:failed_tokens"
    REDIS_STATS_KEY = "fcm:stats"

    # Sending performance
    # FCM multicast: max 500 tokens per request
    BATCH_SIZE = 500
    # Concurrent async workers per process
    CONCURRENT_WORKERS = 500
    # Number of OS processes (set to CPU count for multi-core)
    NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
    # Max retry attempts per batch
    MAX_RETRIES = 3
    # Seconds to wait between retries (exponential backoff base)
    RETRY_BACKOFF_BASE = 1.0

    # Notification payload defaults
    DEFAULT_TITLE = os.getenv("NOTIF_TITLE", "New Notification")
    DEFAULT_BODY = os.getenv("NOTIF_BODY", "You have a new notification")
    DEFAULT_IMAGE = os.getenv("NOTIF_IMAGE", "")

    # Throughput target
    TARGET_USERS = 20_000_000
    TARGET_SECONDS = 60
