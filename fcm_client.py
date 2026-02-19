"""
FCM HTTP v1 Client
Uses Google's FCM HTTP v1 API directly via aiohttp for maximum async performance.
Sends multicast messages (up to 500 tokens per request) using the batch endpoint.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account

from config import Config

logger = logging.getLogger(__name__)

FCM_BATCH_URL = "https://fcm.googleapis.com/batch"
FCM_SEND_URL = "https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
FCM_SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]


@dataclass
class NotificationPayload:
    title: str = Config.DEFAULT_TITLE
    body: str = Config.DEFAULT_BODY
    image: str = Config.DEFAULT_IMAGE
    data: dict = field(default_factory=dict)


@dataclass
class BatchResult:
    success_count: int = 0
    failure_count: int = 0
    failed_tokens: list = field(default_factory=list)
    duration_ms: float = 0.0


class FCMClient:
    """
    Async FCM client that:
    1. Obtains OAuth2 access token from service account
    2. Sends batches of up to 500 tokens per multicast request
    3. Refreshes access token before expiry
    """

    def __init__(self):
        self._credentials: Optional[service_account.Credentials] = None
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
        self._project_id = Config.FIREBASE_PROJECT_ID
        self._send_url = FCM_SEND_URL.format(project_id=self._project_id)

    def _load_credentials(self):
        self._credentials = service_account.Credentials.from_service_account_file(
            Config.FIREBASE_CREDENTIALS_PATH,
            scopes=FCM_SCOPES,
        )
        if not self._project_id:
            with open(Config.FIREBASE_CREDENTIALS_PATH) as f:
                sa = json.load(f)
            self._project_id = sa.get("project_id", "")
            self._send_url = FCM_SEND_URL.format(project_id=self._project_id)

    def _refresh_token(self):
        """Synchronously refresh the OAuth2 token (called rarely)."""
        if self._credentials is None:
            self._load_credentials()
        request = google.auth.transport.requests.Request()
        self._credentials.refresh(request)
        self._access_token = self._credentials.token
        # Expire 5 minutes early to avoid using a stale token
        self._token_expiry = time.time() + (self._credentials.expiry.timestamp() - time.time() - 300)
        logger.debug("Access token refreshed, expires in %.0fs", self._token_expiry - time.time())

    def _get_token(self) -> str:
        if self._access_token is None or time.time() >= self._token_expiry:
            self._refresh_token()
        return self._access_token

    async def start(self):
        """Create shared aiohttp session with connection pooling."""
        connector = aiohttp.TCPConnector(
            limit=Config.CONCURRENT_WORKERS + 50,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=5)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )
        # Pre-fetch token
        self._refresh_token()

    async def close(self):
        if self._session:
            await self._session.close()

    async def send_multicast(
        self,
        tokens: list[str],
        notification: NotificationPayload,
    ) -> BatchResult:
        """
        Send a notification to up to 500 tokens using FCM v1 batch endpoint.
        Returns a BatchResult with counts and any failed tokens.
        """
        if not tokens:
            return BatchResult()

        start = time.perf_counter()
        result = BatchResult()

        # Build multipart batch body
        boundary = "batch_fcm_boundary"
        parts = []
        for i, token in enumerate(tokens):
            message = {
                "message": {
                    "token": token,
                    "notification": {
                        "title": notification.title,
                        "body": notification.body,
                    },
                    **({"data": notification.data} if notification.data else {}),
                }
            }
            if notification.image:
                message["message"]["notification"]["image"] = notification.image

            part = (
                f"--{boundary}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"\r\n"
                + json.dumps(message)
                + "\r\n"
            )
            parts.append(part)

        body = "".join(parts) + f"--{boundary}--"

        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": f"multipart/mixed; boundary={boundary}",
        }

        try:
            async with self._session.post(
                FCM_BATCH_URL,
                data=body.encode(),
                headers=headers,
            ) as resp:
                resp_text = await resp.text()
                result = self._parse_batch_response(resp_text, tokens)
        except aiohttp.ClientError as e:
            logger.warning("Batch request failed: %s", e)
            result.failure_count = len(tokens)
            result.failed_tokens = tokens[:]

        result.duration_ms = (time.perf_counter() - start) * 1000
        return result

    def _parse_batch_response(self, response_text: str, tokens: list[str]) -> BatchResult:
        """Parse the multipart batch response from FCM."""
        result = BatchResult()
        lines = response_text.split("\n")
        token_idx = 0

        for i, line in enumerate(lines):
            line = line.strip()
            # Each part contains a JSON response
            if line.startswith("{") and '"name"' in line:
                # Success
                result.success_count += 1
                token_idx += 1
            elif line.startswith("{") and '"error"' in line:
                # Failure - try to extract token
                result.failure_count += 1
                if token_idx < len(tokens):
                    result.failed_tokens.append(tokens[token_idx])
                token_idx += 1

        # Fallback: if parsing didn't work well, count by splitting on boundary
        if result.success_count + result.failure_count == 0:
            # Rough estimate: count JSON objects
            import re
            successes = len(re.findall(r'"name"\s*:', response_text))
            failures = len(re.findall(r'"error"\s*:', response_text))
            result.success_count = successes
            result.failure_count = failures
            result.failed_tokens = tokens[failures:] if failures else []

        return result
