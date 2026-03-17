"""Async HTTP client with retry logic and rate limiting for ETL pipelines."""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_RETRIES = 3
_DEFAULT_BACKOFF_BASE = 2.0  # seconds; delay = base ** attempt
_DEFAULT_TIMEOUT = 60.0  # seconds


class RateLimiter:
    """Token-bucket rate limiter for a single pipeline.

    Args:
        requests_per_second: Maximum sustained request rate.
    """

    def __init__(self, requests_per_second: float) -> None:
        self._rate = requests_per_second
        self._min_interval = 1.0 / requests_per_second
        self._last_call: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a request token is available."""
        async with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()


class AsyncHttpClient:
    """Async HTTP client with retry + exponential back-off + rate limiting.

    Args:
        requests_per_second: Sustained rate limit (default: unlimited).
        retries: Number of retry attempts on transient errors (default: 3).
        backoff_base: Seconds for exponential back-off base (default: 2).
        timeout: Per-request timeout in seconds (default: 60).
        headers: Default headers added to every request.
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        retries: int = _DEFAULT_RETRIES,
        backoff_base: float = _DEFAULT_BACKOFF_BASE,
        timeout: float = _DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._retries = retries
        self._backoff_base = backoff_base
        self._limiter = RateLimiter(requests_per_second)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "User-Agent": "ee-opengraph/1.0 (+https://github.com/ee-opengraph)",
                **(headers or {}),
            },
            follow_redirects=True,
        )

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying httpx client."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_url(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> httpx.Response:
        """Fetch a URL with retry logic.

        Args:
            url: Target URL.
            params: Optional query parameters.
            method: HTTP method (default GET).

        Returns:
            httpx.Response with 2xx status.

        Raises:
            httpx.HTTPStatusError: On non-retryable HTTP error.
            httpx.RequestError: If all retries are exhausted.
        """
        last_exc: Exception | None = None
        for attempt in range(self._retries + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.request(method, url, params=params)
                response.raise_for_status()
                logger.debug("GET %s → %s", url, response.status_code)
                return response
            except httpx.HTTPStatusError as exc:
                # 4xx errors are not retryable (except 429 rate-limit)
                if exc.response.status_code != 429 and exc.response.status_code < 500:
                    raise
                last_exc = exc
            except httpx.RequestError as exc:
                last_exc = exc

            if attempt < self._retries:
                delay = self._backoff_base ** attempt
                logger.warning(
                    "Request failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    self._retries,
                    delay,
                    last_exc,
                )
                await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]

    async def download_file(
        self,
        url: str,
        dest: Path,
        chunk_size: int = 65536,
    ) -> Path:
        """Stream a binary file to disk with retry logic.

        Args:
            url: Download URL.
            dest: Destination file path (parent must exist).
            chunk_size: Streaming chunk size in bytes.

        Returns:
            The destination path on success.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        last_exc: Exception | None = None

        for attempt in range(self._retries + 1):
            await self._limiter.acquire()
            try:
                async with self._client.stream("GET", url) as response:
                    response.raise_for_status()
                    with dest.open("wb") as fh:
                        async for chunk in response.aiter_bytes(chunk_size):
                            fh.write(chunk)
                logger.info("Downloaded %s → %s", url, dest)
                return dest
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                if isinstance(exc, httpx.HTTPStatusError):
                    if exc.response.status_code < 500 and exc.response.status_code != 429:
                        raise

            if attempt < self._retries:
                delay = self._backoff_base ** attempt
                logger.warning(
                    "Download failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    self._retries,
                    delay,
                    last_exc,
                )
                await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]
