"""
Mirror-and-compare proxy support.

The legacy metamist server serves every request as normal, and *additionally* mirrors
matching requests to a new server instance so the two responses can be compared. Every
comparison (match and mismatch) is recorded as a JSON object in a GCS bucket, letting us
deploy and observe real traffic to quantify incompatibilities before any cutover.

Everything is bundled on the `Mirror` class; a shared `mirror` singleton is imported by
the server. The feature is gated behind `mirror.enabled` (all three target settings
present) and is designed to never affect the client response:
  - in the default 'old' serve-from mode the new-server call and comparison happen entirely
    in a fire-and-forget background task, so client latency/errors are unaffected;
  - all background work swallows exceptions and uses a finite timeout;
  - the number of concurrent background comparisons is bounded.
"""

import asyncio
import contextlib
import datetime
import json
import uuid
from dataclasses import asdict, dataclass

import httpx
from cloudpathlib import AnyPath
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPAuthorizationCredentials

from cpg_utils.cloud import get_google_identity_token

from api.settings import (
    METAMIST_PROXY_DIFF_BUCKET,
    METAMIST_PROXY_MAX_CONCURRENCY,
    METAMIST_PROXY_SERVE_FROM,
    METAMIST_PROXY_TARGET_AUDIENCE,
    METAMIST_PROXY_TARGET_URL,
    METAMIST_PROXY_TIMEOUT,
)
from api.utils.db import authenticate
from db.python.utils import get_logger


logger = get_logger()


@dataclass
class Comparison:
    """
    All the info needed to mirror one request to the new server and compare responses.

    The request fields are populated up-front; the `old_*` fields are filled once the local
    response is available, and the `new_*` fields once the new server has responded (or
    `new_error` is set if it couldn't be reached).
    """

    method: str
    path: str
    query: str
    req_headers: dict[str, str]
    req_body: bytes
    old_status: int | None = None
    old_body: bytes | None = None
    new_status: int | None = None
    new_body: bytes | None = None
    new_error: str | None = None


@dataclass
class ComparisonRecord:
    """
    The JSON-serialisable record of a single old-vs-new comparison, written to the diff
    bucket. Optional fields are omitted from the written JSON when unset (e.g. the bodies
    are only populated on a mismatch, and `new_error` only on a transport failure).
    """

    timestamp: str
    served_from: str
    method: str
    path: str
    query: str
    old_status: int | None
    new_status: int | None
    matched: bool
    status_match: bool | None = None
    body_match: bool | None = None
    body_type: str | None = None
    new_error: str | None = None
    old_body: object = None
    new_body: object = None


class Mirror:
    """Bundles all mirror-and-compare behaviour behind a single importable object."""

    # Request headers forwarded to the new server.
    HEADERS_TO_PASS = (
        'sm-ar-guid',
        'sm-extra-values',
        'sm-on-behalf-of',
        'content-type',
        'accept',
    )

    # Path suffixes to never mirror: replaying them triggers real external side effects
    # (the seqr sync endpoint posts to seqr, writes a GCS map file and sends Slack
    # notifications), so it is important they don't run twice.
    DENYLIST_SUFFIXES = ('/sync-dataset',)

    def __init__(self) -> None:
        # Feature is a complete no-op unless all three targets are configured.
        self.enabled = bool(
            METAMIST_PROXY_TARGET_URL
            and METAMIST_PROXY_TARGET_AUDIENCE
            and METAMIST_PROXY_DIFF_BUCKET
        )
        self.serve_from_new = METAMIST_PROXY_SERVE_FROM == 'new'
        # Shared async HTTP client, opened/closed via the app lifespan so it lives on the
        # same event loop as the app. Strong references to background tasks are retained in
        # _bg_tasks so they aren't garbage collected mid-run.
        self._client: httpx.AsyncClient | None = None
        self._bg_tasks: set[asyncio.Task[None]] = set()

    # -- lifecycle -----------------------------------------------------------------

    async def open(self) -> None:
        """Open the shared httpx client (call from app lifespan startup)."""
        if self.enabled and self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(METAMIST_PROXY_TIMEOUT),
                limits=httpx.Limits(max_connections=METAMIST_PROXY_MAX_CONCURRENCY),
            )

    async def close(self) -> None:
        """Close the shared httpx client (call from app lifespan shutdown)."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- routing helpers -----------------------------------------------------------

    @staticmethod
    def is_api_path(path: str) -> bool:
        """Whether `path` targets the JSON API or GraphQL endpoints (vs frontend/static)."""
        stripped = path.lstrip('/')
        return stripped in {'graphql', 'api/v1'} or stripped.startswith(
            ('graphql/', 'api/v1/')
        )

    def should_mirror(self, path: str) -> bool:
        """
        Whether a request to `path` should be mirrored to the new server.

        Mirrors the api and graphql requests, while avoiding any paths in the denylist.
        """
        if not self.is_api_path(path):
            return False
        return not path.endswith(self.DENYLIST_SUFFIXES)

    @staticmethod
    def target_url(path: str, query: str) -> str:
        """Build the absolute URL of `path` (+ query) on the new server."""
        url = f'{METAMIST_PROXY_TARGET_URL.rstrip("/")}/{path.lstrip("/")}'
        if query:
            url += f'?{query}'
        return url

    # -- calling the new server ----------------------------------------------------

    @staticmethod
    def _resolve_author(req_headers: dict[str, str]) -> str | None:
        """
        Resolve the caller's identity the same way the normal request path does, so we can
        forward it to the new server as `sm-legacy-proxy-author`. Returns None if the
        request isn't authenticated (the new server then authenticates the forwarded token
        itself).

        NB: this is a sync function (it may verify an IAP JWT over the network) and should
        be called via run_in_threadpool.
        """
        token = None
        auth_header = req_headers.get('authorization')
        if auth_header and auth_header.lower().startswith('bearer '):
            token = HTTPAuthorizationCredentials(
                scheme='Bearer', credentials=auth_header[len('bearer ') :]
            )
        iap_jwt = req_headers.get('x-goog-iap-jwt-assertion')
        try:
            return authenticate(token=token, x_goog_iap_jwt_assertion=iap_jwt)
        except Exception as e:  # noqa: BLE001 - never let auth failures affect mirroring
            logger.debug(f'Could not resolve author for mirror: {e}')
            return None

    async def call_new_server(self, comparison: Comparison) -> httpx.Response | None:
        """
        Forward a request to the new server and return its (fully-read) response, or None
        on a transport-level error (timeout, connection failure, ...).
        """
        if self._client is None:
            return None

        url = self.target_url(comparison.path, comparison.query)

        headers: dict[str, str] = {}
        for header in self.HEADERS_TO_PASS:
            value = comparison.req_headers.get(header)
            if value:
                headers[header] = value

        author = await run_in_threadpool(self._resolve_author, comparison.req_headers)
        if author:
            headers['sm-legacy-proxy-author'] = author

        try:
            token = await run_in_threadpool(
                get_google_identity_token,
                target_audience=METAMIST_PROXY_TARGET_AUDIENCE,
            )
            if token:
                headers['Authorization'] = f'Bearer {token}'
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f'Could not mint identity token for mirror {comparison.method} {url}: {e}'
            )

        try:
            return await self._client.request(
                method=comparison.method,
                url=url,
                headers=headers,
                content=comparison.req_body,
            )
        except Exception as e:  # noqa: BLE001 - transport error, recorded as a diff
            logger.warning(f'Mirror request failed for {comparison.method} {url}: {e}')
            return None

    # -- comparison + recording ----------------------------------------------------

    @staticmethod
    def _normalise(obj):
        """Recursively sort dict keys (leave list order intact) for order-insensitive diffing."""
        if isinstance(obj, dict):
            return {k: Mirror._normalise(obj[k]) for k in sorted(obj)}
        if isinstance(obj, list):
            return [Mirror._normalise(x) for x in obj]
        return obj

    @staticmethod
    def _try_json(body: bytes | None):
        if body is None:
            return None, True
        try:
            return json.loads(body), True
        except (json.JSONDecodeError, TypeError, ValueError):
            return None, False

    def _compare(self, comparison: Comparison) -> ComparisonRecord:
        """Build a comparison record for old vs new server responses."""
        record = ComparisonRecord(
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            served_from=METAMIST_PROXY_SERVE_FROM,
            method=comparison.method,
            path=comparison.path,
            query=comparison.query,
            old_status=comparison.old_status,
            new_status=comparison.new_status,
            matched=False,
        )

        if comparison.new_error is not None:
            record.new_error = comparison.new_error
            return record

        record.status_match = comparison.old_status == comparison.new_status

        old_json, old_ok = self._try_json(comparison.old_body)
        new_json, new_ok = self._try_json(comparison.new_body)

        if old_ok and new_ok:
            record.body_type = 'json'
            record.body_match = self._normalise(old_json) == self._normalise(new_json)
            if not record.body_match:
                record.old_body = old_json
                record.new_body = new_json
        else:
            record.body_type = 'raw'
            record.body_match = comparison.old_body == comparison.new_body
            if not record.body_match:
                record.old_body = (comparison.old_body or b'').decode(
                    'utf-8', errors='replace'
                )
                record.new_body = (comparison.new_body or b'').decode(
                    'utf-8', errors='replace'
                )

        record.matched = bool(record.status_match and record.body_match)
        return record

    @staticmethod
    def _write_record(record: ComparisonRecord) -> None:
        """Write a comparison record as JSON to the diff bucket (blocking; use threadpool)."""
        now = datetime.datetime.now(datetime.timezone.utc)
        key = f'{now:%Y/%m/%d}/{int(now.timestamp() * 1000)}-{uuid.uuid4().hex}.json'
        assert METAMIST_PROXY_DIFF_BUCKET
        path = AnyPath(f'{METAMIST_PROXY_DIFF_BUCKET.rstrip("/")}/{key}')
        # Omit unset optional fields (bodies on a match, new_error on success, ...).
        payload = {k: v for k, v in asdict(record).items() if v is not None}
        with contextlib.suppress(Exception):
            path.parent.mkdir(parents=True, exist_ok=True)  # no-op-ish for cloud paths
        with path.open('w') as f:  # type: ignore[union-attr]
            json.dump(payload, f)

    async def _run_comparison(
        self, comparison: Comparison, new_prefetched: bool
    ) -> None:
        """Background coroutine: (optionally) call the new server, compare, and record."""
        try:
            if not new_prefetched:
                resp = await self.call_new_server(comparison)
                if resp is None:
                    comparison.new_error = 'transport_error'
                else:
                    comparison.new_status = resp.status_code
                    comparison.new_body = resp.content

            record = self._compare(comparison)
            await run_in_threadpool(self._write_record, record)
        except Exception as e:  # noqa: BLE001 - the mirror must never affect the client
            logger.warning(
                f'Mirror comparison failed for {comparison.method} {comparison.path}: {e}'
            )

    def schedule_comparison(
        self, comparison: Comparison, new_prefetched: bool = False
    ) -> None:
        """
        Schedule a fire-and-forget background comparison of old vs new responses.

        In serve-from-old mode the new response is fetched inside the background task
        (`new_prefetched=False`). In serve-from-new mode the new response was already
        fetched on the request path and is set on `comparison` (`new_prefetched=True`).
        """
        if not self.enabled:
            return
        if len(self._bg_tasks) >= METAMIST_PROXY_MAX_CONCURRENCY:
            logger.warning('Mirror comparison dropped: too many in-flight comparisons')
            return

        task = asyncio.create_task(self._run_comparison(comparison, new_prefetched))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)


# Shared singleton imported by the server.
mirror = Mirror()
