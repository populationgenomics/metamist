"""
Mirror-and-capture proxy support.

The legacy metamist server serves every request as normal, and *additionally* mirrors
matching requests to a new server instance and CAPTURES both responses (plus the request)
as a JSON object in a GCS bucket. A separate offline script
(``scripts/compare_mirror_diffs.py``) reads the bucket and performs the in-depth
comparison, so the server itself does no diffing.

Everything is bundled on the `Mirror` class; a shared `mirror` singleton is imported by the
server. The feature is gated behind `mirror.enabled` (all three target settings present) and
is designed to never affect the client response:
  - in the default 'old' serve-from mode the new-server call and capture happen entirely in
    a fire-and-forget background task, so client latency/errors are unaffected;
  - all background work swallows exceptions and uses a finite timeout;
  - the number of concurrent background captures is bounded.
"""

import asyncio
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


def _decode(body: bytes | None) -> str:
    """Decode a response/request body to text for recording (lossy for binary)."""
    return (body or b'').decode('utf-8', errors='replace')


@dataclass
class MirrorContext:
    """
    All the info needed to mirror one request to the new server and capture both responses.

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
class MirrorRecord:
    """
    The JSON-serialisable capture of a single mirrored request written to the diff bucket:
    the request plus the old and new responses as raw text. The in-depth comparison is done
    offline by ``scripts/compare_mirror_diffs.py``. Optional fields are omitted from the
    written JSON when unset (e.g. `new_body` when the new server couldn't be reached).
    """

    timestamp: str
    served_from: str
    method: str
    path: str
    query: str
    request_body: str
    old_status: int | None
    new_status: int | None
    old_body: str | None = None
    new_body: str | None = None
    new_error: str | None = None


class Mirror:
    """Bundles all mirror-and-capture behaviour behind a single importable object."""

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
        if not self.enabled:
            missing = [
                name
                for name, value in (
                    ('METAMIST_PROXY_TARGET_URL', METAMIST_PROXY_TARGET_URL),
                    ('METAMIST_PROXY_TARGET_AUDIENCE', METAMIST_PROXY_TARGET_AUDIENCE),
                    ('METAMIST_PROXY_DIFF_BUCKET', METAMIST_PROXY_DIFF_BUCKET),
                )
                if not value
            ]
            logger.warning(
                f'mirror: DISABLED - not writing to bucket. Missing settings: {missing}'
            )
            return

        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(METAMIST_PROXY_TIMEOUT),
                limits=httpx.Limits(max_connections=METAMIST_PROXY_MAX_CONCURRENCY),
            )
        logger.info(
            'mirror: ENABLED '
            f'(serve_from={METAMIST_PROXY_SERVE_FROM}, '
            f'target_url={METAMIST_PROXY_TARGET_URL}, '
            f'diff_bucket={METAMIST_PROXY_DIFF_BUCKET}, '
            f'timeout={METAMIST_PROXY_TIMEOUT}s, '
            f'max_concurrency={METAMIST_PROXY_MAX_CONCURRENCY})'
        )

    async def close(self) -> None:
        """Close the shared httpx client (call from app lifespan shutdown)."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- routing helpers -----------------------------------------------------------

    @staticmethod
    def is_api_request(method: str, path: str) -> bool:
        """
        Whether `(method, path)` is a real API/GraphQL operation to mirror, as opposed to
        the GraphiQL UI or the frontend/static assets.

        GraphQL only counts as an API request on POST - `GET /graphql` serves the GraphiQL
        UI and should be treated like a frontend request (redirected, not mirrored).
        """
        stripped = path.lstrip('/')
        if stripped == 'api/v1' or stripped.startswith('api/v1/'):
            return True
        if stripped == 'graphql' or stripped.startswith('graphql/'):
            return method.upper() == 'POST'
        return False

    def should_mirror(self, method: str, path: str) -> bool:
        """Whether a request should be mirrored: a real API request not in the denylist."""
        if not self.is_api_request(method, path):
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

    async def call_new_server(self, ctx: MirrorContext) -> httpx.Response | None:
        """
        Forward a request to the new server and return its (fully-read) response, or None
        on a transport-level error (timeout, connection failure, ...).
        """
        if self._client is None:
            logger.warning(
                'mirror: cannot call new server - http client is not open '
                f'(enabled={self.enabled})'
            )
            return None

        url = self.target_url(ctx.path, ctx.query)

        headers: dict[str, str] = {}
        for header in self.HEADERS_TO_PASS:
            value = ctx.req_headers.get(header)
            if value:
                headers[header] = value

        author = await run_in_threadpool(self._resolve_author, ctx.req_headers)
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
                f'Could not mint identity token for mirror {ctx.method} {url}: {e}'
            )

        try:
            resp = await self._client.request(
                method=ctx.method,
                url=url,
                headers=headers,
                content=ctx.req_body,
            )
            logger.info(
                f'mirror: new server responded {resp.status_code} for {ctx.method} {url}'
            )
            return resp
        except Exception as e:  # noqa: BLE001 - transport error, captured as new_error
            logger.warning(f'Mirror request failed for {ctx.method} {url}: {e}')
            return None

    # -- capture + recording -------------------------------------------------------

    def _build_record(self, ctx: MirrorContext) -> MirrorRecord:
        """Build the raw capture record for a mirrored request (no comparison)."""
        return MirrorRecord(
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            served_from=METAMIST_PROXY_SERVE_FROM,
            method=ctx.method,
            path=ctx.path,
            query=ctx.query,
            request_body=_decode(ctx.req_body),
            old_status=ctx.old_status,
            new_status=ctx.new_status,
            old_body=_decode(ctx.old_body),
            new_body=None if ctx.new_error else _decode(ctx.new_body),
            new_error=ctx.new_error,
        )

    @staticmethod
    def _write_record(record: MirrorRecord) -> None:
        """Write a capture record as JSON to the diff bucket (blocking; use threadpool)."""
        now = datetime.datetime.now(datetime.timezone.utc)
        key = f'{now:%Y/%m/%d}/{int(now.timestamp() * 1000)}-{uuid.uuid4().hex}.json'
        assert METAMIST_PROXY_DIFF_BUCKET
        path = AnyPath(f'{METAMIST_PROXY_DIFF_BUCKET.rstrip("/")}/{key}')
        # Omit unset optional fields (e.g. new_body/new_error).
        payload = {k: v for k, v in asdict(record).items() if v is not None}
        try:
            path.parent.mkdir(parents=True, exist_ok=True)  # no-op-ish for cloud paths
        except Exception as e:  # noqa: BLE001 - mkdir isn't needed for cloud paths
            logger.debug(f'mirror: could not mkdir {path.parent}: {e}')
        with path.open('w') as f:  # type: ignore[union-attr]
            json.dump(payload, f)
        logger.info(f'mirror: wrote capture record to {path}')

    async def _capture(self, ctx: MirrorContext, new_prefetched: bool) -> None:
        """Background coroutine: (optionally) call the new server, then record the capture."""
        try:
            if not new_prefetched:
                resp = await self.call_new_server(ctx)
                if resp is None:
                    ctx.new_error = 'transport_error'
                else:
                    ctx.new_status = resp.status_code
                    ctx.new_body = resp.content

            record = self._build_record(ctx)
            logger.info(
                f'mirror: captured {ctx.method} {ctx.path} '
                f'(old_status={ctx.old_status}, new_status={ctx.new_status}, '
                f'new_error={ctx.new_error})'
            )
            await run_in_threadpool(self._write_record, record)
        except Exception:
            # Full traceback so failures writing to the bucket (auth, permissions, missing
            # dependency, ...) are visible rather than silently swallowed.
            logger.exception(f'mirror: capture failed for {ctx.method} {ctx.path}')

    def schedule_capture(self, ctx: MirrorContext, new_prefetched: bool = False) -> None:
        """
        Schedule a fire-and-forget background capture of the old + new responses.

        In serve-from-old mode the new response is fetched inside the background task
        (`new_prefetched=False`). In serve-from-new mode the new response was already
        fetched on the request path and is set on `ctx` (`new_prefetched=True`).
        """
        if not self.enabled:
            logger.debug('mirror: schedule_capture skipped - feature disabled')
            return
        if len(self._bg_tasks) >= METAMIST_PROXY_MAX_CONCURRENCY:
            logger.warning('Mirror capture dropped: too many in-flight captures')
            return

        task = asyncio.create_task(self._capture(ctx, new_prefetched))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        logger.info(
            f'mirror: scheduled capture for {ctx.method} {ctx.path} '
            f'(new_prefetched={new_prefetched}, in_flight={len(self._bg_tasks)})'
        )


# Shared singleton imported by the server.
mirror = Mirror()
