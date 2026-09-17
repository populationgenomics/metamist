"""
Helpers to allow for mirroring requests from this server to the new postgres server
"""

import asyncio
import datetime
import json
import uuid
from dataclasses import asdict, dataclass

import httpx
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPAuthorizationCredentials
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
from httpx import Response as HttpxResponse
from starlette.responses import JSONResponse, Response

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
    """Decode a response or request body to plain text for recording to the diff bucket."""
    return (body or b'').decode('utf-8', errors='replace')


@dataclass
class MirrorContext:
    """
    All the info needed to mirror one request to the new server and capture both responses.

    This is initially constructed with just the request fields, old fields are then
    populated once the response from this server is available. New fields are populated
    once the new server has responded
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
    The data that is eventually saved as JSON to the mirror diff bucket
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
    """Class to handle all mirroring functionality"""

    # Request headers forwarded to the new server, we don't need to pass everything and
    # particularly not auth headers as those need to be replaced. These ones are
    # the important ones
    REQUEST_HEADERS_TO_PASS = (
        'sm-ar-guid',
        'sm-extra-values',
        'sm-on-behalf-of',
        'content-type',
        'accept',
    )

    # list of headers to pass on from new server when proxying. We don't set many
    # headers in metamist so the list is pretty short.
    RESPONSE_HEADERS_TO_PASS = (
        'content-type',
        'content-disposition',  # file/export downloads
        'x-bq-cost',  # billing cost header
        'x-process-time',  # request timing added by the new server's middleware
    )

    # Avoid mirroring these paths as they have duplicated side effects.
    # (the seqr sync endpoint posts to seqr, writes a GCS map file and sends Slack
    # notifications), so it is important they don't run twice.
    DENYLIST_SUFFIXES = ('/sync-dataset',)

    def __init__(self) -> None:
        self.enabled = bool(
            METAMIST_PROXY_TARGET_URL
            and METAMIST_PROXY_TARGET_AUDIENCE
            and METAMIST_PROXY_DIFF_BUCKET
        )
        self.serve_from_new = METAMIST_PROXY_SERVE_FROM == 'new'
        # Shared async HTTP client which is opened and closed via the app lifespan so it
        # lives on the same event loop as the app.
        self._client: httpx.AsyncClient | None = None
        # Strong references to background tasks are retained in _bg_tasks so they won't
        # be garbage collected mid-run.
        self._bg_tasks: set[asyncio.Task[None]] = set()
        # Shared GCS client to limit overhead of writes
        self._storage_client: storage.Client | None = None
        self._bucket: storage.Bucket | None = None

    async def open(self) -> None:
        """Open the shared httpx and GCS clients"""
        if not self.enabled:
            return

        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(METAMIST_PROXY_TIMEOUT),
                limits=httpx.Limits(max_connections=METAMIST_PROXY_MAX_CONCURRENCY),
            )

        if self._bucket is None:
            # open won't be called unless this is set, see self.enabled
            assert METAMIST_PROXY_DIFF_BUCKET
            self._storage_client = storage.Client()
            self._bucket = self._storage_client.bucket(METAMIST_PROXY_DIFF_BUCKET)

    async def close(self) -> None:
        """Close the shared httpx and GCS clients"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._storage_client is not None:
            self._storage_client.close()
            self._storage_client = None
            self._bucket = None

    @staticmethod
    def is_api_request(method: str, path: str) -> bool:
        """
        Determine whether the request is to an API endpoint. This is used to decide
        whether a request should be mirrored. Generally we only want to mirror API
        requests.
        """
        stripped = path.lstrip('/')
        if stripped == 'api/v1' or stripped.startswith('api/v1/'):
            return True
        if stripped == 'graphql' or stripped.startswith('graphql/'):
            return method.upper() == 'POST'
        return False

    def should_mirror(self, method: str, path: str) -> bool:
        """
        Whether a request should be mirrored: a real API request not in the denylist.
        This is only used when serving from old. In serve from new everything is either
        mirrored or redirected.
        """
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

    @staticmethod
    def _resolve_author(req_headers: dict[str, str]) -> str | None:
        """
        Get the author via authentication in the same way that routes in the app do.
        This needs to be handled kinda separately as we don't have access to FastAPI's
        dependency injection in middleware.
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

    async def call_new_server(
        self, ctx: MirrorContext, author: str | None
    ) -> httpx.Response | None:
        """
        Forward a request to the new server and return its response, any http errors
        from the new server will be captured in the response and won't raise an exception
        """
        if self._client is None:
            return None

        url = self.target_url(ctx.path, ctx.query)

        headers: dict[str, str] = {}
        for header in self.REQUEST_HEADERS_TO_PASS:
            value = ctx.req_headers.get(header)
            if value:
                headers[header] = value

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
            return resp
        # This will only be transport level errors, not HTTP errors
        except Exception as e:  # noqa: BLE001 - transport error, captured as new_error
            logger.warning(f'Mirror request failed for {ctx.method} {url}: {e}')
            return None

    async def proxy_to_new(self, ctx: MirrorContext) -> Response:
        """
        Forward a request straight to the new server and return its response as is.

        This is used in serve_from_new mode
        """
        author = await run_in_threadpool(self._resolve_author, ctx.req_headers)
        if not author:
            # When making requests to the new server, we must know the author of the
            # request to this server. This is because requests to the new server are
            # made by this server's service account, so we must be able to send an
            # additional header with the real author so the new server can remove the
            # proxy sa as the author and replace it with the real one before doing
            # any authorization
            logger.warning(
                f'mirror: could not authenticate {ctx.method} {ctx.path}, returning 401'
            )
            return JSONResponse(
                status_code=401,
                content={'name': 'Unauthorized', 'description': 'Not authenticated :('},
            )

        resp = await self.call_new_server(ctx, author)
        if resp is None:
            logger.warning(
                f'mirror: new server unreachable for {ctx.method} {ctx.path}, returning 502'
            )
            return JSONResponse(
                status_code=502,
                content={
                    'name': 'BadGateway',
                    'description': 'The upstream metamist server could not be reached.',
                },
            )

        headers = {
            k: v
            for k, v in resp.headers.items()
            if k.lower() in self.RESPONSE_HEADERS_TO_PASS
        }
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=headers,
        )

    # -- capture + recording -------------------------------------------------------

    def _build_record(self, ctx: MirrorContext) -> MirrorRecord:
        """Build the raw capture record for a mirrored request"""
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

    def _write_record(self, record: MirrorRecord) -> None:
        """Write a capture record as JSON to the diff bucket (blocking; use threadpool)."""
        assert self._bucket is not None
        now = datetime.datetime.now(datetime.timezone.utc)
        key = f'{now:%Y/%m/%d}/{int(now.timestamp() * 1000)}-{uuid.uuid4().hex}.json'
        # Omit unset optional fields (e.g. new_body/new_error).
        payload = {k: v for k, v in asdict(record).items() if v is not None}
        # Pass retry explicitly: the default only retries uploads when an
        # `if_generation_match` is given, so without this a stale pooled connection on a
        # long-lived client would drop the record. Safe to retry as the blob key is unique.
        self._bucket.blob(key).upload_from_string(
            json.dumps(payload),
            content_type='application/json',
            retry=DEFAULT_RETRY,
        )

    async def _capture(self, ctx: MirrorContext) -> None:
        """Run in the background, call new server and then write response record."""
        try:
            author = await run_in_threadpool(self._resolve_author, ctx.req_headers)
            resp: HttpxResponse | None = None

            if author is not None:
                resp = await self.call_new_server(ctx, author)

            if resp is None:
                error = 'missing_author' if author is None else 'transport_error'
                ctx.new_error = error
            else:
                ctx.new_status = resp.status_code
                ctx.new_body = resp.content

            record = self._build_record(ctx)
            await run_in_threadpool(self._write_record, record)
        except Exception:
            # Full traceback so failures writing to the bucket (auth, permissions, missing
            # dependency, ...) are visible rather than silently swallowed.
            logger.exception(f'mirror: capture failed for {ctx.method} {ctx.path}')

    def schedule_capture(self, ctx: MirrorContext) -> None:
        """
        Schedule a background capture of the old + new responses.

        The new response is fetched inside the background task so its latency and errors
        never affect the client. This is only used in serve-from-old mode.
        """
        if not self.enabled:
            logger.debug('mirror: schedule_capture skipped - feature disabled')
            return
        if len(self._bg_tasks) >= METAMIST_PROXY_MAX_CONCURRENCY:
            logger.warning('Mirror capture dropped: too many in-flight captures')
            return

        task = asyncio.create_task(self._capture(ctx))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)


# Shared singleton imported by the server.
mirror = Mirror()
