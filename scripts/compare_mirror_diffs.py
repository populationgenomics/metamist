#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["click", "cloudpathlib[gs]", "deepdiff"]
# ///
r"""
Compare the old-vs-new response captures written by the metamist mirror proxy.

The mirror proxy (api/utils/mirror.py) captures every mirrored request to a GCS bucket as a
JSON object containing the request plus the old and new server responses (as raw text). It
does NOT compare them - this script does, offline, so it never adds load to the server.

For each captured record it parses the old/new bodies (as JSON where possible) and reports
the differences using DeepDiff, then prints a summary.

Captures are stored under YYYY/MM/DD/ day-prefixes, so comparison is scoped by date range
(defaulting to the last 7 days).

Run it directly with uv (dependencies are resolved from the inline metadata above):

    # last 7 days (default)
    uv run scripts/compare_mirror_diffs.py --bucket gs://my-diff-bucket

    # an explicit date range (inclusive)
    uv run scripts/compare_mirror_diffs.py --bucket gs://my-diff-bucket \\
        --start 2026-09-01 --end 2026-09-03

    # the last 14 days
    uv run scripts/compare_mirror_diffs.py --bucket gs://my-diff-bucket --days 14

    # or against a local directory of captures (e.g. for testing)
    uv run scripts/compare_mirror_diffs.py --bucket /tmp/mirror-captures --days 30
"""

import datetime
import json
import os
import sys
from dataclasses import dataclass

import click
from cloudpathlib import AnyPath
from deepdiff import DeepDiff


@dataclass
class Result:
    """The outcome of comparing a single captured record."""

    blob: str
    method: str
    path: str
    request_body: str
    old_status: int | None
    new_status: int | None
    new_error: str | None
    status_match: bool
    body_match: bool
    diff_pretty: str

    @property
    def matched(self) -> bool:
        """Whether the old and new responses matched (status + body, no transport error)."""
        return self.new_error is None and self.status_match and self.body_match


def _parse_body(text: str | None):
    """Parse a captured body as JSON, falling back to (raw_text, is_json=False)."""
    if text is None:
        return None, True
    try:
        return json.loads(text), True
    except (json.JSONDecodeError, TypeError, ValueError):
        return text, False


def _compare_record(
    blob: str,
    record: dict,
    *,
    ignore_order: bool,
    exclude_regex: tuple[str, ...],
) -> Result:
    """Compare the old vs new response captured in a single record."""
    new_error = record.get('new_error')
    old_status = record.get('old_status')
    new_status = record.get('new_status')
    status_match = old_status == new_status

    diff_pretty = ''
    body_match = True

    if new_error is not None:
        body_match = False
        diff_pretty = f'new server error: {new_error}'
    else:
        old_obj, old_is_json = _parse_body(record.get('old_body'))
        new_obj, new_is_json = _parse_body(record.get('new_body'))

        if old_is_json and new_is_json:
            diff = DeepDiff(
                old_obj,
                new_obj,
                ignore_order=ignore_order,
                exclude_regex_paths=list(exclude_regex) or None,
            )
            body_match = not diff
            diff_pretty = diff.pretty() if diff else ''
        else:
            body_match = old_obj == new_obj
            if not body_match:
                diff_pretty = 'non-JSON body differs (raw text mismatch)'

    return Result(
        blob=blob,
        method=record.get('method', '?'),
        path=record.get('path', '?'),
        request_body=record.get('request_body', ''),
        old_status=old_status,
        new_status=new_status,
        new_error=new_error,
        status_match=status_match,
        body_match=body_match,
        diff_pretty=diff_pretty,
    )


def _day_prefixes(start: datetime.date, end: datetime.date) -> list[str]:
    """The `YYYY/MM/DD` capture prefixes for every day in [start, end] inclusive."""
    return [
        (start + datetime.timedelta(days=i)).strftime('%Y/%m/%d')
        for i in range((end - start).days + 1)
    ]


def _iter_records(roots):
    """Yield (blob_name, parsed_json) for every *.json capture under each root path."""
    for root in roots:
        try:
            blobs = sorted(root.rglob('*.json'), key=str)
        except FileNotFoundError:
            continue  # a day with no captures (missing local dir)
        for blob in blobs:
            try:
                yield str(blob), json.loads(blob.read_text())
            except Exception as e:  # noqa: BLE001 - skip unreadable records, keep going
                click.echo(f'WARNING: could not read {blob}: {e}', err=True)


def _format_result(result: Result) -> str:
    """Human-readable block for a single record."""
    lines = [
        f'{"MATCH" if result.matched else "MISMATCH"}: '
        f'{result.method} {result.path}',
        f'  record: {result.blob}',
        f'  status: old={result.old_status} new={result.new_status}'
        + ('' if result.status_match else '  <-- STATUS DIFFERS'),
    ]
    if result.request_body:
        lines.append(f'  request_body: {result.request_body}')
    if result.diff_pretty:
        indented = '\n'.join(f'    {line}' for line in result.diff_pretty.splitlines())
        lines.append('  body diff:')
        lines.append(indented)
    return '\n'.join(lines)


@click.command()
@click.option(
    '--bucket',
    default=lambda: os.getenv('METAMIST_PROXY_DIFF_BUCKET'),
    help='Bucket/dir holding captures (default: $METAMIST_PROXY_DIFF_BUCKET). '
    'e.g. gs://my-bucket or a local path.',
)
@click.option(
    '--start',
    type=click.DateTime(formats=['%Y-%m-%d']),
    default=None,
    help='Start date (inclusive, UTC), YYYY-MM-DD.',
)
@click.option(
    '--end',
    type=click.DateTime(formats=['%Y-%m-%d']),
    default=None,
    help='End date (inclusive, UTC), YYYY-MM-DD. Default: today.',
)
@click.option(
    '--days',
    type=int,
    default=None,
    help='Number of days ending at --end (inclusive), instead of --start. Default: 7.',
)
@click.option(
    '--show-matches/--mismatches-only',
    default=False,
    help='Print matching records too (default: only mismatches).',
)
@click.option('--limit', type=int, default=0, help='Max records to process (0 = all).')
@click.option(
    '--ignore-order',
    is_flag=True,
    default=False,
    help='Ignore list ordering when diffing (off by default; GraphQL lists are ordered).',
)
@click.option(
    '--exclude-regex',
    multiple=True,
    help='DeepDiff exclude_regex_paths pattern(s) to ignore, e.g. noisy error/extensions '
    "fields. Repeatable, e.g. --exclude-regex \"root\\['errors'\\].*\".",
)
@click.option(
    '--output',
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help='Write the report to this file instead of stdout.',
)
def main(
    bucket: str | None,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    days: int | None,
    show_matches: bool,
    limit: int,
    ignore_order: bool,
    exclude_regex: tuple[str, ...],
    output: str | None,
):
    """Compare old-vs-new response captures from the mirror proxy and report differences."""
    if not bucket:
        raise click.UsageError(
            'No bucket given: pass --bucket or set METAMIST_PROXY_DIFF_BUCKET.'
        )
    if start and days:
        raise click.UsageError('Pass only one of --start or --days.')

    end_date = end.date() if end else datetime.datetime.now(datetime.timezone.utc).date()
    if start:
        start_date = start.date()
    else:
        # default to a 7-day window ending at --end
        start_date = end_date - datetime.timedelta(days=(days or 7) - 1)
    if start_date > end_date:
        raise click.UsageError('--start must be on or before --end.')

    prefixes = _day_prefixes(start_date, end_date)
    base = AnyPath(bucket)
    roots = [base / prefix for prefix in prefixes]
    click.echo(
        f'Scanning {bucket} for captures from {start_date} to {end_date} '
        f'({len(prefixes)} day(s))...',
        err=True,
    )

    results: list[Result] = []
    for blob, record in _iter_records(roots):
        results.append(
            _compare_record(
                blob,
                record,
                ignore_order=ignore_order,
                exclude_regex=exclude_regex,
            )
        )
        if limit and len(results) >= limit:
            break

    mismatches = [r for r in results if not r.matched]
    matches = [r for r in results if r.matched]

    lines: list[str] = []
    for result in results:
        if result.matched and not show_matches:
            continue
        lines.append(_format_result(result))
        lines.append('')

    # Summary, incl. a breakdown of mismatches by endpoint.
    lines.append('=' * 60)
    lines.append(
        f'Total: {len(results)}  Matches: {len(matches)}  '
        f'Mismatches: {len(mismatches)}'
    )
    if mismatches:
        by_endpoint: dict[str, int] = {}
        for r in mismatches:
            by_endpoint[f'{r.method} {r.path}'] = by_endpoint.get(
                f'{r.method} {r.path}', 0
            ) + 1
        lines.append('Mismatches by endpoint:')
        for endpoint, count in sorted(
            by_endpoint.items(), key=lambda kv: kv[1], reverse=True
        ):
            lines.append(f'  {count:>5}  {endpoint}')

    report = '\n'.join(lines)
    if output:
        AnyPath(output).write_text(report)
        click.echo(f'Wrote report to {output}', err=True)
    else:
        click.echo(report)

    # Non-zero exit if any mismatches, so it's usable in checks.
    sys.exit(1 if mismatches else 0)


if __name__ == '__main__':
    main()
