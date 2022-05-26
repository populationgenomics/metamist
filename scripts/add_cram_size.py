# pylint: disable=too-many-nested-blocks,logging-not-lazy
"""
This script goes through all CRAMS in sample-metadata, gets the size,
and updates the meta['size'] attribute on the analysis.
"""
import logging
import os
import asyncio
from itertools import groupby
from typing import Dict, List

from google.cloud import storage
from google.api_core.exceptions import NotFound

from sample_metadata.apis import AnalysisApi, ProjectApi
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_update_model import AnalysisUpdateModel
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.parser.generic_parser import chunk

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

client = storage.Client()
aapi = AnalysisApi()


def get_bucket_name_from_path(path):
    """
    >>> get_bucket_name_from_path('gs://my-bucket/path')
    'my-bucket'
    """

    if not path.startswith('gs://'):
        raise ValueError(f'path does not start with gs://: {path}')

    return path[len('gs://') :].split('/', maxsplit=1)[0]


async def process_all_projects():
    """Process all projects"""

    projects = await ProjectApi().get_my_projects_async()
    for pobj in projects:
        pname = pobj['name']
        if pname.endswith('test'):
            continue
        await process_project(pname)


async def process_project(project: str):
    """Process a single project"""
    logger.info(f'{project} :: Begin processing')
    analysis = await aapi.query_analyses_async(
        AnalysisQueryModel(
            projects=[project],
            status=AnalysisStatus('completed'),
            type=AnalysisType('cram'),
        )
    )

    analysis_by_file = {a['output']: a for a in analysis if not a['meta'].get('size')}
    logger.info(
        f'{project} :: Found {len(analysis_by_file)}/{len(analysis)} actionable files'
    )

    if len(analysis_by_file) == 0:
        logger.info(f'{project} :: skipping')
        return

    base_paths = set(os.path.dirname(a) for a in analysis_by_file)
    base_paths_by_bucket: Dict[str, List[str]] = {
        k: list(v) for k, v in groupby(base_paths, get_bucket_name_from_path)
    }

    # the next few lines are equiv to `bucket.get_blob(path)`
    # but without requiring storage.objects.get permission
    updaters: Dict[int, AnalysisUpdateModel] = {}
    missing_files: List[str] = []
    for bucket_name, bps in base_paths_by_bucket.items():
        file_size_by_name = {}
        try:
            bucket = client.get_bucket(bucket_name)
            bucket_path = f'gs://{bucket_name}/'
            bucket_name_length = len(bucket_path)
            for path in bps:
                prefix = path[bucket_name_length:]
                try:
                    for blob in client.list_blobs(bucket, prefix=prefix):
                        if not blob.name.endswith('.cram'):
                            continue
                        file_size_by_name[
                            os.path.join(bucket_path, blob.name)
                        ] = blob.size
                except NotFound:
                    continue
        except NotFound:
            pass

        for file, a in analysis_by_file.items():
            if file not in file_size_by_name:
                missing_files.append(file)
                continue

            updaters[a['id']] = AnalysisUpdateModel(
                meta={'size': file_size_by_name[file]},
                status=AnalysisStatus(a['status']),
            )
    logger.info(f'{project} :: Updating {len(updaters)} analysis objects')

    for updates in chunk(list(updaters.items())):
        promises = []
        for aid, aupdate in updates:
            promises.append(aapi.update_analysis_status_async(aid, aupdate))
        await asyncio.gather(*promises)

    if missing_files:
        logger.error(
            f'{project} :: Missing {len(missing_files)} files: '
            + ', '.join(missing_files)
        )

    logger.info(f'{project} :: Finished processing')

    print(base_paths)


if __name__ == '__main__':
    asyncio.new_event_loop().run_until_complete(process_all_projects())
