# pylint: disable=missing-timeout,unnecessary-lambda-assignment,import-outside-toplevel,too-many-locals
import asyncio
import json
from typing import Any

import aiohttp

from metamist.parser.generic_parser import chunk

from .config import (
    SeqrDatasetType,
    url_family_sync,
    url_igv_diff,
    url_igv_individual_update,
    url_individual_metadata_sync,
    url_individuals_sync,
    url_update_es_index,
    url_update_saved_variants,
)
from .data_fetchers import MetamistFetcher
from .logging_config import logger
from .utils import get_token


class SeqrSync:
    """Class to sync datasets to seqr via the seqr API urls"""

    def __init__(
        self,
        seqr_url_base: str,
        dataset: str,
        project_guid: str,
        sequencing_type: str,
        seqr_dataset_type: SeqrDatasetType = None,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.seqr_url_base = seqr_url_base
        self.dataset = dataset
        self.project_guid = project_guid
        self.sequencing_type = sequencing_type
        self.seqr_dataset_type = seqr_dataset_type
        self.dry_run = dry_run
        self.verbose = verbose

    def sync_dataset(
        self,
        data_to_sync: dict[str, Any],
    ):
        """Sync single dataset without looking up seqr guid"""
        return asyncio.new_event_loop().run_until_complete(
            self.sync_dataset_async(
                data_to_sync,
            )
        )

    async def sync_dataset_async(
        self,
        data_to_sync: dict[str, Any],
    ):
        """Synchronisation driver for a single dataset"""
        logger.info(
            f'{self.dataset} ({self.sequencing_type} - {self.seqr_dataset_type.value}) :: Syncing to {self.project_guid}'
        )
        token = get_token()
        async with aiohttp.ClientSession() as client:
            headers: dict[str, str] = {'Authorization': f'Bearer {token}'}
            params: dict[str, Any] = {
                'headers': headers,
                'session': client,
            }
            if families_metadata := data_to_sync.get('families'):
                await self.sync_families_metadata(
                    **params, families_metadata=families_metadata
                )
            if pedigree_data := data_to_sync.get('pedigree'):
                await self.sync_pedigree(**params, pedigree_data=pedigree_data)
            if individual_metadata := data_to_sync.get('individual_metadata'):
                await self.sync_individual_metadata(
                    **params, individual_metadata=individual_metadata
                )
            if es_index_data := data_to_sync.get('es_index_data'):
                await self.sync_es_index_analyses(
                    **params,
                    es_index_data=es_index_data,
                )
            if data_to_sync.get('sync_saved_variants'):
                await self.update_saved_variants(
                    **params,
                )
            if reads_map := data_to_sync.get('reads_map'):
                await self.sync_reads_map(
                    **params,
                    peid_to_reads_map=reads_map,
                )

    async def sync_pedigree(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        pedigree_data: list[dict],
    ):
        """
        Synchronise pedigree data to a seqr project
        """
        req_url = self.seqr_url_base + url_individuals_sync.format(
            projectGuid=self.project_guid
        )
        logger.info(f'{self.dataset} :: Uploading pedigree to {req_url}')
        if self.verbose:
            logger.info(f'{self.dataset} :: {json.dumps(pedigree_data, indent=2)}')
        if self.dry_run:
            logger.info(f'{self.dataset} :: Dry run, skipping pedigree sync')
            logger.info(
                f'{self.dataset} :: Would have uploaded {len(pedigree_data)} records'
            )
            return
        resp = await session.post(
            req_url, json={'individuals': pedigree_data}, headers=headers
        )
        if not resp.ok:
            logger.warning(
                f'{self.dataset} :: Confirming pedigree failed: {await resp.text()}'
            )
            with open(f'{self.dataset}.ped', 'w+') as f:
                import csv

                writer = csv.writer(f, delimiter='\t')
                col_headers = [
                    'familyId',
                    'individualId',
                    'paternalId',
                    'maternalId',
                    'sex',
                    'affected',
                ]
                writer.writerows(
                    [[row[h] for h in col_headers] for row in pedigree_data]
                )

        resp.raise_for_status()

        logger.info(f'{self.dataset} :: Uploaded pedigree')

    async def sync_families_metadata(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        families_metadata: list[dict],
    ):
        """
        Synchronise families metadata to a seqr project
        """
        req_url = self.seqr_url_base + url_family_sync.format(
            projectGuid=self.project_guid
        )
        logger.info(f'{self.dataset} :: Uploading family template to {req_url}')
        if self.verbose:
            logger.info(f'{self.dataset} :: {json.dumps(families_metadata, indent=2)}')
        if self.dry_run:
            logger.info(f'{self.dataset} :: Dry run, skipping families sync')
            logger.info(
                f'{self.dataset} :: Would have uploaded {len(families_metadata)} records'
            )
            return

        resp_2 = await session.post(
            req_url, json={'families': families_metadata}, headers=headers
        )
        resp_2.raise_for_status()
        logger.info(f'{self.dataset} :: Uploaded family template')

    async def sync_individual_metadata(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        individual_metadata: list[dict],
    ):
        """
        Sync individual participant metadata (eg: phenotypes) to a seqr project
        """
        req_url = self.seqr_url_base + url_individual_metadata_sync.format(
            projectGuid=self.project_guid
        )
        logger.info(f'{self.dataset} :: Uploading individual metadata to {req_url}')
        if self.verbose:
            logger.info(
                f'{self.dataset} :: {json.dumps(individual_metadata, indent=2)}'
            )
        if self.dry_run:
            logger.info(f'{self.dataset} :: Dry run, skipping individual metadata sync')
            logger.info(
                f'{self.dataset} :: Would have uploaded {len(individual_metadata)} records'
            )
            return

        resp = await session.post(
            req_url, json={'individuals': individual_metadata}, headers=headers
        )
        resp_text = await resp.text()
        if resp.status == 400 and 'Unable to find individuals to update' in resp_text:
            logger.info(f'{self.dataset} :: No individual metadata needed updating')
            return

        if not resp.ok:
            logger.info(
                f'{self.dataset} :: Error syncing individual metadata {resp_text}'
            )
            resp.raise_for_status()

        logger.info(f'{self.dataset} :: Uploaded individual metadata')

    async def sync_es_index_analyses(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        es_index_data: list[dict],
    ):
        """Update seqr samples for latest elastic-search index"""
        for es_index in es_index_data:
            es_index_name = es_index['elasticsearchIndex']
            req_url = self.seqr_url_base + url_update_es_index.format(
                projectGuid=self.project_guid
            )
            logger.info(
                f'{self.dataset} :: {self.seqr_dataset_type.value} :: Updating ES index {es_index_name!r} to {req_url}'
            )
            if self.verbose:
                logger.info(f'{self.dataset} :: {json.dumps(es_index, indent=2)}')
            if self.dry_run:
                logger.info(f'{self.dataset} :: Dry run, skipping actual update')
                return

            resp_1 = await session.post(req_url, json=es_index, headers=headers)
            logger.info(
                f'{self.dataset} :: {self.seqr_dataset_type} :: Updated ES index {es_index_name!r} with status: {resp_1.status}'
            )
            if not resp_1.ok:
                logger.info(
                    f'{self.dataset} :: Request failed with information: {resp_1.text}'
                )
            resp_1.raise_for_status()

    async def update_saved_variants(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ):
        """POST an empty JSON to update saved variants in the seqr project"""
        req2_url = self.seqr_url_base + url_update_saved_variants.format(
            projectGuid=self.project_guid
        )
        if self.dry_run:
            logger.info(f'{self.dataset} :: Dry run, skipping update saved variants')
            return
        resp_2 = await session.post(req2_url, json={}, headers=headers)
        logger.info(
            f'{self.dataset} :: Updated saved variants with status code: {resp_2.status}'
        )
        if not resp_2.ok:
            logger.info(
                f'{self.dataset} :: Request failed with information: {resp_2.text()}'
            )
        resp_2.raise_for_status()

    async def get_igv_diff(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        peid_to_reads_map: dict[str, list[str]],
        number_of_uploadable_reads: int,
    ):
        """Get IGV diff for a dataset to check if any CRAMS need updating"""
        req1_url = self.seqr_url_base + url_igv_diff.format(
            projectGuid=self.project_guid
        )
        if self.dry_run:
            logger.info(f'{self.dataset} :: Dry run, skipping CRAM sync')
            logger.info(
                f'{self.dataset} :: Would have uploaded {number_of_uploadable_reads} records'
            )
            return {'updates': []}

        resp_1 = await session.post(
            req1_url, json={'mapping': peid_to_reads_map}, headers=headers
        )
        if not resp_1.ok:
            t = await resp_1.text()
            logger.info(f'{self.dataset} :: Failed to diff CRAM updates: {t!r}')
        resp_1.raise_for_status()

        response = await resp_1.json()
        if 'updates' not in response:
            logger.info(f'{self.dataset} :: All CRAMS are up to date')

        return response

    async def sync_reads_map(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        peid_to_reads_map: dict[str, list[str]],
    ):
        """
        Get map of participant EID to cram path and sync it to seqr in three steps:

        1. Get the cram map from Metamist or file
        2. Check which CRAMS need updating with IGV diff
        3. Update the CRAMS in seqr
        """
        logger.info(f'{self.dataset} :: Getting cram map')
        number_of_uploadable_reads = sum(
            len(reads) for reads in peid_to_reads_map.values()
        )
        logger.info(
            f'{self.dataset} :: Found {number_of_uploadable_reads} uploadable reads.'
        )

        response = await self.get_igv_diff(
            session,
            headers,
            peid_to_reads_map,
            number_of_uploadable_reads,
        )
        if 'updates' not in response:
            logger.info(f'{self.dataset} :: All CRAMS are up to date')
            return

        async def _make_update_igv_call(update):
            individual_guid = update['individualGuid']
            req_igv_update_url = self.seqr_url_base + url_igv_individual_update.format(
                individualGuid=individual_guid
            )
            resp = await session.post(req_igv_update_url, json=update, headers=headers)

            t = await resp.text()
            if not resp.ok:
                raise ValueError(
                    f'{self.dataset} :: Failed to update {individual_guid} with response: {t!r})',
                    resp,
                )

            return t

        chunk_size = 10
        wait_time = 3
        all_updates = response['updates']
        exceptions = []
        for idx, updates in enumerate(chunk(all_updates, chunk_size=10)):
            if not updates:
                continue
            logger.info(
                f'{self.dataset} :: Updating CRAMs {idx * chunk_size + 1} -> {(min((idx + 1 ) * chunk_size, len(all_updates)))} (/{len(all_updates)})'
            )

            responses = await asyncio.gather(
                *[_make_update_igv_call(update) for update in updates],
                return_exceptions=True,
            )
            exceptions.extend(
                [(r, u) for u, r in zip(updates, responses) if isinstance(r, Exception)]
            )
            await asyncio.sleep(wait_time)

        if exceptions:
            exceptions_str = '\n'.join(f'\t{e} {u}' for e, u in exceptions)
            logger.info(
                f'{self.dataset} :: Failed to update {len(exceptions)} CRAMs: \n{exceptions_str}'
            )
        logger.info(
            f'{self.dataset} :: Updated {len(all_updates)} / {number_of_uploadable_reads} CRAMs'
        )

    def sync_single_dataset_from_name(
        self,
        data_to_sync: dict[str, Any],
    ):
        """Sync a single dataset by fetching the guid and initiating sync"""
        seqr_projects = MetamistFetcher().get_seqr_projects(
            sequencing_type=self.sequencing_type, ignore_datasets=None
        )
        for project in seqr_projects:  # type: ignore
            project_name = project['name']
            project_guid = project['guid']
            if project_name != self.dataset:
                continue

            logger.info(f'Syncing {project_name} to {project_guid}')

            return self.sync_dataset(
                data_to_sync=data_to_sync,
            )

        raise ValueError(f'Could not find {self.dataset} seqr project')
