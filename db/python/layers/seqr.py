# pylint: disable=unnecessary-lambda-assignment,too-many-locals,broad-exception-caught

import os
import re
import asyncio
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Iterator, TypeVar

import aiohttp
import slack_sdk
import slack_sdk.errors
from cloudpathlib import AnyPath
from cpg_utils.cloud import get_google_identity_token

from api.settings import (
    SEQR_URL,
    SEQR_AUDIENCE,
    SEQR_MAP_LOCATION,
    SEQR_SLACK_NOTIFICATION_CHANNEL,
    get_slack_token,
)
from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.base import BaseLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.project import ProjectPermissionsTable
from db.python.enum_tables import SequencingTypeTable
from db.python.utils import ProjectId
from models.enums import AnalysisStatus

# literally the most temporary thing ever, but for complete
# automation need to have sample inclusion / exclusion
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_format,
)

SEQUENCING_GROUPS_TO_IGNORE = {22735, 22739}

_url_individuals_sync = '/api/project/sa/{projectGuid}/individuals/sync'
_url_individual_meta_sync = '/api/project/sa/{projectGuid}/individuals_metadata/sync'
_url_family_sync = '/api/project/sa/{projectGuid}/families/sync'
_url_update_es_index = '/api/project/sa/{projectGuid}/add_dataset/variants'
_url_update_saved_variants = '/api/project/sa/{projectGuid}/saved_variant/update'
_url_igv_diff = '/api/project/sa/{projectGuid}/igv/diff'
_url_igv_individual_update = '/api/individual/sa/{individualGuid}/igv/update'

T = TypeVar('T')


def chunk(iterable: Iterable[T], chunk_size=50) -> Iterator[list[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    chnk: list[T] = []
    for element in iterable:
        chnk.append(element)
        if len(chnk) >= chunk_size:
            yield chnk
            chnk = []

    if chnk:
        yield chnk


class SeqrLayer(BaseLayer):
    """Layer for more complex seqr logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.flayer = FamilyLayer(connection)
        self.player = ParticipantLayer(connection)

    @staticmethod
    async def is_seqr_sync_setup():
        """Check if metamist is configured to interact with seqr"""
        return SEQR_URL and SEQR_AUDIENCE

    @staticmethod
    def get_seqr_link_from_guid(guid: str):
        """Get link to seqr project from guid"""
        return f'{SEQR_URL}/project/{guid}/project_page'

    @staticmethod
    def get_meta_key_from_sequence_type(sequence_type: str):
        """
        Convenience method for computing the key where the SEQR_GUID
        is stored within the project.meta
        """
        return f'seqr-project-{sequence_type}'

    async def get_synchronisable_types(
        self, project_id: ProjectId | None = None
    ) -> list[str]:
        """
        Check the project meta to find out which sequence_types are synchronisable
        """
        if not await self.is_seqr_sync_setup():
            return []

        pptable = ProjectPermissionsTable(connection=self.connection.connection)
        project = await pptable.get_project_by_id(project_id or self.connection.project)

        has_access = pptable.check_access_to_project_id(
            user=self.author,
            project_id=project.id,
            readonly=False,
            raise_exception=False,
        )
        if not has_access:
            return []

        sequencing_types = await SequencingTypeTable(connection=self.connection).get()
        sts = [
            st
            for st in sequencing_types
            if self.get_meta_key_from_sequence_type(st) in project.meta
        ]
        return sts

    async def sync_dataset(
        self,
        sequence_type: str,
        sync_families: bool = True,
        sync_individual_metadata: bool = True,
        sync_individuals: bool = True,
        sync_es_index: bool = True,
        sync_saved_variants: bool = True,
        sync_cram_map: bool = True,
        post_slack_notification: bool = True,
    ) -> dict[str, list[str]]:
        """Sync a specific dataset for seqr"""
        if not await self.is_seqr_sync_setup():
            raise ValueError('Seqr synchronisation is not configured in metamist')

        token = self.generate_seqr_auth_token()
        pptable = ProjectPermissionsTable(connection=self.connection.connection)
        project = await pptable.get_project_by_id(self.connection.project)

        seqr_guid = project.meta.get(
            self.get_meta_key_from_sequence_type(sequence_type)
        )

        if not seqr_guid:
            raise ValueError(
                f'The project {project.name} does NOT have an appropriate seqr '
                f'project attached for {sequence_type}'
            )

        seqlayer = SequencingGroupLayer(self.connection)

        pid_to_sid_map = (
            await seqlayer.get_participant_ids_sequencing_group_ids_for_sequencing_type(
                sequence_type
            )
        )
        participant_ids = list(pid_to_sid_map.keys())
        sequencing_group_ids = set(
            sid for sids in pid_to_sid_map.values() for sid in sids
        )
        families = await self.flayer.get_families_by_participants(participant_ids)
        family_ids = set(f.id for fams in families.values() for f in fams)

        # filter to only participants with a family
        participant_ids = list(families.keys())

        if not family_ids and not participant_ids:
            raise ValueError('No families / participants to synchronize')

        messages = []
        async with aiohttp.ClientSession() as session:
            params = {
                'headers': {'Authorization': f'Bearer {token}'},
                'project_guid': seqr_guid,
                'session': session,
            }

            promises = []

            # Sync pedigree separately AND first, and don't continue if there's an error
            if sync_individuals:
                try:
                    messages.extend(
                        await self.sync_pedigree(family_ids=family_ids, **params)
                    )
                except Exception as e:
                    _errors = [
                        ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                    ]
                    if post_slack_notification:
                        _errors.extend(
                            self.send_slack_notification(
                                project_name=project.name,
                                sequence_type=sequence_type,
                                errors=_errors,
                                messages=messages,
                                seqr_guid=seqr_guid,
                            )
                        )
                        messages.append('Sent a notification to slack')
                    return {'errors': _errors, 'messages': messages}

            if sync_families:
                promises.append(self.sync_families(family_ids=family_ids, **params))

            if sync_individual_metadata:
                promises.append(
                    self.sync_individual_metadata(
                        participant_ids=participant_ids, **params
                    )
                )
            if sync_es_index:
                promises.append(
                    self.update_es_index(
                        sequencing_type=sequence_type,
                        sequencing_group_ids=sequencing_group_ids,
                        **params,
                    )
                )
            if sync_saved_variants:
                promises.append(self.update_saved_variants(**params))
            if sync_cram_map:
                promises.append(
                    self.sync_cram_map(
                        sequence_type=sequence_type,
                        participant_ids=participant_ids,
                        **params,
                    )
                )

            _messages = await asyncio.gather(
                *promises,
                return_exceptions=True,
            )
            errors = []
            for m in _messages:
                if isinstance(m, BaseException):
                    errors.append(m)
                else:
                    messages.extend(m)

        _errors = [
            ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            for e in errors
        ]
        if post_slack_notification:
            _errors.extend(
                self.send_slack_notification(
                    project_name=project.name,
                    sequence_type=sequence_type,
                    seqr_guid=seqr_guid,
                    errors=_errors,
                    messages=messages,
                )
            )
            messages.append('Sent a notification to slack')

        if _errors:
            return {'errors': _errors, 'messages': messages}
        return {'messages': messages}

    def generate_seqr_auth_token(self):
        """Generate an OAUTH2 token for talking to seqr"""
        return get_google_identity_token(target_audience=SEQR_AUDIENCE)

    async def sync_families(
        self,
        session: aiohttp.ClientSession,
        project_guid: str,
        headers: dict[str, str],
        family_ids: set[int],
    ) -> list[str]:
        """
        Synchronise families template from SM -> seqr
        """

        fam_rows = await self.flayer.get_families_by_ids(family_ids=list(family_ids))
        if not fam_rows:
            return ['No families to synchronise']
        family_data = [
            {
                'familyId': fam.external_id,
                'displayName': fam.external_id,
                'description': fam.description,
                'codedPhenotype': fam.coded_phenotype,
            }
            for fam in fam_rows
        ]

        # 1. Get family data from SM

        # use a filename ending with .csv to signal to seqr it's comma-delimited
        req_url = SEQR_URL + _url_family_sync.format(projectGuid=project_guid)
        resp_2 = await session.post(
            req_url, json={'families': family_data}, headers=headers
        )
        resp_2.raise_for_status()
        return [f'Synchronised {len(fam_rows)} families']

    async def sync_pedigree(
        self,
        session: aiohttp.ClientSession,
        project_guid,
        headers,
        family_ids: set[int],
    ) -> list[str]:
        """
        Synchronise pedigree from SM -> seqr in 3 steps:

        1. Get pedigree from SM
        2. Upload pedigree to seqr
        3. Confirm the upload
        """

        # 1. Get pedigree from SM
        pedigree_data = await self._get_pedigree_from_sm(family_ids=family_ids)
        if not pedigree_data:
            return ['No pedigree to synchronise']

        # 2. Upload pedigree to seqr
        req_url = SEQR_URL + _url_individuals_sync.format(projectGuid=project_guid)
        resp = await session.post(
            req_url, json={'individuals': pedigree_data}, headers=headers
        )
        resp.raise_for_status()

        return [f'Uploaded {len(pedigree_data)} rows of pedigree data']

    async def sync_individual_metadata(
        self,
        session: aiohttp.ClientSession,
        project_guid,
        headers,
        participant_ids: list[int],
    ):
        """
        Sync individual participant metadata (eg: phenotypes)
        for a dataset into a seqr project
        """

        processed_records = await self.get_individual_meta_objs_for_seqr(
            participant_ids
        )

        if not processed_records:
            return ['No individual metadata to synchronise']

        req_url = SEQR_URL + _url_individual_meta_sync.format(projectGuid=project_guid)
        resp = await session.post(
            req_url, json={'individuals': processed_records}, headers=headers
        )

        if resp.status == 400 and 'Unable to find individuals to update' in resp.text:
            return [
                f'No individual metadata needed updating (from {len(processed_records)} rows)'
            ]

        resp.raise_for_status()

        return [
            f'Uploaded individual metadata for {len(processed_records)} individuals'
        ]

    async def update_es_index(
        self,
        session: aiohttp.ClientSession,
        sequencing_type: str,
        project_guid,
        headers,
        sequencing_group_ids: set[int],
    ) -> list[str]:
        """Update seqr samples for latest elastic-search index"""
        eid_to_sgid_rows = await self.player.get_external_participant_id_to_internal_sequencing_group_id_map(
            self.connection.project, sequencing_type=sequencing_type
        )

        # format sample ID for transport
        person_sample_map_rows: list[tuple[str, str]] = [
            (p[0], sequencing_group_id_format(p[1]))
            for p in eid_to_sgid_rows
            if p[1] not in SEQUENCING_GROUPS_TO_IGNORE
        ]

        # highlighting that seqr wants:
        #   Col 1: Sample Ids (sequencing group ID in our case)
        #   Col 2: Seqr Individual Ids (column 2).
        rows_to_write = [
            '\t'.join(s[::-1])
            for s in person_sample_map_rows
            if not any(sid in s for sid in SEQUENCING_GROUPS_TO_IGNORE)
        ]

        filename = f'{project_guid}_pid_sgid_map_{datetime.now().isoformat()}.tsv'
        # remove any non-filename compliant filenames
        filename = re.sub(r'[/\\?%*:|\'<>\x7F\x00-\x1F]', '-', filename)

        fn_path = os.path.join(SEQR_MAP_LOCATION, filename)
        # pylint: disable=no-member

        alayer = AnalysisLayer(connection=self.connection)
        es_index_analyses = await alayer.query_analysis(
            project_ids=[self.connection.project],
            analysis_type='es-index',
            meta={'sequencing_type': sequencing_type},
            status=AnalysisStatus('completed'),
        )

        es_index_analyses = sorted(
            es_index_analyses,
            key=lambda el: el.timestamp_completed,
        )

        if len(es_index_analyses) == 0:
            return [f'No ES index to synchronise']

        with AnyPath(fn_path).open('w+') as f:
            f.write('\n'.join(rows_to_write))

        es_index = es_index_analyses[-1].output

        messages = []

        if sequencing_group_ids:
            sequencing_groups_in_new_index = set(
                es_index_analyses[-1].sequencing_group_ids
            )

            if len(es_index_analyses) > 1:
                sequencing_groups_in_old_index = set(
                    es_index_analyses[-2].sequencing_group_ids
                )
                sequencing_groups_diff = sequencing_group_id_format_list(
                    sequencing_groups_in_new_index - sequencing_groups_in_old_index
                )
                if sequencing_groups_diff:
                    messages.append(
                        'Samples added to index: ' + ', '.join(sequencing_groups_diff),
                    )

            sg_ids_missing_from_index = sequencing_group_id_format_list(
                sequencing_group_ids - sequencing_groups_in_new_index
            )
            if sg_ids_missing_from_index:
                messages.append(
                    'Sequencing groups missing from index: '
                    + ', '.join(sg_ids_missing_from_index),
                )

        req1_url = SEQR_URL + _url_update_es_index.format(projectGuid=project_guid)
        resp_1 = await session.post(
            req1_url,
            json={
                'elasticsearchIndex': es_index,
                'datasetType': 'VARIANTS',
                'mappingFilePath': fn_path,
                'ignoreExtraSamplesInCallset': True,
            },
            headers=headers,
        )
        resp_1.raise_for_status()

        messages.append(f'Updated ES index {es_index}')

        return messages

    async def update_saved_variants(
        self,
        session: aiohttp.ClientSession,
        project_guid,
        headers,
    ) -> list[str]:
        """Update saved variants"""
        req2_url = SEQR_URL + _url_update_saved_variants.format(
            projectGuid=project_guid
        )
        resp_2 = await session.post(req2_url, json={}, headers=headers)
        resp_2.raise_for_status()

        return ['Updated saved variants']

    async def sync_cram_map(
        self,
        session: aiohttp.ClientSession,
        participant_ids: list[int],
        sequence_type: str,
        project_guid: str,
        headers,
    ):
        """Get map of participant EID to cram path"""

        alayer = AnalysisLayer(self.connection)

        reads_map = await alayer.get_sample_cram_path_map_for_seqr(
            project=self.connection.project,
            sequence_types=[sequence_type],
            participant_ids=participant_ids,
        )
        output_filter = lambda row: True  # noqa
        # eventually solved by sequence groups
        if sequence_type == 'genome':
            output_filter = lambda output: 'exome' not in output  # noqa
        elif sequence_type == 'exome':
            output_filter = lambda output: 'exome' in output  # noqa

        parsed_records = defaultdict(list)

        for row in reads_map:
            output = row['output']
            if not output_filter(output):
                continue
            participant_id = row['participant_id']
            parsed_records[participant_id].append(
                {
                    'filePath': output,
                    'sampleId': sequencing_group_id_format(row['sequencing_group_id']),
                }
            )

        if not reads_map:
            return ['No CRAMs to synchronise']

        req_url = SEQR_URL + _url_igv_diff.format(projectGuid=project_guid)
        resp = await session.post(
            req_url, json={'mapping': parsed_records}, headers=headers
        )
        resp.raise_for_status()

        response = await resp.json()
        if 'updates' not in response:
            return [f'All CRAMs ({len(reads_map)}) are up to date']

        async def _make_update_igv_call(update):
            individual_guid = update['individualGuid']
            req_igv_update_url = SEQR_URL + _url_igv_individual_update.format(
                individualGuid=individual_guid
            )
            igv_resp = await session.post(
                req_igv_update_url, json=update, headers=headers
            )

            igv_resp.raise_for_status()
            return await igv_resp.text()

        chunk_size = 10
        all_updates = response['updates']
        exceptions: list[tuple[str, Exception]] = []
        for idx, updates in enumerate(chunk(all_updates, chunk_size=10)):
            start = idx * chunk_size + 1
            finish = start + len(updates)
            print(f'Updating CRAMs {start} -> {finish} (/{len(all_updates)})')

            responses = await asyncio.gather(
                *[_make_update_igv_call(update) for update in updates],
                return_exceptions=True,
            )
            exceptions.extend(
                (update['sampleId'], e)
                for update, e in zip(updates, responses)
                if isinstance(e, Exception)
            )

        if exceptions:
            ps = '; '.join(f'{sid}: {ex}' for sid, ex in exceptions)
            return [f'Could not update {len(exceptions)} IGV paths: {ps}']

        return [f'{len(all_updates)} (/{len(reads_map)}) CRAMs were updated']

    async def _get_pedigree_from_sm(self, family_ids: set[int]) -> list[dict] | None:
        """Call get_pedigree and return formatted string with header"""

        ped_rows = await self.flayer.get_pedigree(
            self.connection.project,
            family_ids=list(family_ids),
            replace_with_family_external_ids=True,
            replace_with_participant_external_ids=True,
        )

        if not ped_rows:
            return None

        def process_sex(value):
            if not value:
                return ''
            if value == 'U':
                return 'U'
            if isinstance(value, str):
                return value[0]
            if not isinstance(value, int):
                raise ValueError(f'Unexpected type for sex {type(value)}: {value}')
            return {1: 'M', 2: 'F'}.get(value, 'U')

        def process_affected(value):
            if not isinstance(value, int):
                raise ValueError(f'Unexpected affected value: {value}')
            return {
                -9: 'U',
                0: 'U',
                1: 'N',
                2: 'A',
            }[value]

        keys = {
            'familyId': 'family_id',
            'individualId': 'individual_id',
            'paternalId': 'paternal_id',
            'maternalId': 'maternal_id',
            # 'sex': 'sex',
            # 'affected': 'affected',
            'notes': 'notes',
        }

        def get_row(row):
            d = {
                seqr_key: row[sm_key]
                for seqr_key, sm_key in keys.items()
                if sm_key in row
            }
            d['sex'] = process_sex(row['sex'])
            d['affected'] = process_affected(row['affected'])

            return d

        return list(map(get_row, ped_rows))

    async def get_individual_meta_objs_for_seqr(
        self, participant_ids: list[int]
    ) -> list[dict] | None:
        """Get formatted list of dictionaries for syncing individual meta to seqr"""
        individual_metadata_resp = await self.player.get_seqr_individual_template(
            self.connection.project, internal_participant_ids=participant_ids
        )

        json_rows: list[dict] = individual_metadata_resp['rows']

        if len(json_rows) == 0:
            return None

        def _process_hpo_terms(terms: str):
            return [t.strip() for t in terms.split(',')]

        def _parse_affected(affected):
            affected = str(affected).upper()
            if affected in ('1', 'U', 'UNAFFECTED'):
                return 'N'
            if affected == '2' or affected.startswith('A'):
                return 'A'
            if not affected or affected in ('0', 'UNKNOWN'):
                return 'U'

            return None

        def _parse_consanguity(consanguity):
            if not consanguity:
                return None

            if isinstance(consanguity, bool):
                return consanguity

            if consanguity.lower() in ('t', 'true', 'yes', 'y', '1'):
                return True

            if consanguity.lower() in ('f', 'false', 'no', 'n', '0'):
                return False

            return None

        key_processor = {
            'hpo_terms_present': _process_hpo_terms,
            'hpo_terms_absent': _process_hpo_terms,
            'affected': _parse_affected,
            'consanguinity': _parse_consanguity,
        }

        seqr_map = {
            'family_id': 'family_id',
            'individual_id': 'individual_id',
            'affected': 'affected',
            'features': 'hpo_terms_present',
            'absent_features': 'hpo_terms_absent',
            'birth_year': 'birth_year',
            'death_year': 'death_year',
            'onset_age': 'age_of_onset',
            'notes': 'notes',
            'maternal_ethnicity': 'maternal_ancestry',
            'paternal_ethnicity': 'paternal_ancestry',
            'consanguinity': 'consanguinity',
            'affected_relatives': 'affected_relatives',
            'expected_inheritance': 'expected_inheritance',
            # 'assigned_analyst',
            # 'disorders',
            # 'rejected_genes',
            # 'candidate_genes',
        }

        def process_row(row):
            return {
                seqr_key: key_processor[sm_key](row[sm_key])
                if sm_key in key_processor
                else row[sm_key]
                for seqr_key, sm_key in seqr_map.items()
                if sm_key in row
            }

        return list(map(process_row, json_rows))

    def send_slack_notification(
        self,
        project_name: str,
        sequence_type: str,
        messages: list[str],
        errors: list[str],
        seqr_guid: str,
    ):
        """Generate and send slack notification from responses"""
        slack_channel = SEQR_SLACK_NOTIFICATION_CHANNEL
        if not slack_channel:
            return ['Slack channel not setup for seqr notifications']

        slack_client = slack_sdk.WebClient(token=get_slack_token())
        try:
            seqr_link = self.get_seqr_link_from_guid(seqr_guid)
            pn_link = f'<{seqr_link}|{project_name}>'
            if errors:
                text = (
                    f':rotating_light: Error syncing *{pn_link}* '
                    f'(_{sequence_type}_) seqr project :rotating_light:'
                )
            else:
                text = f'Synced {pn_link} ({sequence_type}) seqr project'

            blocks = []
            if errors:
                error_block = ['*Errors*']
                for error in errors:
                    error_block.append(f'> ```{error}```')
                blocks.append('\n'.join(error_block))

            if messages:
                blocks.append('*Messages*\n\n' + '\n'.join(f'* {e}' for e in messages))

            text = '\n\n'.join([text, *blocks])

            slack_client.api_call(
                'chat.postMessage',
                json={
                    'channel': slack_channel,
                    'text': text,
                },
            )
        except slack_sdk.errors.SlackApiError as err:
            return [f'SlackAPI error: {err}']
        except Exception as e:
            return [f'Error posting to slack: {e}']

        return []
