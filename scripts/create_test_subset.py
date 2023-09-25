#!/usr/bin/env python3
# type: ignore
# pylint: skip-file


# # pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order,unused-argument,too-many-arguments

""" Example Invocation

analysis-runner \
--dataset acute-care --description "populate acute care test subset" --output-dir "acute-care-test" \
--access-level full \
scripts/create_test_subset.py --project acute-care --families 4

This example will populate acute-care-test with the metamist data for 4 families.
"""

from typing import Optional, Counter as CounterType
import csv
import logging
import os
import random
import subprocess
from collections import Counter

import click
from google.cloud import storage

from metamist.apis import AnalysisApi, AssayApi, FamilyApi, ParticipantApi, SampleApi
from metamist.models import (
    AssayUpsert,
    SampleUpsert,
    Analysis,
    AnalysisStatus,
    AnalysisUpdateModel,
    SequencingGroupUpsert,
)

from metamist.graphql import gql, query

logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)

sapi = SampleApi()
aapi = AnalysisApi()
assayapi = AssayApi()
fapi = FamilyApi()
papi = ParticipantApi()

DEFAULT_SAMPLES_N = 10

QUERY_ALL_DATA = gql(
    """
    query getAllData($project: String!, $sids: [String!]) {
        project(name: $project) {
            samples(id: {in_: $sids}) {
                id
                meta
                type
                externalId
                participant {
                    externalId
                    id
                    karyotype
                    meta
                    reportedGender
                    reportedSex
                }
                sequencingGroups{
                    id
                    meta
                    platform
                    technology
                    type
                    assays {
                        id
                        meta
                        type
                    }
                    analyses {
                        active
                        id
                        meta
                        output
                        status
                        timestampCompleted
                        type
                    }
                }
            }
        }
    }
    """
)

# TODO: We can change this to filter external sample ids
EXISTING_DATA_QUERY = gql(
    """
    query getExistingData($project: String!) {
        project(name: $project) {
            samples{
                id
                externalId
                sequencingGroups {
                    id
                    type
                    assays {
                        id
                        type
                    }
                    analyses {
                        id
                        type
                    }
                }
            }
        }
    }
    """
)

QUERY_FAMILY_SGID = gql(
    """
    query FamilyQuery($project: String!) {
        project(name: $project) {
            families {
                id
                externalId
                participants {
                    samples {
                        id
                    }
                }
            }

        }
    }
"""
)

SG_ID_QUERY = gql(
    """
    query getSGIds($project: String!) {
        project(name: $project) {
            samples{
                id
                externalId
                sequencingGroups {
                    id
                }
            }
        }
    }
    """
)

PARTICIPANT_QUERY = """
    query ($project: String!) {
        project (externalId: $project) {
            participants {
                id
                externalId
            }
        }
    }
    """


@click.command()
@click.option(
    '--project',
    required=True,
    help='The metamist project ($DATASET)',
)
@click.option(
    '-n',
    '--samples',
    'samples_n',
    type=int,
    help='Number of samples to subset',
)
@click.option(
    '--families',
    'families_n',
    type=int,
    help='Minimal number of families to include',
)
# Flag to be used when there isn't available pedigree/family information.
@click.option(
    '--skip-ped',
    'skip_ped',
    is_flag=True,
    default=False,
    help='Skip transferring pedigree/family information',
)
@click.option(
    '--add-family',
    'additional_families',
    type=str,
    multiple=True,
    help="""Additional families to include.
    All samples from these fams will be included.
    This is in addition to the number of families specified in
    --families and the number of samples specified in -n""",
)
@click.option(
    '--add-sample',
    'additional_samples',
    type=str,
    multiple=True,
    help="""Additional samples to include.
    This is in addition to the number of families specified in
    --families and the number of samples specified in -n""",
)
@click.option(
    '--noninteractive',
    'noninteractive',
    is_flag=True,
    default=False,
    help='Skip interactive confirmation',
)
def main(
    project: str,
    samples_n: Optional[int],
    families_n: Optional[int],
    skip_ped: Optional[bool] = True,
    additional_families: Optional[tuple[str]] = None,
    additional_samples: Optional[tuple[str]] = None,
    noninteractive: Optional[bool] = False,
):
    """
    Script creates a test subset for a given project.
    A new project with a prefix -test is created, and for any files in sample/meta,
    sequence/meta, or analysis/output a copy in the -test namespace is created.
    """
    samples_n, families_n = _validate_opts(samples_n, families_n, noninteractive)
    _additional_families: list[str] = list(additional_families)
    _additional_samples: list[str] = list(additional_samples)

    # 1. Determine the sids to be moved into -test.
    specific_sids = _get_sids_for_families(
        project,
        families_n,
        _additional_families,
    )
    if not samples_n and not families_n:
        samples_n = DEFAULT_SAMPLES_N
    if not samples_n and families_n:
        samples_n = 0

    specific_sids = specific_sids + _additional_samples

    # 2. Get all sids in project.
    sid_output = query(SG_ID_QUERY, variables={'project': project})
    all_sids = [sid['id'] for sid in sid_output.get('project').get('samples')]

    # 3. Subtract the specific_sgs from all the sgs
    sgids_after_inclusions = list(set(all_sids) - set(specific_sids))
    # 4. Randomly select from the remaining sgs
    random_sgs: list[str] = []
    random.seed(42)  # for reproducibility
    if (samples_n - len(specific_sids)) > 0:
        random_sgs = random.sample(
            sgids_after_inclusions, samples_n - len(specific_sids)
        )
    # 5. Add the specific_sgs to the randomly selected sgs
    final_subset_sids = specific_sids + random_sgs
    # 6. Query all the samples from the selected sgs
    original_project_subset_data = query(
        QUERY_ALL_DATA, {'project': project, 'sids': final_subset_sids}
    )

    # Pull Participant Data
    participant_data = []
    participant_ids: list = []
    for sg in original_project_subset_data.get('project').get('samples'):
        participant = sg.get('participant')
        if participant:
            participant_data.append(participant)
            participant_ids.append(participant.get('externalId'))

    # Populating test project
    target_project = project + '-test'

    # Parse Families & Participants
    if skip_ped:
        # If no family data is available, only the participants should be transferred.
        upserted_participant_map = transfer_participants(
            target_project=target_project,
            participant_data=participant_data,
        )

    else:
        family_ids = transfer_families(project, target_project, participant_ids)
        upserted_participant_map = transfer_ped(project, target_project, family_ids)

    existing_data = query(EXISTING_DATA_QUERY, {'project': target_project})

    samples = original_project_subset_data.get('project').get('samples')
    transfer_samples_sgs_assays(
        samples, existing_data, upserted_participant_map, target_project, project
    )
    transfer_analyses(samples, existing_data, target_project, project)


def transfer_samples_sgs_assays(
    samples: dict,
    existing_data: dict,
    upserted_participant_map: dict[str, int],
    target_project: str,
    project: str,
):
    """
    Transfer samples, sequencing groups, and assays from the original project to the
    test project.
    """
    for s in samples:
        sample_sgs: list[SequencingGroupUpsert] = []
        for sg in s.get('sequencingGroups'):
            sg_assays: list[AssayUpsert] = []
            _existing_sg = _get_existing_sg(
                existing_data, s.get('externalId'), sg.get('type')
            )
            _existing_sgid = _existing_sg.get('id') if _existing_sg else None
            for assay in sg.get('assays'):
                _existing_assay: dict[str, str] = {}
                if _existing_sgid:
                    _existing_assay = _get_existing_assay(
                        existing_data,
                        s.get('externalId'),
                        _existing_sgid,
                        assay.get('type'),
                    )
                existing_assay_id = (
                    _existing_assay.get('id') if _existing_assay else None
                )
                assay_upsert = AssayUpsert(
                    type=assay.get('type'),
                    id=existing_assay_id,
                    external_ids=assay.get('externalIds') or {},
                    # sample_id=self.s,
                    meta=assay.get('meta'),
                )
                sg_assays.append(assay_upsert)
            sg_upsert = SequencingGroupUpsert(
                id=_existing_sgid,
                external_ids=sg.get('externalIds') or {},
                meta=sg.get('meta'),
                platform=sg.get('platform'),
                technology=sg.get('technology'),
                type=sg.get('type'),
                assays=sg_assays,
            )
            sample_sgs.append(sg_upsert)

        _sample_type = None if s['type'] == 'None' else s['type']
        _existing_sid: str = None
        _existing_sample = _get_existing_sample(existing_data, s['externalId'])
        if _existing_sample:
            _existing_sid = _existing_sample['id']

        _existing_pid: int = None
        if s['participant']:
            _existing_pid = upserted_participant_map[s['participant']['externalId']]

        sample_upsert = SampleUpsert(
            external_id=s['externalId'],
            type=_sample_type or None,
            meta=(_copy_files_in_dict(s['meta'], project) or {}),
            participant_id=_existing_pid,
            sequencing_groups=sample_sgs,
            id=_existing_sid,
        )

        logger.info(f'Processing sample {s["id"]}')
        logger.info('Creating test sample entry')
        sapi.create_sample(
            project=target_project,
            sample_upsert=sample_upsert,
        )


def transfer_analyses(
    samples: dict, existing_data: dict, target_project: str, project: str
):
    """
    This function will transfer the analyses from the original project to the test project.
    """
    new_sg_data = query(SG_ID_QUERY, {'project': target_project})

    new_sg_map = {}
    for s in new_sg_data.get('project').get('samples'):
        sg_ids: list = []
        for sg in s.get('sequencingGroups'):
            sg_ids.append(sg.get('id'))
        new_sg_map[s.get('externalId')] = sg_ids

    for s in samples:
        for sg in s['sequencingGroups']:
            _existing_sg = _get_existing_sg(
                existing_data, s.get('externalId'), sg.get('type')
            )
            _existing_sgid = _existing_sg.get('id') if _existing_sg else None
            for analysis in sg['analyses']:
                if analysis['type'] not in ['cram', 'gvcf']:
                    # Currently the create_test_subset script only handles crams or gvcf files.
                    continue

                _existing_analysis: dict = {}
                if _existing_sgid:
                    _existing_analysis = _get_existing_analysis(
                        existing_data,
                        s['externalId'],
                        _existing_sgid,
                        analysis['type'],
                    )
                _existing_analysis_id = (
                    _existing_analysis.get('id') if _existing_analysis else None
                )
                if _existing_analysis_id:
                    am = AnalysisUpdateModel(
                        type=analysis['type'],
                        output=_copy_files_in_dict(
                            analysis['output'],
                            project,
                            (str(sg['id']), new_sg_map[s['externalId']][0]),
                        ),
                        status=AnalysisStatus(analysis['status'].lower()),
                        sequencing_group_ids=new_sg_map[s['externalId']],
                        meta=analysis['meta'],
                    )
                    aapi.update_analysis(
                        analysis_id=_existing_analysis_id,
                        analysis_update_model=am,
                    )
                else:
                    am = Analysis(
                        type=analysis['type'],
                        output=_copy_files_in_dict(
                            analysis['output'],
                            project,
                            (str(sg['id']), new_sg_map[s['externalId']][0]),
                        ),
                        status=AnalysisStatus(analysis['status'].lower()),
                        sequencing_group_ids=new_sg_map[s['externalId']],
                        meta=analysis['meta'],
                    )

                    logger.info(f'Creating {analysis["type"]}analysis entry in test')
                    aapi.create_analysis(project=target_project, analysis=am)


def _get_existing_sample(data: dict, sample_id: str) -> dict:
    for sample in data.get('project').get('samples'):
        if sample.get('externalId') == sample_id:
            return sample

    return None


def _get_existing_sg(
    existing_data: dict, sample_id: str, sg_type: str = None, sg_id: str = None
) -> dict:
    if not sg_type and not sg_id:
        raise ValueError('Must provide sg_type or sg_id when getting exsisting sg')
    sample = _get_existing_sample(existing_data, sample_id)
    if sample:
        for sg in sample.get('sequencingGroups'):
            if sg_id and sg.get('id') == sg_id:
                return sg
            if sg_type and sg.get('type') == sg_type:
                return sg

    return None


def _get_existing_assay(
    data: dict, sample_id: str, sg_id: str, assay_type: str
) -> dict:
    sg = _get_existing_sg(
        existing_data=data,
        sample_id=sample_id,
        sg_id=sg_id,
    )
    for assay in sg.get('assays'):
        if assay.get('type') == assay_type:
            return assay

    return None


def _get_existing_analysis(
    data: dict, sample_id: str, sg_id: str, analysis_type: str
) -> dict:
    sg = _get_existing_sg(existing_data=data, sample_id=sample_id, sg_id=sg_id)
    for analysis in sg.get('analyses'):
        if analysis.get('type') == analysis_type:
            return analysis
    return None


def _get_sids_for_families(
    project: str,
    families_n: int,
    additional_families,
) -> list[str]:
    """Returns specific sequencing groups to be included in the test project."""

    included_sids: list = []
    _num_families_to_subset: int = None
    _randomly_selected_families: list = []

    # Case 1: If neither families_n nor _additional_families
    if not families_n and not additional_families:
        return included_sids

    # Case 2: If families_n but not _additional_families
    if families_n and not additional_families:
        _num_families_to_subset = families_n

    # Case 3: If both families_n and _additional_families
    if families_n and additional_families:
        _num_families_to_subset = families_n - len(additional_families)

    family_sgid_output = query(QUERY_FAMILY_SGID, {'project': project})

    # 1. Remove the families in _families_to_subset
    all_family_sgids = family_sgid_output.get('project').get('families')
    _filtered_family_sgids = [
        fam for fam in all_family_sgids if fam['externalId'] not in additional_families
    ]
    _user_input_families = [
        fam for fam in all_family_sgids if fam['externalId'] in additional_families
    ]

    # TODO: Replace this with the nice script that randomly selects better :)
    # 2. Randomly select _num_families_to_subset from the remaining families
    if _num_families_to_subset:
        _randomly_selected_families = random.sample(
            _filtered_family_sgids, _num_families_to_subset
        )

    # 3. Combine the families in _families_to_subset with the randomly selected families & return sequencing group ids

    _all_families_to_subset = _randomly_selected_families + _user_input_families

    for fam in _all_families_to_subset:
        for participant in fam['participants']:
            for sample in participant['samples']:
                included_sids.append(sample['id'])

    return included_sids


def transfer_families(
    initial_project: str, target_project: str, participant_ids: list[int]
) -> list[int]:
    """Pull relevant families from the input project, and copy to target_project"""
    families = fapi.get_families(
        project=initial_project,
        participant_ids=participant_ids,
    )

    family_ids = [family['id'] for family in families]

    tmp_family_tsv = 'tmp_families.tsv'
    family_tsv_headers = ['Family ID', 'Description', 'Coded Phenotype', 'Display Name']
    # Work-around as import_families takes a file.
    with open(tmp_family_tsv, 'wt') as tmp_families:
        tsv_writer = csv.writer(tmp_families, delimiter='\t')
        tsv_writer.writerow(family_tsv_headers)
        for family in families:
            tsv_writer.writerow(
                [
                    family['external_id'],
                    family['description'] or '',
                    family['coded_phenotype'] or '',
                ]
            )

    with open(tmp_family_tsv) as family_file:
        fapi.import_families(file=family_file, project=target_project)

    return family_ids


def transfer_ped(
    initial_project: str, target_project: str, family_ids: list[int]
) -> dict[str, int]:
    """Pull pedigree from the input project, and copy to target_project"""
    ped_tsv = fapi.get_pedigree(
        initial_project,
        export_type='tsv',
        internal_family_ids=family_ids,
    )
    tmp_ped_tsv = 'tmp_ped.tsv'
    # Work-around as import_pedigree takes a file.
    with open(tmp_ped_tsv, 'w') as tmp_ped:
        tmp_ped.write(ped_tsv)

    with open(tmp_ped_tsv) as ped_file:
        fapi.import_pedigree(
            file=ped_file,
            has_header=True,
            project=target_project,
            create_missing_participants=True,
        )

    # Get map of external participant id to internal
    participant_output = query(PARTICIPANT_QUERY, {'project': target_project})
    participant_map = {
        participant['externalId']: participant['id']
        for participant in participant_output.get('project').get('participants')
    }

    return participant_map


def transfer_participants(
    target_project: str,
    participant_data,
) -> dict[str, int]:
    """Transfers relevant participants between projects"""
    existing_participants = papi.get_participants(target_project)

    target_project_epids = [
        participant['external_id'] for participant in existing_participants
    ]

    participants_to_transfer = []
    for participant in participant_data:
        if participant['externalId'] not in target_project_epids:
            # Participants with id field will be updated & those without will be inserted
            del participant['id']
        # transfer_participant = {k: v for k, v in participant.items() if v is not None}
        transfer_participant = {
            'external_id': participant['externalId'],
            'meta': participant.get('meta') or {},
            'karyotype': participant.get('karyotype'),
            'reported_gender': participant.get('reportedGender'),
            'reported_sex': participant.get('reportedSex'),
            'id': participant.get('id'),
        }
        # Participants are being created before the samples are, so this will be empty for now.
        transfer_participant['samples'] = []
        participants_to_transfer.append(transfer_participant)

    upserted_participants = papi.upsert_participants(
        target_project, participant_upsert=participants_to_transfer
    )

    external_to_internal_participant_id_map: dict[str, int] = {}

    for participant in upserted_participants:
        external_to_internal_participant_id_map[
            participant['external_id']
        ] = participant['id']
    return external_to_internal_participant_id_map


def get_samples_for_families(project: str, additional_families: list[str]) -> list[str]:
    """Returns the samples that belong to a list of families"""

    full_pedigree = fapi.get_pedigree(
        project=project,
        replace_with_participant_external_ids=False,
        replace_with_family_external_ids=True,
    )

    ipids = [
        family['individual_id']
        for family in full_pedigree
        if family['family_id'] in additional_families
    ]

    sample_objects = sapi.get_samples(
        body_get_samples={
            'project_ids': [project],
            'participant_ids': ipids,
            'active': True,
        }
    )

    samples: list[str] = [sample['id'] for sample in sample_objects]

    return samples


def get_fams_for_samples(
    project: str,
    additional_samples: Optional[list[str]] = None,
) -> list[str]:
    """Returns the families that a list of samples belong to"""
    sample_objects = sapi.get_samples(
        body_get_samples={
            'project_ids': [project],
            'sample_ids': additional_samples,
            'active': True,
        }
    )

    pids = [sample['participant_id'] for sample in sample_objects]
    full_pedigree = fapi.get_pedigree(
        project=project,
        replace_with_participant_external_ids=False,
        replace_with_family_external_ids=True,
    )

    fams: set[str] = {
        fam['family_id'] for fam in full_pedigree if str(fam['individual_id']) in pids
    }

    return list(fams)


def _normalise_map(unformatted_map: list[list[str]]) -> dict[str, str]:
    """Input format: [[value1,key1,key2],[value2,key4]]
    Output format: {key1:value1, key2: value1, key3:value2}"""

    normalised_map = {}
    for group in unformatted_map:
        value = group[0]
        for key in group[1:]:
            normalised_map[key] = value

    return normalised_map


def _validate_opts(
    samples_n: int, families_n: int, noninteractive: bool
) -> tuple[Optional[int], Optional[int]]:
    """Validates the options passed to the script"""
    if samples_n is None and families_n is None:
        samples_n = DEFAULT_SAMPLES_N
        logger.info(
            f'Neither --samples nor --families specified, defaulting to selecting '
            f'{samples_n} samples'
        )

    return samples_n, families_n


def _print_fam_stats(families: list[dict[str, str]]):
    family_sizes = Counter([fam['family_id'] for fam in families])
    fam_by_size: CounterType[int] = Counter()
    # determine number of singles, duos, trios, etc
    for fam in family_sizes:
        fam_by_size[family_sizes[fam]] += 1
    for fam_size in sorted(fam_by_size):
        if fam_size == 1:
            label = 'singles'
        elif fam_size == 2:
            label = 'duos'
        elif fam_size == 3:
            label = 'trios'
        else:
            label = f'{fam_size} members'
        logger.info(f'  {label}: {fam_by_size[fam_size]}')


def _get_random_families(
    families: list[dict[str, str]],
    families_n: int,
    include_single_person_families: Optional[bool] = False,
) -> list[str]:
    """Obtains a subset of families, that are a little less random.
    By default single-person families are discarded.
    The function aims to evenly distribute the families chosen by size.
    For example, if the composition of families inputted is as follows
    Duos - 5 families, Trios - 10 families, Quads - 5 families
    and families_n = 4
    Then this function will randomly select, 1 duo, 1 quad, and 2 trios.
    """

    family_sizes = dict(Counter([fam['family_id'] for fam in families]))

    # Discard single-person families
    family_threshold = 0 if include_single_person_families else 1
    families_within_threshold = [
        k for k, v in family_sizes.items() if v > family_threshold
    ]

    # Get family size distribution, i.e. {1:[FAM1, FAM2], 2:[FAM3], 3:[FAM4,FAM5, FAM6]}
    distributed_by_size: dict[int, list[str]] = {}
    for k, v in family_sizes.items():
        if k in families_within_threshold:
            if distributed_by_size.get(v):
                distributed_by_size[v].append(k)
            else:
                distributed_by_size[v] = [k]

    sizes = len(list(distributed_by_size.keys()))
    returned_families: list[str] = []

    proportion = families_n / len(families_within_threshold)

    if sizes <= families_n:
        for _s, fams in distributed_by_size.items():
            n_pull = round(proportion * len(fams))
            returned_families.extend(random.sample(fams, n_pull))

    else:
        # we can't evenly distribute, so we'll just pull randomly
        returned_families = random.sample(families_within_threshold, families_n)

    return returned_families


def _copy_files_in_dict(d, dataset: str, sid_replacement: tuple[str, str] = None):
    """
    Replaces all `gs://cpg-{project}-main*/` paths
    into `gs://cpg-{project}-test*/` and creates copies if needed
    If `d` is dict or list, recursively calls this function on every element
    If `d` is str, replaces the path
    """
    if not d:
        return d
    if isinstance(d, str) and d.startswith(f'gs://cpg-{dataset}-main'):
        logger.info(f'Looking for analysis file {d}')
        old_path = d
        if not file_exists(old_path):
            logger.warning(f'File {old_path} does not exist')
            return d
        new_path = old_path.replace(
            f'gs://cpg-{dataset}-main', f'gs://cpg-{dataset}-test'
        )
        # Replace the internal sample ID from the original project
        # With the new internal sample ID from the test project
        if sid_replacement is not None:
            new_path = new_path.replace(sid_replacement[0], sid_replacement[1])

        if not file_exists(new_path):
            cmd = f'gsutil cp {old_path!r} {new_path!r}'
            logger.info(f'Copying file in metadata: {cmd}')
            subprocess.run(cmd, check=False, shell=True)
        extra_exts = ['.md5']
        if new_path.endswith('.vcf.gz'):
            extra_exts.append('.tbi')
        if new_path.endswith('.cram'):
            extra_exts.append('.crai')
        for ext in extra_exts:
            if file_exists(old_path + ext) and not file_exists(new_path + ext):
                cmd = f'gsutil cp {old_path + ext!r} {new_path + ext!r}'
                logger.info(f'Copying extra file in metadata: {cmd}')
                subprocess.run(cmd, check=False, shell=True)
        return new_path
    if isinstance(d, list):
        return [_copy_files_in_dict(x, dataset) for x in d]
    if isinstance(d, dict):
        return {k: _copy_files_in_dict(v, dataset) for k, v in d.items()}
    return d


def _pretty_format_samples(samples: list[dict[str, str]]) -> str:
    return ', '.join(f"{s['id']}/{s['external_id']}" for s in samples)


def file_exists(path: str) -> bool:
    """
    Check if the object exists, where the object can be:
        * local file
        * local directory
        * Google Storage object
    :param path: path to the file/directory/object
    :return: True if the object exists
    """
    if path.startswith('gs://'):
        bucket = path.replace('gs://', '').split('/')[0]
        path = path.replace('gs://', '').split('/', maxsplit=1)[1]
        gs = storage.Client()
        return gs.get_bucket(bucket).get_blob(path)
    return os.path.exists(path)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
