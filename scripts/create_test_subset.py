#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals

""" Example Invocation

analysis-runner \
--dataset acute-care --description "populate acute care test subset" --output-dir "acute-care-test" \
--access-level full \
scripts/create_test_subset.py --project acute-care --families 4

This example will populate acute-care-test with the metamist data for 4 families.
"""

import csv
import logging
import os
import random
import subprocess
from argparse import ArgumentParser
from collections import Counter

from google.cloud import storage

from metamist.apis import AnalysisApi, AssayApi, SampleApi, FamilyApi, ParticipantApi
from metamist.graphql import gql, query
from metamist.models import (
    AssayUpsert,
    SampleUpsert,
    Analysis,
    AnalysisStatus,
    AnalysisUpdateModel,
    SequencingGroupUpsert,
)


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

PARTICIPANT_QUERY = gql(
    """
    query ($project: String!) {
        project (externalId: $project) {
            participants {
                id
                externalId
            }
        }
    }
    """
)


def main(
    project: str,
    samples_n: int,
    families_n: int,
    additional_families: set[str],
    additional_samples: set[str],
    skip_ped: bool = True,
):
    """
    Script creates a test subset for a given project.
    A new project with a prefix -test is created, and for any files in sample/meta,
    sequence/meta, or analysis/output a copy in the -test namespace is created.
    """

    if not any([additional_families, additional_samples, samples_n, families_n]):
        raise ValueError('Come on, what exactly are you asking for?')

    # for reproducibility
    random.seed(42)

    # 1. Find and SG IDs to be moved by Family ID -test.
    if families_n or additional_families:
        additional_samples.update(
            get_sids_for_families(project, families_n, additional_families)
        )

    # 2. Get all sids in project.
    sid_output = query(SG_ID_QUERY, variables={'project': project})
    all_sids = {sid['id'] for sid in sid_output.get('project').get('samples')}

    # 3. Randomly select from the remaining sgs
    additional_samples.update(random.sample(all_sids - additional_samples, samples_n))

    # 4. Query all the samples from the selected sgs
    original_project_subset_data = query(
        QUERY_ALL_DATA, {'project': project, 'sids': list(additional_samples)}
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
    logging.info('Transferring samples, sequencing groups, and assays')
    for s in samples:
        sample_sgs: list[SequencingGroupUpsert] = []
        for sg in s.get('sequencingGroups'):
            sg_assays: list[AssayUpsert] = []
            existing_sg = get_existing_sg(
                existing_data, s.get('externalId'), sg.get('type')
            )
            _existing_sgid = existing_sg.get('id') if existing_sg else None
            for assay in sg.get('assays'):
                _existing_assay: dict[str, str] = {}
                if _existing_sgid:
                    _existing_assay = get_existing_assay(
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

        sample_type = None if s['type'] == 'None' else s['type']
        existing_sid: str | None = None
        existing_sample = get_existing_sample(existing_data, s['externalId'])
        if existing_sample:
            existing_sid = existing_sample['id']

        existing_pid: int | None = None
        if s['participant']:
            existing_pid = upserted_participant_map[s['participant']['externalId']]

        sample_upsert = SampleUpsert(
            external_id=s['externalId'],
            type=sample_type or None,
            meta=(copy_files_in_dict(s['meta'], project) or {}),
            participant_id=existing_pid,
            sequencing_groups=sample_sgs,
            id=existing_sid,
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
            existing_sg = get_existing_sg(
                existing_data, s.get('externalId'), sg.get('type')
            )
            existing_sgid = existing_sg.get('id') if existing_sg else None
            for analysis in sg['analyses']:
                if analysis['type'] not in ['cram', 'gvcf']:
                    # Currently the create_test_subset script only handles crams or gvcf files.
                    continue

                existing_analysis: dict = {}
                if existing_sgid:
                    existing_analysis = get_existing_analysis(
                        existing_data, s['externalId'], existing_sgid, analysis['type']
                    )
                existing_analysis_id = (
                    existing_analysis.get('id') if existing_analysis else None
                )
                if existing_analysis_id:
                    am = AnalysisUpdateModel(
                        type=analysis['type'],
                        output=copy_files_in_dict(
                            analysis['output'],
                            project,
                            (str(sg['id']), new_sg_map[s['externalId']][0]),
                        ),
                        status=AnalysisStatus(analysis['status'].lower()),
                        sequencing_group_ids=new_sg_map[s['externalId']],
                        meta=analysis['meta'],
                    )
                    aapi.update_analysis(
                        analysis_id=existing_analysis_id,
                        analysis_update_model=am,
                    )
                else:
                    am = Analysis(
                        type=analysis['type'],
                        output=copy_files_in_dict(
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


def get_existing_sample(data: dict, sample_id: str) -> dict | None:
    """
    Get the existing sample object for this ID
    Returns:
        The Sample dictionary, or None if unmatched
    """
    for sample in data.get('project', {}).get('samples', []):
        if sample.get('externalId') == sample_id:
            return sample

    return None


def get_existing_sg(
    existing_data: dict, sample_id: str, sg_type: str = None, sg_id: str = None
) -> dict | None:
    """
    Find a SG ID in the main data based on a sample ID
    Match either on CPG ID or type (exome/genome)
    Returns:
        The SG Data, or None if no match is found
    """
    if not sg_type and not sg_id:
        raise ValueError('Must provide sg_type or sg_id when getting exsisting sg')
    if sample := get_existing_sample(existing_data, sample_id):
        for sg in sample.get('sequencingGroups'):
            if sg_id and sg.get('id') == sg_id:
                return sg
            if sg_type and sg.get('type') == sg_type:
                return sg

    return None


def get_existing_assay(
    data: dict, sample_id: str, sg_id: str, assay_type: str
) -> dict | None:
    """
    Find assay in main data for this SGID
    Returns:
        The Assay Data, or None if no match is found
    """
    if sg := get_existing_sg(existing_data=data, sample_id=sample_id, sg_id=sg_id):
        for assay in sg.get('assays', []):
            if assay.get('type') == assay_type:
                return assay

    return None


def get_existing_analysis(
    data: dict, sample_id: str, sg_id: str, analysis_type: str
) -> dict | None:
    """
    Find the existing SG for this sample, then identify any relevant analysis objs
    Returns:
        an analysis dict, or None if the right type isn't found
    """
    if sg := get_existing_sg(existing_data=data, sample_id=sample_id, sg_id=sg_id):
        for analysis in sg.get('analyses', []):
            if analysis.get('type') == analysis_type:
                return analysis
    return None


def get_sids_for_families(
    project: str, families_n: int, additional_families: set[str]
) -> set[str]:
    """Returns specific sequencing groups to be included in the test project."""

    family_sgid_output = query(QUERY_FAMILY_SGID, {'project': project})

    all_family_sgids = family_sgid_output.get('project', {}).get('families', [])
    assert all_family_sgids, 'No families returned in GQL result'

    # 1. Remove the specifically requested families
    user_input_families = [
        fam for fam in all_family_sgids if fam['externalId'] in additional_families
    ]

    # TODO: Replace this with the nice script that randomly selects better :)
    # 2. Randomly select from the remaining families (families_n can be 0)
    user_input_families.extend(
        random.sample(
            [
                fam
                for fam in all_family_sgids
                if fam['externalId'] not in additional_families
            ],
            families_n,
        )
    )

    # 3. Pull SGs from random + specific families
    included_sids: set[str] = set()
    for fam in user_input_families:
        for participant in fam['participants']:
            for sample in participant['samples']:
                included_sids.add(sample['id'])

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
        transfer_participant = {
            'external_id': participant['externalId'],
            'meta': participant.get('meta') or {},
            'karyotype': participant.get('karyotype'),
            'reported_gender': participant.get('reportedGender'),
            'reported_sex': participant.get('reportedSex'),
            'id': participant.get('id'),
            'samples': [],
        }
        # Participants are being created before the samples are, so this will be empty for now.
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


def get_random_families(
    families: list[dict[str, str]],
    families_n: int,
    include_single_person_families: bool = False,
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


def copy_files_in_dict(d, dataset: str, sid_replacement: tuple[str, str] = None):
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
        return [copy_files_in_dict(x, dataset) for x in d]
    if isinstance(d, dict):
        return {k: copy_files_in_dict(v, dataset) for k, v in d.items()}
    return d


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
    parser = ArgumentParser(description='Argument parser for subset generator')
    parser.add_argument(
        '--project', required=True, help='The sample-metadata project ($DATASET)'
    )
    parser.add_argument('-n', type=int, help='# Random Samples to copy', default=0)
    parser.add_argument('-f', type=int, help='# Random families to copy', default=0)
    # Flag to be used when there isn't available pedigree/family information.
    parser.add_argument(
        '--skip-ped',
        action='store_true',
        help='Skip transferring pedigree/family information',
    )
    parser.add_argument(
        '--families',
        nargs='+',
        help='Additional families to include.',
        type=str,
        default={},
    )
    parser.add_argument(
        '--samples',
        nargs='+',
        help='Additional samples to include.',
        type=str,
        default={},
    )
    parser.add_argument(
        '--noninteractive', action='store_true', help='Skip interactive confirmation'
    )
    args, fail = parser.parse_known_args()
    if fail:
        parser.print_help()
        raise AttributeError(f'Invalid arguments: {fail}')

    main(
        project=args.project,
        samples_n=args.n,
        families_n=args.f,
        additional_samples=set(args.samples),
        additional_families=set(args.families),
        skip_ped=args.skip_ped,
    )
