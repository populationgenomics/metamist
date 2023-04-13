#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,too-many-arguments

""" Example Invocation

analysis-runner \
--dataset acute-care --description "populate acute care test subset" --output-dir "acute-care-test" \
--access-level full \
scripts/create_test_subset.py --project acute-care --families 4

This example will populate acute-care-test with the metamist data for 4 families.
"""

from typing import Optional, Counter as CounterType
import logging
import os
import random
import subprocess
import traceback
from collections import Counter
import csv

import click
from google.cloud import storage

from sample_metadata import exceptions
from sample_metadata.apis import (
    AnalysisApi,
    SequenceApi,
    SampleApi,
    FamilyApi,
    ParticipantApi,
)
from sample_metadata.models import (
    AnalysisType,
    NewSequence,
    NewSample,
    AnalysisModel,
    SampleUpdateModel,
    SequenceType,
    SampleType,
    SequenceStatus,
    SequenceTechnology,
    AnalysisStatus,
)

logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)

sapi = SampleApi()
aapi = AnalysisApi()
seqapi = SequenceApi()
fapi = FamilyApi()
papi = ParticipantApi()

DEFAULT_SAMPLES_N = 10


@click.command()
@click.option(
    '--project',
    required=True,
    help='The sample-metadata project ($DATASET)',
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

    all_samples = sapi.get_samples(
        body_get_samples={
            'project_ids': [project],
            'active': True,
        }
    )
    logger.info(f'Found {len(all_samples)} samples')
    if (samples_n and samples_n >= len(all_samples)) and not noninteractive:
        resp = str(
            input(
                f'Requesting {samples_n} samples which is >= '
                f'than the number of available samples ({len(all_samples)}). '
                f'The test project will be a copy of the production project. '
                f'Please confirm (y): '
            )
        )
        if resp.lower() != 'y':
            raise SystemExit()

    random.seed(42)  # for reproducibility

    pid_sid = papi.get_external_participant_id_to_internal_sample_id(project)
    sample_id_by_participant_id = dict(pid_sid)

    if families_n:
        if _additional_samples:
            _additional_families.extend(
                get_fams_for_samples(
                    project,
                    _additional_samples,
                )
            )
        fams = fapi.get_families(project)
        _additional_families_set = set(_additional_families)
        families_to_subset = [
            family['id']
            for family in fams
            if family['external_id'] not in _additional_families_set
        ]
        full_pedigree = fapi.get_pedigree(
            project=project, internal_family_ids=families_to_subset
        )
        external_family_ids = _get_random_families(full_pedigree, families_n)
        external_family_ids.extend(_additional_families)
        internal_family_ids = [
            fam['id'] for fam in fams if fam['external_id'] in external_family_ids
        ]
        pedigree = fapi.get_pedigree(
            project=project, internal_family_ids=internal_family_ids
        )
        _print_fam_stats(pedigree)

        p_ids = [ped['individual_id'] for ped in pedigree]
        sample_ids = [
            sample
            for (participant, sample) in sample_id_by_participant_id.items()
            if participant in p_ids
        ]
        sample_set = set(sample_ids)
        samples = [s for s in all_samples if s['id'] in sample_set]

    else:
        if _additional_families:
            _additional_samples.extend(
                get_samples_for_families(project, _additional_families)
            )

        _additional_samples_set = set(_additional_samples)
        samples_to_subset = [
            sample
            for sample in all_samples
            if sample['id'] not in _additional_samples_set
        ]
        samples_to_add = [
            sample for sample in all_samples if sample['id'] in _additional_samples_set
        ]
        samples = random.sample(list(samples_to_subset), samples_n)
        samples.extend(samples_to_add)
        sample_ids = [s['id'] for s in samples]

    logger.info(
        f'Subset to {len(samples)} samples (internal ID / external ID): '
        f'{_pretty_format_samples(samples)}'
    )

    # Populating test project
    target_project = project + '-test'
    logger.info('Checking any existing test samples in the target test project')

    test_sample_by_external_id = _process_existing_test_samples(target_project, samples)

    try:
        seq_infos: list[dict[str, str]] = seqapi.get_sequences_by_sample_ids(sample_ids)
    except exceptions.ApiException:
        seq_info_by_s_id = {}
    else:
        seq_info_by_s_id = {seq['sample_id']: seq for seq in seq_infos}

    analysis_by_sid_by_type: dict[str, dict[str, dict[str, str]]] = {
        'cram': {},
        'gvcf': {},
    }
    for a_type, analysis_by_sid in analysis_by_sid_by_type.items():
        try:
            analyses: list[
                dict[str, str]
            ] = aapi.get_latest_analysis_for_samples_and_type(
                project=project,
                analysis_type=AnalysisType(a_type),
                request_body=sample_ids,
            )
        except exceptions.ApiException:
            traceback.print_exc()
        else:
            for a in analyses:
                analysis_by_sid[a['sample_ids'][0]] = a
        logger.info(f'Will copy {a_type} analysis entries: {analysis_by_sid}')

    # Parse Families & Participants
    participant_ids = [int(sample['participant_id']) for sample in samples]
    if skip_ped:
        # If no family data is available, only the participants should be transferred.
        external_participant_ids = transfer_participants(
            initial_project=project,
            target_project=target_project,
            participant_ids=participant_ids,
        )

    else:
        family_ids = transfer_families(project, target_project, participant_ids)
        external_participant_ids = transfer_ped(project, target_project, family_ids)

    external_sample_internal_participant_map = get_map_ipid_esid(
        project, target_project, external_participant_ids
    )

    new_sample_map = {}

    for s in samples:
        logger.info(f'Processing sample {s["id"]}')

        if s['external_id'] in test_sample_by_external_id:
            new_s_id = test_sample_by_external_id[s['external_id']]['id']
            logger.info(f'Sample already in test project, with ID {new_s_id}')
            new_sample_map[s['id']] = new_s_id

        else:
            logger.info('Creating test sample entry')
            new_s_id = sapi.create_new_sample(
                project=target_project,
                new_sample=NewSample(
                    external_id=s['external_id'],
                    type=SampleType(s['type']),
                    meta=(_copy_files_in_dict(s['meta'], project) or {}),
                    participant_id=external_sample_internal_participant_map[
                        s['external_id']
                    ],
                ),
            )
            new_sample_map[s['id']] = new_s_id

            seq_info = seq_info_by_s_id.get(s['id'])
            if seq_info:
                logger.info('Processing sequence entry')
                new_meta = _copy_files_in_dict(seq_info.get('meta'), project)
                logger.info('Creating sequence entry in test')
                seqapi.create_new_sequence(
                    new_sequence=NewSequence(
                        sample_id=new_s_id,
                        meta=new_meta,
                        type=SequenceType(seq_info['type']),
                        external_ids=seq_info['external_ids'],
                        status=SequenceStatus(seq_info['status']),
                        technology=SequenceTechnology(seq_info['technology']),
                    ),
                )

        for a_type in ['cram', 'gvcf']:
            analysis = analysis_by_sid_by_type[a_type].get(s['id'])
            if analysis:
                logger.info(f'Processing {a_type} analysis entry')
                am = AnalysisModel(
                    type=AnalysisType(a_type),
                    output=_copy_files_in_dict(
                        analysis['output'],
                        project,
                        (str(s['id']), new_sample_map[s['id']]),
                    ),
                    status=AnalysisStatus(analysis['status']),
                    sample_ids=[new_sample_map[s['id']]],
                    meta=analysis['meta'],
                )
                logger.info(f'Creating {a_type} analysis entry in test')
                aapi.create_new_analysis(project=target_project, analysis_model=am)
        logger.info(f'-')


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
) -> list[str]:
    """Pull pedigree from the input project, and copy to target_project"""
    ped_tsv = fapi.get_pedigree(
        initial_project,
        export_type='tsv',
        internal_family_ids=family_ids,
    )
    ped_json = fapi.get_pedigree(
        initial_project,
        export_type='json',
        internal_family_ids=family_ids,
    )

    external_participant_ids = [ped['individual_id'] for ped in ped_json]
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

    return external_participant_ids


def transfer_participants(
    initial_project: str, target_project: str, participant_ids: list[int]
) -> list[str]:
    """Transfers relevant participants between projects"""

    current_participants = papi.get_participants(
        initial_project,
        body_get_participants={'internal_participant_ids': participant_ids},
    )
    existing_participants = papi.get_participants(target_project)

    target_project_epids = [
        participant['external_id'] for participant in existing_participants
    ]

    participants_to_transfer = []
    for participant in current_participants:
        if participant['external_id'] not in target_project_epids:
            # Participants with id field will be updated & those without will be inserted
            del participant['id']
        transfer_participant = {k: v for k, v in participant.items() if v is not None}
        # Participants are being created before the samples are, so this will be empty for now.
        transfer_participant['samples'] = []
        participants_to_transfer.append(transfer_participant)

    upserted_participants = papi.batch_upsert_participants(
        target_project, {'participants': participants_to_transfer}
    )
    return list(upserted_participants.keys())


def get_map_ipid_esid(
    project: str, target_project: str, external_participant_ids: list[str]
) -> dict[str, str]:
    """Intermediate steps to determine the mapping of esid to ipid
    Acronyms
    ep : external participant id
    ip : internal participant id
    es : external sample id
    is : internal sample id
    """

    # External PID: Internal PID
    ep_ip_map = papi.get_participant_id_map_by_external_ids(
        target_project, request_body=external_participant_ids
    )

    # External PID : Internal SID
    ep_is_map = papi.get_external_participant_id_to_internal_sample_id(project)

    # Internal PID : Internal SID
    ip_is_map = []
    for ep_is_pair in ep_is_map:
        if ep_is_pair[0] in ep_ip_map:
            ep_is_pair[0] = ep_ip_map[ep_is_pair[0]]
            ip_is_map.append(ep_is_pair)

    # Internal PID : External SID
    is_es_map = sapi.get_all_sample_id_map_by_internal(project)

    ip_es_map = []
    for ip_is_pair in ip_is_map:
        samples_per_participant = [ip_is_pair[0]]
        for isid in ip_is_pair[1:]:
            if isid in is_es_map:
                samples_per_participant.append(is_es_map[isid])
        ip_es_map.append(samples_per_participant)

    # External SID : Internal PID (Normalised)
    external_sample_internal_participant_map = _normalise_map(ip_es_map)

    return external_sample_internal_participant_map


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
    if samples_n is not None and families_n is not None:
        raise click.BadParameter('Please specify only one of --samples or --families')

    if samples_n is None and families_n is None:
        samples_n = DEFAULT_SAMPLES_N
        logger.info(
            f'Neither --samples nor --families specified, defaulting to selecting '
            f'{samples_n} samples'
        )

    if samples_n is not None and samples_n < 1:
        raise click.BadParameter('Please specify --samples higher than 0')

    if families_n is not None and families_n < 1:
        raise click.BadParameter('Please specify --families higher than 0')

    if (families_n is not None and families_n >= 30) and not noninteractive:
        resp = str(
            input(
                f'You requested a subset of {families_n} families. '
                f'Please confirm (y): '
            )
        )
        if resp.lower() != 'y':
            raise SystemExit()

    if (samples_n is not None and samples_n >= 100) and not noninteractive:
        resp = str(
            input(
                f'You requested a subset of {samples_n} samples. '
                f'Please confirm (y): '
            )
        )
        if resp.lower() != 'y':
            raise SystemExit()
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


def _process_existing_test_samples(
    test_project: str, samples: list[dict[str, str]]
) -> dict[str, dict[str, str]]:
    """
    Removes samples that need to be removed and returns those that need to be kept
    """
    test_samples = sapi.get_samples(
        body_get_samples={
            'project_ids': [test_project],
            'active': True,
        }
    )
    external_ids = [s['external_id'] for s in samples]
    test_samples_to_remove = [
        s for s in test_samples if s['external_id'] not in external_ids
    ]
    test_samples_to_keep = [s for s in test_samples if s['external_id'] in external_ids]
    if test_samples_to_remove:
        logger.info(
            f'Removing test samples: {_pretty_format_samples(test_samples_to_remove)}'
        )
        for s in test_samples_to_remove:
            sapi.update_sample(s['id'], SampleUpdateModel(active=False))

    if test_samples_to_keep:
        logger.info(
            f'Test samples already exist: {_pretty_format_samples(test_samples_to_keep)}'
        )

    return {s['external_id']: s for s in test_samples_to_keep}


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
