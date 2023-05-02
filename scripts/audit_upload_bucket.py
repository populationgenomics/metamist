#!/usr/bin/env python
"""
What's happening here?
1. Input a Metamist project
2. Get all the samples and sequences for this project from Metamist
3. Check the bucket for all sequence data, and compare to the data in the metamist sequences
    - If there are discrepencies, check if the file name and size is the same in the bucket as it is in metamist
      - If they are both the same, assume this file has just been moved to a different location in the bucket
      - Note the link between the Metamist sequence ID and this new bucket location
      - Note the link between the Metamist sequence ID and the Sample ID

4. Check if a sample has a completed cram - if so, its sequence data can be removed
5. Remove the sequence data for samples with completed crams, including those whose data has been moved
"""

from collections import defaultdict, namedtuple
import csv
from datetime import datetime
import sys
import logging
import os
from typing import Any, Type, TypeVar
import click

from cloudpathlib import AnyPath
from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import get_config
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from sample_metadata.apis import (
    ParticipantApi,
    AnalysisApi,
    SampleApi,
    SequenceApi,
)
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.graphql import query

FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
READ_EXTENSIONS = FASTQ_EXTENSIONS + BAM_EXTENSIONS + CRAM_EXTENSIONS
ALL_EXTENSIONS = (
    FASTQ_EXTENSIONS
    + BAM_EXTENSIONS
    + CRAM_EXTENSIONS
    + GVCF_EXTENSIONS
    + VCF_EXTENSIONS
)

FILE_TYPES_MAP = {
    'fastq': FASTQ_EXTENSIONS,
    'bam': BAM_EXTENSIONS,
    'cram': CRAM_EXTENSIONS,
    'gvcf': GVCF_EXTENSIONS,
    'vcf': VCF_EXTENSIONS,
    'all_reads': READ_EXTENSIONS,
    'all': ALL_EXTENSIONS,
}

SEQUENCE_TYPES_MAP = {
    'genome': [
        'genome',
    ],
    'exome': [
        'exome',
    ],
    'all': [
        'genome',
        'exome',
    ],
}

QUERY = """
query ProjectData($projectName: String!) {
    project(name: $projectName) {
        participants {
            externalId
            id
            samples {
                id
                externalId
                type
                sequences {
                    id
                    meta
                    type
                }
            }
        }
    }
}
"""

ANALYSIS_TYPES = [
    'qc',
    'joint-calling',
    'gvcf',
    'cram',
    'custom',
    'es-index',
    'sv',
    'web',
    'analysis-runner',
]

BUCKET_TYPES = [
    'main',
    'test',
    'archive',
    'release',
]

EXCLUDED_SAMPLES = [
    'CPG11783',  # acute-care, no FASTQ data
    'CPG13409',  # perth-neuro, coverage ~0x
    'CPG243717',  # validation, NA12878_KCCG low coverage https://main-web.populationgenomics.org.au/validation/qc/cram/multiqc.html,
    'CPG246645',  # ag-hidden, eof issue  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/440
    'CPG246678',  # ag-hidden, diff fastq size  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/446
    'CPG261792',  # rdp-kidney misformated fastq - https://batch.hail.populationgenomics.org.au/batches/378736/jobs/43
    # acute care fasq parsing errors https://batch.hail.populationgenomics.org.au/batches/379303/jobs/24
    'CPG259150',
    'CPG258814',
    'CPG258137',
    'CPG258111',
    'CPG258012',
    # ohmr4 cram parsing in align issues
    'CPG261339',
    'CPG261347',
    # IBMDX truncated sample? https://batch.hail.populationgenomics.org.au/batches/422181/jobs/99
    'CPG265876',
]

AAPI = AnalysisApi()
PAPI = ParticipantApi()
SAPI = SampleApi()
SEQAPI = SequenceApi()
CLIENT = storage.Client()

Sequence_Report_Entry = namedtuple(
    'Sequence_Report_Entry', 'Sample_ID Sequence_ID Sequence_File_Path Analysis_IDs'
)
Completed_Analysis = namedtuple(
    'Completed_Analysis',
    'Analysis_ID Analysis_Type Analysis_Output Timestamp_Completed',
)
TBucket = TypeVar('TBucket', bound=Bucket)


def get_bucket_subdirs_to_search(paths: list[str]) -> defaultdict[str, list]:
    """
    Takes a list of paths and extracts the bucket name and subdirectory, returning all unique pairs
    of buckets/subdirectories
    """
    buckets_subdirs_to_search = defaultdict(list)  # type: defaultdict[str,list]
    for path in paths:
        try:
            pc = get_path_components_from_gcp_path(path)
        except ValueError:
            logging.warning(f'{path} invalid')
            continue
        bucket = pc['bucket']
        subdir = pc['suffix']
        if subdir and subdir not in buckets_subdirs_to_search[bucket]:
            buckets_subdirs_to_search[bucket].append(subdir)

    return buckets_subdirs_to_search


def get_paths_for_subdir(bucket_name, subdirectory, file_extension):
    """Iterate through a gcp bucket/subdir and get all the blobs with the specified file extension(s)"""
    if isinstance(file_extension, list) and len(file_extension) == 1:
        file_extension = file_extension[0]
    files_in_bucket_subdir = []
    for blob in CLIENT.list_blobs(bucket_name, prefix=subdirectory, delimiter='/'):
        # Check if file ends with specified analysis type
        if not blob.name.endswith(file_extension):
            continue
        files_in_bucket_subdir.append(f'gs://{bucket_name}/{blob.name}')

    return files_in_bucket_subdir


def find_files_in_buckets_subdirs(
    buckets_subdirs: defaultdict[str, list], file_types: list[str]
):
    """
    Takes a list of (bucket,subdirectory) tuples and finds all the files contained in that directory
    with filetypes defined with an input list
    """
    files_in_bucket = []
    for bucket, subdirs in buckets_subdirs.items():
        for subdir in subdirs:
            # matrixtable / hailtable subdirectories should not appear in main-upload buckets,
            # but handle them just in case. They are too large to search
            if '.mt' in subdir or '.ht' in subdir:
                continue
            files_in_bucket.extend(get_paths_for_subdir(bucket, subdir, file_types))

    return files_in_bucket


def find_sequence_files_in_bucket(
    bucket_name: str, file_extensions: tuple[str]
) -> list[str]:
    """Gets all the gs paths to fastq files in the projects upload bucket"""
    sequence_paths = []
    if 'upload' not in bucket_name:
        # No prefix means it will get all blobs in the bucket (regardless of path)
        # This can be a dangerous call outside of the upload buckets
        raise NameError(
            'Call to list_blobs without prefix only valid for upload buckets'
        )

    for blob in CLIENT.list_blobs(bucket_name, prefix=''):
        if blob.name.endswith(file_extensions):
            sequence_paths.append(f'gs://{bucket_name}/{blob.name}')
        continue

    return sequence_paths


def get_filesize_from_path(bucket_name: str, bucket: Type[TBucket], path: str) -> int:
    """Extracts file size from gs path"""
    blob_path = path.removeprefix(f'gs://{bucket_name}/')
    blob = bucket.get_blob(blob_path)

    return blob.size


def get_project_participant_data(project_name: str):
    """
    Uses a graphQL query to return all participants in a Metamist project.
    Nested in the return are all samples associated with the participants,
    and then all the sequences associated with those samples.
    """

    return (
        query(QUERY, {'projectName': project_name}).get('project').get('participants')
    )


def map_participants_to_samples(participants: list[dict]) -> dict[str, list]:
    """
    Returns the {external_participant_id : sample_internal_id} mapping for a Metamist project
     - Also reports if any participants have more than one sample
    """

    # Create mapping of participant external ID to sample ID
    participant_sample_id_map = {
        participant.get('externalId'): [
            sample.get('id') for sample in participant.get('samples')
        ]
        for participant in participants
    }

    # Create mapping of participant external ID to internal ID
    participant_eid_to_iid = {
        participant.get('externalId'): participant.get('id')
        for participant in participants
    }

    # Report if any participants have more than one sample ID
    for participant_eid, sample_ids in participant_sample_id_map.items():
        if len(sample_ids) > 1:
            logging.info(
                f'Participant {participant_eid_to_iid[participant_eid]} (external ID: {participant_eid})'
                + f' associated with more than one sample id: {sample_ids}'
            )

    return participant_sample_id_map


def get_internal_to_external_sample_map_for_participants(
    participants: list[dict],
) -> dict[str, str]:
    """
    Returns the {internal sample ID : external sample ID} mapping for all participants in a Metamist project
    Also removes any samples that are part of the exclusions list
    """
    # Create a list of dictionaries, each mapping a sample ID to its external ID
    samples_all = [
        {
            sample.get('id'): sample.get('externalId')
            for sample in participant.get('samples')
        }
        for participant in participants
    ]

    # Squish them into a single dictionary not a list of dictionaries
    sample_internal_external_map = {
        sample_id: sample_external_id
        for sample_map in samples_all
        for sample_id, sample_external_id in sample_map.items()
        if sample_id not in EXCLUDED_SAMPLES
    }

    return sample_internal_external_map


def get_sequences_from_participants(
    participants: list[dict], sequence_type
) -> tuple[dict[int, str], defaultdict[int, list]]:
    """Return latest sequence ids for internal sample ids"""
    all_sequences = [
        {
            sample.get('id'): sample.get('sequences')
            for sample in participant.get('samples')
        }
        for participant in participants
    ]

    return get_sequence_mapping(all_sequences, sequence_type)


def get_sequence_mapping(
    all_sequences: list[dict], sequence_type: list[str]
) -> tuple[dict[int, str], defaultdict[int, list]]:
    """
    Input: A list of Metamist sequences
    Returns dict mappings of {sequence_id : sample_internal_id}, and {sequence_id : (read_locations, read_sizes)}
    """
    seq_id_sample_id_map = {}
    # multiple reads per sequence is possible so use defaultdict(list)
    sequence_reads = defaultdict(list)
    for samplesequence in all_sequences:  # pylint: disable=R1702
        sample_internal_id = list(samplesequence.keys())[0]  # Extract the sample ID
        for sequences in samplesequence.values():
            for sequence in sequences:
                if not sequence.get('type').lower() in sequence_type:
                    continue
                meta = sequence.get('meta')
                reads = meta.get('reads')
                seq_id_sample_id_map[sequence.get('id')] = sample_internal_id
                if not reads:
                    logging.info(f'Sample {sample_internal_id} has no sequence reads')
                    continue
                # support up to three levels of nesting, because:
                #
                #   - reads is typically an array
                #       - handle the case where it's not an array
                #   - fastqs exist in pairs.
                # eg:
                #   - reads: cram   # this shouldn't happen, but handle it anyway
                #   - reads: [cram1, cram2]
                #   - reads: [[fq1_forward, fq1_back], [fq2_forward, fq2_back]]
                if isinstance(reads, dict):
                    sequence_reads[sequence.get('id')].append(
                        (reads.get('location'), reads.get('size'))
                    )
                    continue
                if not isinstance(reads, list):
                    logging.error(f'Invalid read type: {type(reads)}: {reads}')
                    continue
                for read in reads:
                    if isinstance(read, list):
                        for inner_read in read:
                            if not isinstance(inner_read, dict):
                                logging.error(
                                    f'Got {type(inner_read)} read, expected dict: {inner_read}'
                                )
                                continue
                            sequence_reads[sequence.get('id')].append(
                                (inner_read.get('location'), inner_read.get('size'))
                            )
                    else:
                        if not isinstance(read, dict):
                            logging.error(
                                f'Got {type(inner_read)} read, expected dict: {inner_read}'
                            )
                            continue
                        sequence_reads[sequence.get('id')].append(
                            (read.get('location'), read.get('size'))
                        )

    return seq_id_sample_id_map, sequence_reads


def get_analysis_cram_paths_for_project_samples(
    project: str,
    sample_internal_to_external_id_map: dict[str, str],
    sequence_type: list[str],
) -> defaultdict[str, dict[int, str]]:
    """
    Queries all analyses for the list of samples in the given project AND the seqr project.
    Returns a dict mapping {sample_id : (analysis_id, cram_path) }
    """
    sample_ids = list(sample_internal_to_external_id_map.keys())
    projects = [
        project,
    ]
    if 'test' not in project:
        projects = [
            f'{project}',
            'seqr',  # Many analysis objects hiding out in seqr project
        ]
    analyses = AAPI.query_analyses(
        AnalysisQueryModel(
            projects=projects,
            type=AnalysisType('cram'),
            status=AnalysisStatus('completed'),  # Only interested in aligned crams
            sample_ids=sample_ids,
        )
    )

    # Report any crams missing the sequence type
    crams_with_missing_seq_type = [
        analysis.get('id')
        for analysis in analyses
        if 'sequencing_type' not in analysis['meta']
    ]
    if crams_with_missing_seq_type:
        raise ValueError(
            f'CRAMs from dataset {project} did not have sequencing_type: {crams_with_missing_seq_type}'
        )

        # logging.info(
        #     'Assuming CRAM sequencing type based on output path containing /exome/ or not...'
        # )
        # # Assign sequencing types to the crams based on the output path
        # for i, analysis in enumerate(analyses):
        #     if analysis.get('id') in crams_with_missing_seq_type:
        #         exome = 'exome' in analysis.get('output')
        #         if not exome:
        #             analysis['meta']['sequencing_type'] = 'genome'
        #             analyses[i] = analysis
        #             continue
        #         analysis['meta']['sequencing_type'] = 'exome'
        #         analyses[i] = analysis

    # Filter the analyses based on input sequence type
    analyses = [
        analysis
        for analysis in analyses
        if analysis.get('meta')['sequencing_type'] in sequence_type
    ]

    # For each sample ID, collect the analysis IDs and cram paths
    sample_cram_paths: defaultdict[str, dict[int, str]] = defaultdict(dict)
    for analysis in analyses:
        # Check the analysis output path is a valid gs path to a .cram file
        if not analysis['output'].startswith('gs://') and analysis['output'].endswith(
            'cram'
        ):
            logging.info(
                f'Analysis {analysis["id"]} invalid output path: {analysis["output"]}'
            )
            continue
        try:
            # Try getting the sample ID from the 'sample_ids' field
            for sample in analysis.get('sample_ids'):
                sample_id = sample
        except KeyError:
            # Try getting the sample ID from the 'meta' field's 'sample' field
            sample_id = analysis.get('meta')['sample']
            logging.warning(
                f'Analysis: {analysis.get("id")} missing "sample_ids" field. Using analysis["meta"].get("sample") instead.'
            )

        sample_cram_paths[sample_id].update(
            [(analysis.get('id'), analysis.get('output'))]
        )

    return sample_cram_paths


def get_complete_and_incomplete_samples(
    project: str,
    sample_internal_to_external_id_map: dict[str, str],
    sample_cram_paths: dict[str, dict[int, str]],
) -> dict[str, Any]:
    """
    Returns a dictionary containing two categories of samples:
     - the completed samples which have finished aligning and have a cram, as a dict mapping
        the sample_id to the analysis IDs
     - the incomplete samples where the alignment hasn't completed and no cram exists, as a list
    """
    # Get all the unique cram paths to check
    cram_paths = set()
    for analyses in sample_cram_paths.values():
        cram_paths.update(list(analyses.values()))

    # Check the analysis paths actually point to crams that exist in the bucket
    buckets_subdirs = get_bucket_subdirs_to_search(list(cram_paths))
    crams_in_bucket = find_files_in_buckets_subdirs(
        buckets_subdirs,
        [
            'cram',
        ],
    )

    # Incomplete samples initialised as the list of samples without a valid analysis cram output path
    incomplete_samples = set(sample_internal_to_external_id_map.keys()).difference(
        set(sample_cram_paths.keys())
    )

    # Completed samples are those whose completed cram output path exists in the bucket at the same path
    completed_samples = defaultdict(list)
    for sample_internal_id, analyses in sample_cram_paths.items():
        for analysis_id, cram_path in analyses.items():
            if cram_path in crams_in_bucket:
                completed_samples[sample_internal_id].append(analysis_id)
                continue
            incomplete_samples.update(sample_internal_id)

    if (
        incomplete_samples
    ):  # Report on samples without CRAMs especially if other analysis types have been created
        logging.info(f'Samples without CRAMs found: {list(incomplete_samples)}')
        analyses_for_samples_without_crams(project, list(incomplete_samples))

    return {'complete': completed_samples, 'incomplete': list(incomplete_samples)}


def analyses_for_samples_without_crams(project: str, samples_without_crams: list[str]):
    """Checks if other completed analyses exist for samples without completed crams"""
    projects = [
        project,
    ]
    if 'test' not in project:
        projects = [
            f'{project}',
            'seqr',  # Many analysis objects hiding out in seqr project
        ]
    all_sample_analyses: defaultdict[
        str, list[tuple[int, AnalysisType, str, str]]
    ] = defaultdict()
    for analysis_type in ANALYSIS_TYPES:
        analyses = AAPI.query_analyses(
            AnalysisQueryModel(
                projects=projects,
                type=AnalysisType(analysis_type),
                status=AnalysisStatus('completed'),  # Only interested in aligned crams
                sample_ids=samples_without_crams,
            )
        )
        for analysis in analyses:
            try:
                # Try getting the sample ID from the 'sample_ids' field
                for sample in analysis.get('sample_ids'):
                    sample_id = sample
            except KeyError:
                # Try getting the sample ID from the 'meta' field's 'sample' field
                sample_id = analysis.get('meta')['sample']

            analysis_entry = Completed_Analysis(
                analysis.get('id'),
                analysis.get('type'),
                analysis.get('output'),
                analysis.get('timestampCompleted'),
            )
            all_sample_analyses[sample_id].append(analysis_entry)

    if all_sample_analyses:
        for sample_without_cram, completed_analyses in all_sample_analyses.items():
            for completed_analysis in completed_analyses:
                logging.warning(
                    f'{sample_without_cram} missing CRAM but has analysis {completed_analysis}'
                )


def get_reads_to_delete_or_ingest(
    project: str,
    completed_samples: defaultdict[str, list[int]],
    sequence_reads: defaultdict[int, list],
    sample_id_seq_id_map: dict[str, list[int]],
    seq_id_sample_id_map_map: dict[int, str],
    file_types: tuple[str],
) -> tuple[list, list]:
    """
    Inputs: 1. Metamist project name
            2. List of samples which have completed CRAMs
            3. Dictionary mapping of sequence IDs to read paths and read file sizes
            4. Dictionary mapping sample IDs to sequence IDs
            5. Dictionary mapping sequence_ids to sample_ids
            6. The sequence file types to search for in the bucket
    Returns a tuple of two lists, each containing sequences IDs and sequence file paths.
    The first containins reads which can be deleted, the second containing reads to ingest.
    Include the sample ID and analysis ID in the reads to delete list.
    """

    # Check for uningested sequence data that may be hiding or sequence data that has been moved
    (
        reads_to_ingest,
        moved_sequences_to_delete,
    ) = check_for_uningested_or_moved_sequences(
        project,
        sequence_reads,
        completed_samples,
        seq_id_sample_id_map_map,
        file_types,
    )

    sequence_reads_to_delete = []
    for sample_id, analysis_ids in completed_samples.items():
        if sample_id in EXCLUDED_SAMPLES:
            continue
        seq_ids = sample_id_seq_id_map[sample_id]
        for seq_id in seq_ids:
            seq_read_paths = [reads_sizes[0] for reads_sizes in sequence_reads[seq_id]]
            for path in seq_read_paths:
                sequence_reads_to_delete.append(
                    Sequence_Report_Entry(sample_id, seq_id, path, analysis_ids)
                )

    reads_to_delete = sequence_reads_to_delete + moved_sequences_to_delete

    return reads_to_delete, reads_to_ingest


def check_for_uningested_or_moved_sequences(  # pylint: disable=R0914
    project: str,
    sequence_reads: defaultdict[int, list],
    completed_samples: defaultdict[str, list[int]],
    seq_id_sample_id_map_map: dict[int, str],
    file_extensions: tuple[str],
):
    """
    Compares the sequences in a Metamist project to the sequences in the main-upload bucket

    Input: The project name, the {sequence_id : read_paths} mapping, and the sequence file types
    Returns: 1. Paths to sequences that have not yet been ingested.
             2. A dict mapping sequence IDs to GS filepaths that have been ingested,
                but where the path in the bucket is different to the path for this
                sequence in Metamist. If the filenames and file sizes match,
                these are identified as sequence files that have been moved from
                their original location to a new location.
    """
    bucket_name = f'cpg-{project}-upload'
    if 'test' not in project:
        bucket_name = f'cpg-{project}-main-upload'
    bucket = CLIENT.get_bucket(bucket_name)

    # Get all the paths to sequence data anywhere in the main-upload bucket
    sequence_paths_in_bucket = find_sequence_files_in_bucket(
        bucket_name, file_extensions
    )

    # Flatten all the Metamist sequence file paths and sizes into a single list
    sequence_paths_sizes_in_metamist: list[tuple[str, int]] = []
    for sequence in sequence_reads.values():
        sequence_paths_sizes_in_metamist.extend(sequence)

    # Find the paths that exist in the bucket and not in metamist
    sequence_paths_in_metamist = [
        path_size[0] for path_size in sequence_paths_sizes_in_metamist
    ]
    uningested_sequence_paths = set(sequence_paths_in_bucket).difference(
        set(sequence_paths_in_metamist)
    )

    # Strip the metamist paths into just filenames
    # Map each file name to its file size and path
    sequence_files_sizes_in_metamist = {
        os.path.basename(path[0]): path[1] for path in sequence_paths_sizes_in_metamist
    }
    sequence_files_paths_in_metamist = {
        os.path.basename(path[0]): path[0] for path in sequence_paths_sizes_in_metamist
    }

    # Identify if any paths are to files that have actually just been moved by checking if they are in the bucket but not metamist
    ingested_and_moved_filepaths = []
    for path in uningested_sequence_paths:
        filename = os.path.basename(path)
        # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
        if filename in sequence_files_sizes_in_metamist.keys():
            filesize = get_filesize_from_path(bucket_name, bucket, path)
            if filesize == sequence_files_sizes_in_metamist.get(filename):
                ingested_and_moved_filepaths.append(
                    (path, sequence_files_paths_in_metamist.get(filename))
                )
    logging.info(
        f'Found {len(ingested_and_moved_filepaths)} ingested files that have been moved'
    )

    # If the file has just been moved, we consider it ingested
    uningested_sequence_paths -= {
        bucket_path for bucket_path, _ in ingested_and_moved_filepaths
    }

    # flip the sequence id : reads mapping to identify sequence IDs by their read paths
    reads_sequences = {}
    for sequence_id, reads_sizes in sequence_reads.items():
        for read_size in reads_sizes:
            reads_sequences[read_size[0]] = sequence_id

    # Collect the sequences for files that have been ingested and moved to a different bucket location
    sequences_moved_paths = []
    for bucket_path, metamist_path in ingested_and_moved_filepaths:
        seq_id = reads_sequences.get(metamist_path)
        sample_id = seq_id_sample_id_map_map.get(seq_id)
        analysis_ids = completed_samples.get(sample_id)
        if sample_id not in EXCLUDED_SAMPLES:
            sequences_moved_paths.append(
                Sequence_Report_Entry(sample_id, seq_id, bucket_path, analysis_ids)
            )

    return uningested_sequence_paths, sequences_moved_paths


def write_csv_report_to_cloud(
    data_to_write: list[Any], report_path: AnyPath, header_row: list[str] | None
):
    """Writes a csv report to the cloud bucket containing the data to write at the report path with an optional header row"""
    with AnyPath(report_path).open('w+') as f:  # pylint: disable=E1101
        writer = csv.writer(f)
        if header_row:
            writer.writerow(header_row)
        for row in data_to_write:
            writer.writerow(row)

    logging.info('Wrote {len(data_to_write)} lines to report: {report_path}')


def write_sequencing_reports(
    project: str,
    access_level: str,
    file_types: str,
    sequence_type: str,
    sequence_reads_to_delete: list[tuple[str, int, str, list[int]]],
    sequence_reads_to_ingest: list[str],
    incomplete_samples: list[tuple[str, str]],
):
    """Write the sequences to delete and ingest to csv files and upload them to the bucket"""
    # Writing the output to files with the file type, sequence type, and date specified
    today = datetime.today().strftime('%Y-%m-%d')

    if access_level == 'test':
        bucket_name = f'cpg-{project}-upload'
    else:
        bucket_name = f'cpg-{project}-main-upload'

    report_path = f'gs://{bucket_name}/audit_results/{today}/'

    # Sequences to delete report has several data points for validation
    if not sequence_reads_to_delete:
        logging.info('No sequence reads to delete found.')
    else:
        sequences_to_delete_file = (
            f'{project}_{file_types}_{sequence_type}_sequences_to_delete_{today}.csv'
        )
        write_csv_report_to_cloud(
            data_to_write=sequence_reads_to_delete,
            report_path=os.path.join(report_path, sequences_to_delete_file),
            header_row=['Sample_ID', 'Sequence_ID', 'Sequence_Path', 'Analysis_IDs'],
        )

    # Sequences to ingest file only contains paths to the (possibly) uningested files
    if not sequence_reads_to_ingest:
        logging.info('No sequence reads to ingest found.')
    else:
        sequences_to_ingest_file = (
            f'{project}_{file_types}_{sequence_type}_sequences_to_ingest_{today}.csv'
        )
        write_csv_report_to_cloud(
            data_to_write=sequence_reads_to_delete,
            report_path=os.path.join(report_path, sequences_to_ingest_file),
            header_row=None,
        )

    # Write the samples without any completed cram to a csv
    if not incomplete_samples:
        logging.info(f'No samples without crams found.')
    else:
        incomplete_samples_file = (
            f'{project}_{file_types}_{sequence_type}_samples_without_crams_{today}.csv'
        )
        write_csv_report_to_cloud(
            data_to_write=sequence_reads_to_delete,
            report_path=os.path.join(report_path, incomplete_samples_file),
            header_row=['Sample_ID', 'Sequence_ID', 'Sequence_Path', 'Analysis_IDs'],
        )


@click.command()
@click.option(
    '--project',
    help='Metamist project, used to filter samples',
)
@click.option(
    '--sequence-type',
    '-s',
    type=click.Choice(['genome', 'exome', 'all']),
    required='True',
    help='genome, exome, or all',
)
@click.option(
    '--file-types',
    '-f',
    type=click.Choice(['fastq', 'bam', 'cram', 'gvcf', 'vcf', 'all']),
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, or all sequence file types',
)
def main(
    project,
    sequence_type,
    file_types,
):
    """
    Finds sequence files for samples with completed CRAMs and adds these to a csv for deletion.
    Also finds any extra files in the upload bucket which may be uningested sequence data.
    Works in a few simple steps:
        1. Get all participants in a project, their samples, and the sequences for the samples
        2. Get all the sequence file paths that exist in the project's main-upload bucket
        3. For samples without a completed CRAM - log these
           For samples with a completed CRAM - add their sequence files to the delete list
        4. Any files found in the bucket but NOT found in metamist are checked to see if they ARE
           a file from metamist, but in a different location. We match the file names and sizes
           for this check and consider these as "moved" sequence files. Add these to the list
           of sequence files to delete.
        5. Report any uningested files a csv
    """
    config = get_config()
    if not project:
        project = config['workflow']['dataset']
    access_level = config['workflow']['access_level']

    # Get all the participants and all the samples mapped to participants
    participant_data = get_project_participant_data(project)

    # Filter out any participants with no samples
    participant_data = [
        participant for participant in participant_data if participant.get('samples')
    ]

    # Get all the samples
    samples_all = get_internal_to_external_sample_map_for_participants(participant_data)

    # Get all the sequences for the samples in the project and map them to their samples and reads
    seq_id_sample_id_map, sequence_reads = get_sequences_from_participants(
        participant_data, SEQUENCE_TYPES_MAP.get(sequence_type)
    )

    # Also create a mapping of sample ID: sequence ID - use defaultdict in case a sample has several sequences
    sample_id_seq_id = defaultdict(list)
    for sequence, sample in seq_id_sample_id_map.items():
        sample_id_seq_id[sample].append(sequence)

    # Get all completed cram output paths for the samples in the project and validate them
    sample_cram_paths = get_analysis_cram_paths_for_project_samples(
        project, samples_all, SEQUENCE_TYPES_MAP.get(sequence_type)
    )

    # Identify samples with and without completed crams
    sample_completion = get_complete_and_incomplete_samples(
        project, samples_all, sample_cram_paths
    )
    incomplete_samples = [
        (sample_id, samples_all.get(sample_id))
        for sample_id in sample_completion.get('incomplete')
    ]

    # Samples with completed crams can have their sequences deleted - these are the obvious ones
    sequence_reads_to_delete, sequence_reads_to_ingest = get_reads_to_delete_or_ingest(
        project,
        sample_completion.get('complete'),
        sequence_reads,
        sample_id_seq_id,
        seq_id_sample_id_map,
        FILE_TYPES_MAP.get(file_types),
    )

    write_sequencing_reports(
        project,
        access_level,
        file_types,
        sequence_type,
        sequence_reads_to_delete,
        sequence_reads_to_ingest,
        incomplete_samples,
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=E1120
