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

TODO: find other files to delete, not just fastqs
TODO: make sure sequences are grabbed with the appropriate fields, e.g. read_type, project
TODO: add filtering for exome vs genome
TODO: add test and dry-run functionality
TODO: make compatible with analysis-runner
"""

from collections import defaultdict
import logging
import click
from cloudpathlib import CloudPath
from cpg_utils.cloud import get_path_components_from_gcp_path
from google.cloud import storage
from sample_metadata.apis import (
    ProjectApi,
    ParticipantApi,
    AnalysisApi,
    SampleApi,
    SequenceApi,
)
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus


FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
ALL_EXTENSIONS = (
    '.fq.gz',
    '.fastq.gz',
    '.fq',
    '.fastq',
    '.bam',
    '.cram',
    '.g.vcf.gz',
    '.vcf',
    '.vcf.gz',
)

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


def get_bucket_subdirs_to_search(paths: list[str]) -> defaultdict[str, list]:
    """ "
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


def find_sequence_files_in_bucket(bucket_name: str) -> list[str]:
    """Gets all the gs paths to fastq files in the projects main-upload bucket"""
    sequence_paths = []
    for blob in CLIENT.list_blobs(bucket_name, prefix=''):
        if blob.name.endswith(FASTQ_EXTENSIONS):
            sequence_paths.append(f'gs://{bucket_name}/{blob.name}')
        continue

    return sequence_paths


def get_filename_from_path(path: str) -> str:
    """Extracts filename from file path. e.g. 'subdir/file.ext' -> 'file.ext'"""

    return path.rsplit('/', 1)[1]


def get_filesize_from_path(bucket_name: str, path: str) -> int:
    """Extracts filesize from gs path"""
    bucket = CLIENT.get_bucket(bucket_name)
    blob_path = path.split(f'gs://{bucket_name}/')[1]
    blob = bucket.get_blob(blob_path)

    return blob.size


def get_datasets() -> list[dict]:
    """API call to get project list"""
    return ProjectApi().get_seqr_projects()


def get_participant_sample_ids_for_project(project: str) -> defaultdict[str, list]:
    """
    Gets all the participants from a Metamist project + the external_participant_id : sample_id mapping
    """
    # Create mapping of participant external ID to sample ID
    # use defaultdict(list) as one participant could have several samples
    participants_samples = PAPI.get_external_participant_id_to_internal_sample_id(
        project=project
    )
    participant_sample_ids = defaultdict(list)
    for participant_sample in participants_samples:
        # each entry in participants_samples is like [external_participant_id, sample_id]
        participant_sample_ids[participant_sample[0]].append(participant_sample[1])

    return participant_sample_ids


def get_samples_for_project(project: str):
    """Gets all the sample IDs for a Metamist project mapped to their external sample IDs"""
    samples = SAPI.get_all_sample_id_map_by_internal(project=project)
    for exclusion in EXCLUDED_SAMPLES:
        if samples.get(exclusion):
            del samples[exclusion]

    return samples


def analyse_and_validate_participants_samples(
    participants: list[dict],
    participant_sample_ids: defaultdict[str, list],
    samples: dict[str, str],
):
    """
    Input: Metamist list of participants, mapping of participant external ID : Internal Sample ID,
    and the Metamist list of samples for the project.

    No returns, just report if any participants have multiple samples or no samples,
    or if any samples have no participants.
    """
    # Get the external_participant_id : internal_participant_id mapping for all participants
    participant_eid_to_iid = {
        participant['external_id']: participant['id'] for participant in participants
    }

    # Report if any participants have more than one sample ID
    for participant_eid, sample_ids in participant_sample_ids.items():
        if len(sample_ids) > 1:
            logging.info(
                f'Participant {participant_eid_to_iid[participant_eid]} (external ID: {participant_eid})'
                + f' assosciated with more than one sample id: {[(sample_id, samples[sample_id]) for sample_id in sample_ids]}'
            )

    # Compare the participants in the full list to the participants from the sample:participant_eid mapping
    participant_eid_set = set(
        participant['external_id'] for participant in participants
    )
    participant_eid_from_samples_set = set(participant_sample_ids.keys())
    logging.info(
        f'Participants with no samples: {len(participant_eid_set.difference(participant_eid_from_samples_set))}'
    )

    # Compare the samples obtained from SampleApi vs ParticipantApi (papi)
    sample_id_set = set(samples.keys())
    sample_id_from_papi_set = set(
        [item for sublist in participant_sample_ids.values() for item in sublist]
    )
    logging.info(
        f'Samples with no participants: {sample_id_set.difference(sample_id_from_papi_set)}'
    )


def get_sequences_for_samples(
    sequences: list[dict],
) -> tuple[dict[int, str], defaultdict[int, list]]:
    """
    Get all the sequence read locations for the sequences associated with the samples in the project
    Returns dict mappings of {sequence_id: sample_id}, and {sequence_id: read_locations}
    """
    seq_id_sample_id = {}
    sequence_reads = defaultdict(
        list
    )  # multiple reads per sequence is possible so use defaultdict(list)
    for seq in sequences:
        seq_id_sample_id[seq['id']] = seq['sample_id']
        reads = seq['meta'].get('reads')
        if not reads:
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
            sequence_reads[seq['id']].append((reads['location'], reads['size']))
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
                    sequence_reads[seq['id']].append(
                        (inner_read['location'], inner_read['size'])
                    )
            else:
                if not isinstance(read, dict):
                    logging.error(
                        f'Got {type(inner_read)} read, expected dict: {inner_read}'
                    )
                    continue
                sequence_reads[seq['id']].append((read['location'], read['size']))

    return seq_id_sample_id, sequence_reads


def get_analysis_cram_paths_for_project_samples(project: str, samples: dict[str, str]):
    """
    Queries all analyses for the list of samples in the given project AND the seqr project.
    Returns a dict mapping {sample_id : cram_path}
    """
    sample_ids = list(samples.keys())
    analyses = AAPI.query_analyses(
        AnalysisQueryModel(
            projects=[
                project,
                'seqr',
            ],  # Many analysis objects hiding out in seqr project
            type=AnalysisType('cram'),
            status=AnalysisStatus('completed'),  # Only interested in aligned crams
            sample_ids=sample_ids,
        )
    )

    sample_cram_paths = {}
    for analysis in analyses:
        # First check the analysis output path is valid
        if not validate_analysis_output(analysis['output'], 'cram'):
            logging.info(
                f'Analysis {analysis["id"]} invalid output path: {analysis["output"]}'
            )
            continue
        try:
            # Try getting the sample ID from the 'meta' field's 'sample' field
            sample_cram_paths[analysis['meta']['sample']] = analysis['output']
        except KeyError:
            # Try getting the sample ID from the 'sample_ids' field
            for sample_id in analysis['sample_ids']:
                sample_cram_paths[sample_id] = analysis['output']

    return sample_cram_paths


def validate_analysis_output(output_path: str, file_extension: str):
    """Check that the analysis output path begins with gs:// and ends in 'cram'"""
    return output_path.startswith('gs://') and output_path.endswith(file_extension)


def get_complete_and_incomplete_samples(
    samples: list[str], sample_cram_paths: dict[str, list[str]]
):
    """
    Returns a dictionary containing two lists of samples:
     - the completed samples which have finished aligning and have a cram
     - the incomplete samples where the alignment hasn't completed and no cram exists.
    """
    # Check the analysis paths actually point to crams that exist in the bucket
    cram_paths = [path for paths in sample_cram_paths.values() for path in paths]
    buckets_subdirs = get_bucket_subdirs_to_search(cram_paths)
    crams_in_bucket = find_files_in_buckets_subdirs(
        buckets_subdirs,
        [
            'cram',
        ],
    )

    # Incomplete samples initialised as the list of samples without a valid analysis cram output path
    incomplete_samples = set(samples).difference(set(sample_cram_paths.keys()))

    # Completed samples are those whose completed cram output path exists in the bucket at the same path
    completed_samples = []
    for sample, cram_path in sample_cram_paths.items():
        if cram_path in crams_in_bucket:
            completed_samples.append(sample)
            continue
        incomplete_samples.update(sample)

    return {'complete': completed_samples, 'incomplete': list(incomplete_samples)}


def check_for_uningested_or_moved_sequences(
    project: str, sequence_reads: defaultdict[int, list]
):
    """
    Compares the sequences in a Metamist project to the sequences in the main-upload bucket

    Input: The project name, and the sequence dictionaries for the project from Metamist
    Returns: 1. Paths to sequences that have not yet been ingested.
             2. A dict mapping sequence IDs to GS filepaths that have been ingested,
                but where the path in the bucket is different to the path for this
                sequence in Metamist. If the filenames and file sizes match,
                these are identified as sequence files that have been moved from
                their original location to a new location.
    """
    # Get all the paths to sequence data anywhere in the main-upload bucket
    bucket_name = f'cpg-{project}-main-upload'
    sequence_paths_in_bucket = find_sequence_files_in_bucket(bucket_name)

    # Flatten all the sequence file paths and file sizes in metamist for this project into a single list
    sequence_paths_sizes_in_metamist = []
    for sequence in sequence_reads.values():
        sequence_paths_sizes_in_metamist.extend(sequence)

    # Find the paths that exist in the bucket and not in metamist
    sequence_paths_in_metamist = [
        path_size[0] for path_size in sequence_paths_sizes_in_metamist
    ]
    uningested_paths = list(
        set(sequence_paths_in_bucket).difference(set(sequence_paths_in_metamist))
    )

    # Strip the metamist paths into just filenames
    sequence_files_sizes_in_metamist = {
        get_filename_from_path(path[0]): path[1]
        for path in sequence_paths_sizes_in_metamist
    }
    sequence_files_paths_in_metamist = {
        get_filename_from_path(path[0]): path[0]
        for path in sequence_paths_sizes_in_metamist
    }

    # Identify if any paths are to files that have actually just been moved by checking if they are in the bucket but not metamist
    ingested_and_moved_filepaths = []
    for path in uningested_paths:
        filename = get_filename_from_path(path)

        # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
        if filename in sequence_files_sizes_in_metamist.keys():
            filesize = get_filesize_from_path(bucket_name, path)
            if filesize == sequence_files_sizes_in_metamist.get(filename):
                ingested_and_moved_filepaths.append(
                    (path, sequence_files_paths_in_metamist.get(filename))
                )

    # If the file has just been moved, we consider it ingested
    for bucket_path, _ in ingested_and_moved_filepaths:
        uningested_paths.remove(bucket_path)

    # flip the sequence id : reads mapping to identify sequence IDs by their read paths
    reads_sequences = {}
    for sequence_id, reads_sizes in sequence_reads.items():
        for read_size in reads_sizes:
            reads_sequences[read_size[0]] = sequence_id

    # Collect the sequences for files that have been ingested and moved to a different bucket location
    sequences_moved_paths = defaultdict(list)
    for bucket_path, metamist_path in ingested_and_moved_filepaths:
        seq_id = reads_sequences[metamist_path]
        sequences_moved_paths[seq_id].append(bucket_path)

    return uningested_paths, sequences_moved_paths


def get_reads_to_delete(
    completed_samples: list[str],
    sequence_reads: dict[int, list[tuple[str, int]]],
    sample_id_seq_id_map: dict[str, list[int]],
) -> dict[int, list[str]]:
    """
    Inputs: 1. List of samples which have completed CRAMs
            2. Dictionary mapping of sequence IDs to read paths and read file sizes
            3. Dictionary mapping sample IDs to sequence IDs
    Returns a dictionary mapping sequences IDs to sequence read file paths which can be safely deleted.
    """
    sequence_reads_to_delete = defaultdict(list)
    for sample in completed_samples:
        seq_ids = sample_id_seq_id_map[sample]
        for seq_id in seq_ids:
            seq_read_paths = [reads_sizes[0] for reads_sizes in sequence_reads[seq_id]]
            sequence_reads_to_delete[seq_id].extend(seq_read_paths)

    return sequence_reads_to_delete


def clean_up_cloud_storage(locations: list[CloudPath]):
    """Given a list of locations of files to be deleted"""
    for location in locations:
        location.unlink()
        logging.info(f'{location.name} was deleted from cloud storage.')


@click.command()
@click.option(
    '--project',
    help='Metamist project, used to filter samples',
)
# @click.option('--test', '-t', is_flag=True, default=False, help='uses test datasets')
@click.option(
    '--dry-run',
    is_flag=True,
)
def main(
    project,
    # test,
    dry_run,
):
    """
    Finds sequence files which can be deleted and does so if the dry-run flag is absent.
    Works in a few simple steps:
        1. Get all participants in a project, their samples, and the sequences for the samples
        2. Get all the sequence file paths that exist in the project's main-upload bucket
        3. For samples without a completed CRAM - log these
           For samples with a completed CRAM - add their sequence files to the delete list
        4. Any files found in the bucket but NOT found in metamist are checked to see if they ARE
           a file from metamist, but in a different location. We match the file names and sizes
           for this check and consider these as "moved" sequence files.
        5. Report any uningested files with the logger (TODO: report these in an output file).
           Add any "moved" files to the sequence files to delete list.
        6. Execute the delete on the full list.
    """
    # Get all the participants and all the samples mapped to participants
    participants_all = PAPI.get_participants(project)
    participant_sample_ids = get_participant_sample_ids_for_project(project)

    # Get all the samples
    samples_all = get_samples_for_project(project)

    # Check for any participants without samples, or samples without sequence data
    analyse_and_validate_participants_samples(
        participants_all, participant_sample_ids, samples_all
    )

    # Get all the sequences for the samples in the project and map them to their samples and reads
    sequences = SEQAPI.get_sequences_by_sample_ids(
        request_body=list(samples_all.keys())
    )
    seq_id_sample_id, sequence_reads = get_sequences_for_samples(sequences)

    # Also create a mapping of sample ID: sequence ID - use defaultdict in case a sample has several sequences
    sample_id_seq_id = defaultdict(list)
    for sequence, sample in seq_id_sample_id.items():
        sample_id_seq_id[sample].append(sequence)

    # Get all completed cram output paths for the samples in the project and validate them
    sample_cram_paths = get_analysis_cram_paths_for_project_samples(
        project, samples_all
    )

    for sample, cram_path in sample_cram_paths.items():
        if not validate_analysis_output(cram_path, '.cram'):
            raise ValueError(
                f'{sample} cram path does not start with gs://: {cram_path}'
            )

    # Identify samples with and without completed crams
    sample_completion = get_complete_and_incomplete_samples(
        samples_all, sample_cram_paths
    )

    if sample_completion.get('incomplete'):
        logging.info(
            f'Samples without CRAMs found: {sample_completion.get("incomplete")}'
        )

    # Samples with completed crams can have their sequences deleted - these are the obvious ones
    sequence_reads_to_delete = get_reads_to_delete(
        sample_completion.get('complete'), sequence_reads, sample_id_seq_id
    )

    # Check for uningested sequence data that may be hiding or sequence data that has been moved
    uningested_paths, moved_sequence_paths = check_for_uningested_or_moved_sequences(
        project, sequence_reads
    )

    # Any moved data from a sample with a completed CRAM can be added to the delete list
    moved_sequence_reads_to_delete = {}
    for sequence_id, paths in moved_sequence_paths.items():
        sample_id = seq_id_sample_id[sequence_id]
        if sample_id in sample_completion.get('complete'):
            moved_sequence_reads_to_delete[sequence_id]= paths

    logging.info(f'Found len{uningested_paths} possible uningested files in bucket')

    if not dry_run:
        # Combine the obvious reads to delete with the moved reads to delete into a flat list
        reads_to_delete = list(sequence_reads_to_delete.values()) + list(
            moved_sequence_reads_to_delete.values()
        )
        reads_to_delete = [read for readlist in reads_to_delete for read in readlist]
        # Convert the paths to delete to CloudPath objects and delete them
        reads_to_delete = [CloudPath(read) for read in reads_to_delete]
        clean_up_cloud_storage(reads_to_delete)
    else:
        logging.info(f'Dry-run - no files deleted')

    # Writing the output to files - not really completed
    # sequences_to_delete_file = f'./seq_del_{project}.csv'
    # with open(sequences_to_delete_file, 'w') as f:
    #     writer = csv.writer(f)
    #     i = 0
    #     for seq, reads in sequence_reads_to_delete.items():
    #         for read in reads:
    #             i += 1
    #             writer.writerow([seq, read])
    #     logging.info(f'Wrote {i} reads to delete')
    #     i = 0
    #     for seq, reads in moved_sequence_paths.items():
    #         for read in reads:
    #             i += 1
    #             writer.writerow([seq, read])
    #     logging.info(f'Wrote {i} reads to delete which were ingested and moved')

    # logging.info(f'Wrote sequences to delete: {sequences_to_delete_file}')

    # sequences_to_ingest_file = f'./seq_ingest_{project}.csv'
    # with open(sequences_to_ingest_file, 'w') as f:
    #     writer = csv.writer(f)
    #     i = 0
    #     for path in uningested_paths:
    #         writer.writerow([path])
    #         i += 1
    #     logging.info(f'Wrote {i} uningested sequences')

    # logging.info(f'Wrote sequences to ingest: {sequences_to_ingest_file}')


if __name__ == '__main__':
    main()  # pylint: disable=E1120
