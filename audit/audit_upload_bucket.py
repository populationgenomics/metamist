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

from collections import defaultdict
import csv
from datetime import datetime
import sys
import logging
import os
from typing import Any
import click

from cloudpathlib import AnyPath
from cpg_utils.config import get_config

from audit.generic_auditor import GenericAuditor

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

# QUERY = """
# query ProjectData($projectName: String!) {
#     project(name: $projectName) {
#         participants {
#             externalId
#             id
#             samples {
#                 id
#                 externalId
#                 type
#                 sequences {
#                     id
#                     meta
#                     type
#                 }
#             }
#         }
#     }
# }
# """

# ANALYSIS_TYPES = [
#     'qc',
#     'joint-calling',
#     'gvcf',
#     'cram',
#     'custom',
#     'es-index',
#     'sv',
#     'web',
#     'analysis-runner',
# ]

# BUCKET_TYPES = [
#     'main',
#     'test',
#     'archive',
#     'release',
# ]

# EXCLUDED_SAMPLES = [
#     'CPG11783',  # acute-care, no FASTQ data
#     'CPG13409',  # perth-neuro, coverage ~0x
#     'CPG243717',  # validation, NA12878_KCCG low coverage https://main-web.populationgenomics.org.au/validation/qc/cram/multiqc.html,
#     'CPG246645',  # ag-hidden, eof issue  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/440
#     'CPG246678',  # ag-hidden, diff fastq size  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/446
#     'CPG261792',  # rdp-kidney misformated fastq - https://batch.hail.populationgenomics.org.au/batches/378736/jobs/43
#     # acute care fasq parsing errors https://batch.hail.populationgenomics.org.au/batches/379303/jobs/24
#     'CPG259150',
#     'CPG258814',
#     'CPG258137',
#     'CPG258111',
#     'CPG258012',
#     # ohmr4 cram parsing in align issues
#     'CPG261339',
#     'CPG261347',
#     # IBMDX truncated sample? https://batch.hail.populationgenomics.org.au/batches/422181/jobs/99
#     'CPG265876',
# ]

# AAPI = AnalysisApi()
# PAPI = ParticipantApi()
# SAPI = SampleApi()
# SEQAPI = SequenceApi()
# CLIENT = storage.Client()

# Sequence_Report_Entry = namedtuple(
#     'Sequence_Report_Entry', 'Sample_ID Sequence_ID Sequence_File_Path Analysis_IDs'
# )
# Completed_Analysis = namedtuple(
#     'Completed_Analysis',
#     'Analysis_ID Analysis_Type Analysis_Output Timestamp_Completed',
# )
# TBucket = TypeVar('TBucket', bound=Bucket)


class UploadBucketAuditor(GenericAuditor):
    """Auditor specifically for upload cloud storage buckets"""

    def __init__(
        self,
        project: str,
        sequence_type: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(
            project=project,
            sequence_type=sequence_type,
            file_types=file_types,
            default_analysis_type=default_analysis_type,
            default_analysis_status=default_analysis_status,
        )

    @staticmethod
    def write_csv_report_to_cloud(
        data_to_write: list[Any], report_path: AnyPath, header_row: list[str] | None
    ):
        """
        Writes a csv report to the cloud bucket containing the data to write
        at the report path, with an optional header row
        """
        with AnyPath(report_path).open('w+') as f:  # pylint: disable=E1101
            writer = csv.writer(f)
            if header_row:
                writer.writerow(header_row)
            for row in data_to_write:
                if isinstance(row, str):
                    writer.writerow([row])
                    continue
                writer.writerow(row)

        logging.info(f'Wrote {len(data_to_write)} lines to report: {report_path}')

    def write_upload_bucket_audit_reports(
        self,
        access_level: str,
        sequence_files_to_delete: list[tuple[str, int, str, list[int]]],
        sequence_files_to_ingest: list[str],
        incomplete_samples: list[tuple[str, str]],
    ):
        """Write the sequences to delete and ingest to csv files and upload them to the bucket"""
        # Writing the output to files with the file type, sequence type, and date specified
        today = datetime.today().strftime('%Y-%m-%d')

        bucket_name = f'cpg-{self.project}-main-upload'
        if access_level == 'test':
            bucket_name = f'cpg-{self.project}-upload'

        report_path = f'gs://{bucket_name}/audit_results/{today}/'

        # Sequences to delete report has several data points for validation
        if not sequence_files_to_delete:
            logging.info('No sequence read files to delete found. Skipping report...')
        else:
            sequences_to_delete_file = f'{self.project}_{self.file_types}_{self.sequence_type}_sequences_to_delete_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=sequence_files_to_delete,
                report_path=os.path.join(report_path, sequences_to_delete_file),
                header_row=[
                    'Sample_ID',
                    'Sequence_ID',
                    'Sequence_Path',
                    'Analysis_IDs',
                ],
            )

        # Sequences to ingest file only contains paths to the (possibly) uningested files
        if not sequence_files_to_ingest:
            logging.info('No sequence reads to ingest found. Skipping report...')
        else:
            sequences_to_ingest_file = f'{self.project}_{self.file_types}_{self.sequence_type}_sequences_to_ingest_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=sequence_files_to_ingest,
                report_path=os.path.join(report_path, sequences_to_ingest_file),
                header_row=None,
            )

        # Write the samples without any completed cram to a csv
        if not incomplete_samples:
            logging.info(f'No samples without crams found. Skipping report...')
        else:
            incomplete_samples_file = f'{self.project}_{self.file_types}_{self.sequence_type}_samples_without_crams_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=incomplete_samples,
                report_path=os.path.join(report_path, incomplete_samples_file),
                header_row=['Internal_Sample_ID', 'External_Sample_ID'],
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
    type=click.Choice(['fastq', 'bam', 'cram', 'all_reads', 'gvcf', 'vcf', 'all']),
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, all_reads (fastq + bam + cram), or all sequence file types',
)
def main(
    project,
    sequence_type,
    file_types,
    default_analysis_type='cram',
    default_analysis_status='completed',
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

    auditor = UploadBucketAuditor(
        project=project,
        sequence_type=SEQUENCE_TYPES_MAP.get(sequence_type),
        file_types=FILE_TYPES_MAP.get(file_types),
        default_analysis_type=default_analysis_type,
        default_analysis_status=default_analysis_status,
    )

    # Get all the participants and all the samples mapped to participants
    participant_data = auditor.get_participant_data_for_project()

    # Filter out any participants with no samples
    participant_data = [
        participant for participant in participant_data if participant.get('samples')
    ]

    # Get all the samples
    sample_internal_external_id_map = auditor.map_internal_to_external_sample_ids(
        participant_data
    )

    # Get all the sequences for the samples in the project and map them to their samples and reads
    (
        seq_id_sample_id_map,
        sequence_filepaths_filesizes,
    ) = auditor.get_sequences_from_participants(participant_data)

    # Also create a mapping of sample ID: sequence ID - use defaultdict in case a sample has several sequences
    sample_id_seq_id = defaultdict(list)
    for sequence, sample in seq_id_sample_id_map.items():
        sample_id_seq_id[sample].append(sequence)

    # Get all completed cram output paths for the samples in the project and validate them
    sample_cram_paths = auditor.get_analysis_cram_paths_for_project_samples(
        sample_internal_external_id_map
    )

    # Identify samples with and without completed crams
    sample_completion = auditor.get_complete_and_incomplete_samples(
        sample_internal_external_id_map, sample_cram_paths
    )
    incomplete_samples = [
        (sample_id, sample_internal_external_id_map.get(sample_id))
        for sample_id in sample_completion.get('incomplete')
    ]

    # Samples with completed crams can have their sequences deleted - these are the obvious ones
    (
        sequence_reads_to_delete,
        sequence_files_to_ingest,
    ) = auditor.get_reads_to_delete_or_ingest(
        sample_completion.get('complete'),
        sequence_filepaths_filesizes,
        sample_id_seq_id,
        seq_id_sample_id_map,
    )

    auditor.write_upload_bucket_audit_reports(
        access_level,
        sequence_reads_to_delete,
        sequence_files_to_ingest,
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
