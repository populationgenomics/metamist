from collections import defaultdict, namedtuple
import logging
import os
from typing import Any

from audithelper import AuditHelper
from sample_metadata.apis import AnalysisApi
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.graphql import query


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


SequenceReportEntry = namedtuple(
    'SequenceReportEntry', 'sampleID sequenceID sequence_file_path analysisIDs'
)
CompletedAnalysis = namedtuple(
    'CompletedAnalysis',
    'analysisID analysis_type analysis_output timestamp_completed',
)


class GenericAuditor(AuditHelper):
    """Auditor for cloud storage buckets"""

    def __init__(
        self,
        project: str,
        sequence_type: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        if not project:
            raise ValueError('Metamist project is required')

        self.project = project
        self.sequence_type = sequence_type
        self.file_types = file_types
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        self.query = """
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

        self.aapi = AnalysisApi()

        super().__init__()

    def get_participant_data_for_project(self) -> list[dict]:
        """
        Uses a graphQL query to return all participants in a Metamist project.
        Nested in the return are all samples associated with the participants,
        and then all the sequences associated with those samples.
        """

        participant_data = (
            query(self.query, {'projectName': self.project})
            .get('project')
            .get('participants')
        )

        # Filter out any participants with no samples and log any found
        for participant in participant_data:
            if not participant.get('samples'):
                logging.info(
                    f'Participant {participant.get("id")} / {participant.get("externalId")} from {self.project} has no samples. Filtering this participant out.'
                )
        participant_data = [
            participant
            for participant in participant_data
            if participant.get('samples')
        ]

        return participant_data

    def map_participants_to_samples(self, participants: list[dict]) -> dict[str, list]:
        """
        Returns the {external_participant_id : sample_internal_id} mapping for a Metamist project
        - Also reports if any participants have more than one sample
        """

        # Create mapping of participant external ID to sample ID
        participant_eid_sample_id_map = {
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
        for participant_eid, sample_ids in participant_eid_sample_id_map.items():
            if len(sample_ids) > 1:
                logging.info(
                    f'Participant {participant_eid_to_iid[participant_eid]} (external ID: {participant_eid})'
                    + f' associated with more than one sample id: {sample_ids}'
                )

        return participant_eid_sample_id_map

    def map_internal_to_external_sample_ids(
        self,
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
        sample_internal_external_id_map = {
            sample_id: sample_external_id
            for sample_map in samples_all
            for sample_id, sample_external_id in sample_map.items()
            if sample_id not in EXCLUDED_SAMPLES
        }

        return sample_internal_external_id_map

    def get_sequence_mapping(
        self, all_sequences: list[dict]
    ) -> tuple[dict[int, str], defaultdict[int, list]]:
        """
        Input: A list of Metamist sequences
        Returns dict mappings of:
        {sequence_id : sample_internal_id}, and {sequence_id : (sequence_filepath, sequence_filesize)}
        """
        seq_id_sample_id_map = {}
        # multiple reads per sequence is possible so use defaultdict(list)
        sequence_filepaths_filesizes = defaultdict(list)
        for sample_sequence in all_sequences:  # pylint: disable=R1702
            sample_internal_id = list(sample_sequence.keys())[
                0
            ]  # Extract the sample ID
            for sequences in sample_sequence.values():
                for sequence in sequences:
                    if not sequence.get('type').lower() in self.sequence_type:
                        continue
                    meta = sequence.get('meta')
                    reads = meta.get('reads')
                    seq_id_sample_id_map[sequence.get('id')] = sample_internal_id
                    if not reads:
                        logging.info(
                            f'Sample {sample_internal_id} has no sequence reads'
                        )
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
                        sequence_filepaths_filesizes[sequence.get('id')].append(
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
                                sequence_filepaths_filesizes[sequence.get('id')].append(
                                    (inner_read.get('location'), inner_read.get('size'))
                                )
                        else:
                            if not isinstance(read, dict):
                                logging.error(
                                    f'Got {type(inner_read)} read, expected dict: {inner_read}'
                                )
                                continue
                            sequence_filepaths_filesizes[sequence.get('id')].append(
                                (read.get('location'), read.get('size'))
                            )

        return seq_id_sample_id_map, sequence_filepaths_filesizes

    def get_sequences_from_participants(
        self, participants: list[dict]
    ) -> tuple[dict[int, str], defaultdict[int, list]]:
        """Return latest sequence ids for internal sample ids"""
        all_sequences = [
            {
                sample.get('id'): sample.get('sequences')
                for sample in participant.get('samples')
            }
            for participant in participants
        ]

        return self.get_sequence_mapping(all_sequences)

    def get_analysis_cram_paths_for_project_samples(
        self,
        sample_internal_to_external_id_map: dict[str, str],
    ) -> defaultdict[str, dict[int, str]]:
        """
        Queries all analyses for the list of samples in the given project AND the seqr project.
        Returns a dict mapping {sample_id : (analysis_id, cram_path) }
        """
        sample_ids = list(sample_internal_to_external_id_map.keys())
        projects = [
            self.project,
        ]
        if 'test' not in self.project:
            projects.append('seqr')

        analyses = self.aapi.query_analyses(
            AnalysisQueryModel(
                projects=projects,
                type=AnalysisType(self.default_analysis_type),
                status=AnalysisStatus(
                    self.default_analysis_status
                ),  # Only interested in aligned crams
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
                f'CRAMs from dataset {self.project} did not have sequencing_type: {crams_with_missing_seq_type}'
            )

        # Filter the analyses based on input sequence type
        analyses = [
            analysis
            for analysis in analyses
            if analysis.get('meta')['sequencing_type'] in self.sequence_type
        ]

        # For each sample ID, collect the analysis IDs and cram paths
        sample_cram_paths: defaultdict[str, dict[int, str]] = defaultdict(dict)
        for analysis in analyses:
            # Check the analysis output path is a valid gs path to a .cram file
            if not analysis['output'].startswith('gs://') and analysis[
                'output'
            ].endswith('cram'):
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

    def analyses_for_samples_without_crams(self, samples_without_crams: list[str]):
        """Checks if other completed analyses exist for samples without completed crams"""
        projects = [
            self.project,
        ]
        if 'test' not in self.project:
            projects.append('seqr')

        all_sample_analyses: defaultdict[
            str, list[tuple[int, AnalysisType, str, str]]
        ] = defaultdict()
        for analysis_type in ANALYSIS_TYPES:
            analyses = self.aapi.query_analyses(
                AnalysisQueryModel(
                    projects=projects,
                    type=AnalysisType(analysis_type),
                    status=AnalysisStatus(
                        self.default_analysis_status
                    ),  # Only interested in completed analyses
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

                analysis_entry = CompletedAnalysis(
                    analysisID=analysis.get('id'),
                    analysis_type=analysis.get('type'),
                    analysis_output=analysis.get('output'),
                    timestamp_completed=analysis.get('timestampCompleted'),
                )
                all_sample_analyses[sample_id].append(analysis_entry)

        if all_sample_analyses:
            for sample_without_cram, completed_analyses in all_sample_analyses.items():
                for completed_analysis in completed_analyses:
                    logging.warning(
                        f'{sample_without_cram} missing CRAM but has analysis {completed_analysis}'
                    )

    def get_complete_and_incomplete_samples(
        self,
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
        buckets_subdirs = self.get_gcs_bucket_subdirs_to_search(list(cram_paths))
        crams_in_bucket = self.find_files_in_gcs_buckets_subdirs(
            buckets_subdirs,
            ('cram',),
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
            self.analyses_for_samples_without_crams(list(incomplete_samples))

        return {'complete': completed_samples, 'incomplete': list(incomplete_samples)}

    def check_for_uningested_or_moved_sequences(  # pylint: disable=R0914
        self,
        sequence_filepaths_filesizes: defaultdict[int, list[tuple[str, int]]],
        completed_samples: defaultdict[str, list[int]],
        seq_id_sample_id_map_map: dict[int, str],
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
        bucket_name = f'cpg-{self.project}-upload'
        if 'test' not in self.project:
            bucket_name = f'cpg-{self.project}-main-upload'

        # Get all the paths to sequence data anywhere in the main-upload bucket
        sequence_paths_in_bucket = self.find_sequence_files_in_gcs_bucket(
            bucket_name, self.file_types
        )

        # Flatten all the Metamist sequence file paths and sizes into a single list
        sequence_paths_sizes_in_metamist: list[tuple[str, int]] = []
        for sequence in sequence_filepaths_filesizes.values():
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
        metamist_sequence_file_size_map = {
            os.path.basename(path_size[0]): path_size[1]
            for path_size in sequence_paths_sizes_in_metamist
        }
        metamist_sequence_file_path_map = {
            os.path.basename(path_size[0]): path_size[0]
            for path_size in sequence_paths_sizes_in_metamist
        }

        # Identify if any paths are to files that have actually just been moved by checking if they are in the bucket but not metamist
        ingested_and_moved_filepaths = []
        for path in uningested_sequence_paths:
            filename = os.path.basename(path)
            # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
            if filename in metamist_sequence_file_size_map.keys():
                filesize = self.file_size(path)
                if filesize == metamist_sequence_file_size_map.get(filename):
                    ingested_and_moved_filepaths.append(
                        (path, metamist_sequence_file_path_map.get(filename))
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
        for sequence_id, reads_sizes in sequence_filepaths_filesizes.items():
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
                    SequenceReportEntry(
                        sampleID=sample_id,
                        sequenceID=seq_id,
                        sequence_file_path=bucket_path,
                        analysisIDs=analysis_ids,
                    )
                )

        return uningested_sequence_paths, sequences_moved_paths

    def get_reads_to_delete_or_ingest(
        self,
        completed_samples: defaultdict[str, list[int]],
        sequence_filepaths_filesizes: defaultdict[int, list[tuple[str, int]]],
        seq_id_sample_id_map: dict[int, str],
    ) -> tuple[list, list]:
        """
        Inputs: 1. List of samples which have completed CRAMs
                2. Dictionary mapping of sequence IDs to sequence file paths and file sizes
                3. Dictionary mapping sample IDs to sequence IDs
                4. Dictionary mapping sequence IDs to sample IDs
        Returns a tuple of two lists, each containing sequences IDs and sequence file paths.
        The first containins reads which can be deleted, the second containing reads to ingest.
        The sample ID, sequence ID, and analysis ID (of completed cram) are included in the delete list.
        """
        # Check for uningested sequence data that may be hiding or sequence data that has been moved
        (
            reads_to_ingest,
            moved_sequences_to_delete,
        ) = self.check_for_uningested_or_moved_sequences(
            sequence_filepaths_filesizes,
            completed_samples,
            seq_id_sample_id_map,
        )

        # Create a mapping of sample ID: sequence ID - use defaultdict in case a sample has several sequences
        sample_id_seq_id_map: defaultdict[str, list[int]] = defaultdict(list)
        for sequence, sample in seq_id_sample_id_map.items():
            sample_id_seq_id_map[sample].append(sequence)

        sequence_reads_to_delete = []
        for sample_id, analysis_ids in completed_samples.items():
            if sample_id in EXCLUDED_SAMPLES:
                continue
            seq_ids = sample_id_seq_id_map[sample_id]
            for seq_id in seq_ids:
                seq_read_paths = [
                    reads_sizes[0]
                    for reads_sizes in sequence_filepaths_filesizes[seq_id]
                ]
                for path in seq_read_paths:
                    sequence_reads_to_delete.append(
                        SequenceReportEntry(
                            sampleID=sample_id,
                            sequenceID=seq_id,
                            sequence_file_path=path,
                            analysisIDs=analysis_ids,
                        )
                    )

        reads_to_delete = sequence_reads_to_delete + moved_sequences_to_delete

        return reads_to_delete, reads_to_ingest
