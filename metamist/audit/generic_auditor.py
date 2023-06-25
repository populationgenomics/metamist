from collections import defaultdict, namedtuple
import logging
import os
from typing import Any

from metamist.audit.audithelper import AuditHelper
from metamist.graphql import query


ANALYSIS_TYPES = [
    'QC',
    'JOINT_CALLING',
    'GVCF',
    'CRAM',
    'CUSTOM',
    'ES_INDEX',
    'SV',
    'WEB_REPORT',
    'ANALYSIS_RUNNER',
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

PARTICIPANTS_SAMPLES_SEQUENCES_QUERY = """
        query DatasetData($datasetName: String!) {
            project(name: $datasetName) {
                participants {
                    externalId
                    id
                    samples {
                        id
                        externalId
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

SAMPLE_ANALYSIS_QUERY = """
        query sampleCrams($sampleId: String!, $analysisType: AnalysisType!) {
          sample(id: $sampleId) {
            analyses(status: COMPLETED, type: $analysisType) {
              id
              meta
              output
              timestampCompleted
            }
          }
        }
"""

# Variable type definitions
SequenceId = int
SampleId = str
SampleExternalId = str
ParticipantExternalId = str

SequenceReportEntry = namedtuple(
    'SequenceReportEntry',
    'sample_id sequence_id sequence_file_path analysis_ids filesize',
)


class GenericAuditor(AuditHelper):
    """Auditor for cloud storage buckets"""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        dataset: str,
        sequencing_type: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        if not dataset:
            raise ValueError('Metamist dataset is required')

        super().__init__(search_paths=None)

        self.dataset = dataset
        self.sequencing_type = sequencing_type
        self.file_types = file_types
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

    def get_participant_data_for_dataset(self) -> list[dict]:
        """
        Uses a graphQL query to return all participants in a Metamist dataset.
        Nested in the return are all samples associated with the participants,
        and then all the sequences associated with those samples.
        """

        participant_data = query(   # pylint: disable=unsubscriptable-object
            PARTICIPANTS_SAMPLES_SEQUENCES_QUERY, {'datasetName': self.dataset}
        )['project']['participants']

        # Filter out any participants with no samples and log any found
        for participant in participant_data:
            if not participant.get('samples'):
                logging.info(
                    f'{self.dataset} :: Filtering participant {participant.get("id")} ({participant.get("externalId")}) it has no samples.'
                )
        participant_data = [
            participant
            for participant in participant_data
            if participant.get('samples')
        ]

        return participant_data

    @staticmethod
    def map_participants_to_samples(
        participants: list[dict],
    ) -> dict[ParticipantExternalId, list[SampleId]]:
        """
        Returns the {external_participant_id : sample_internal_id}
            mapping for a Metamist dataset
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
                    f'Participant {participant_eid_to_iid[participant_eid]} '
                    f'(external ID: {participant_eid}) associated with more than '
                    f'one sample id: {sample_ids}'
                )

        return participant_eid_sample_id_map

    @staticmethod
    def map_internal_to_external_sample_ids(
        participants: list[dict],
    ) -> dict[SampleId, SampleExternalId]:
        """
        Returns the {internal sample ID : external sample ID} mapping for all
            participants in a Metamist dataset
        Also removes any samples that are part of the exclusions list
        """
        # Create a list of dictionaries, each mapping a sample ID to its external ID
        samples_all = [
            {
                sample.get('id'): sample.get('externalId')
                for sample in participant['samples']
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

    def get_sequence_map_from_participants(
        self, participants: list[dict]
    ) -> tuple[dict[Any, Any], dict[Any, list[tuple[Any, Any]]]]:
        """
        Input the list of Metamist participant dictionaries from the master graphql Query

        Returns dictionary mappings:
        {sequence_id : sample_internal_id}, and {sequence_id : (sequence_filepath, sequence_filesize)}
        """
        all_sequences = [
            {
                sample.get('id'): sample.get('sequences')
                for sample in participant.get('samples')
            }
            for participant in participants
        ]

        seq_id_sample_id_map = {}
        # multiple reads per sequence is possible so use defaultdict(list)
        sequence_filepaths_filesizes = defaultdict(list)
        for sample_sequence in all_sequences:  # pylint: disable=R1702
            sample_internal_id = list(sample_sequence.keys())[
                0
            ]  # Extract the sample ID
            for sequences in sample_sequence.values():
                for sequence in sequences:
                    if not sequence.get('type').lower() in self.sequencing_type:
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
                                    f'Got {type(read)} read, expected dict: {read}'
                                )
                                continue
                            sequence_filepaths_filesizes[sequence.get('id')].append(
                                (read.get('location'), read.get('size'))
                            )

        return seq_id_sample_id_map, sequence_filepaths_filesizes

    def get_analysis_cram_paths_for_dataset_samples(
        self,
        sample_internal_to_external_id_map: dict[str, str],
    ) -> dict[str, dict[int, str]]:
        """
        Queries all analyses for the list of samples in the given dataset AND the seqr dataset.
        Returns a dict mapping {sample_id : (analysis_id, cram_path) }
        """
        sample_ids = list(sample_internal_to_external_id_map.keys())

        analyses = []
        for sample in sample_ids:
            analyses.extend(
                query(   # pylint: disable=unsubscriptable-object
                    SAMPLE_ANALYSIS_QUERY, {'sampleId': sample, 'analysisType': 'CRAM'}
                )['sample']['analyses']
            )
        # Report any crams missing the sequence type
        crams_with_missing_seq_type = [
            analysis.get('id')
            for analysis in analyses
            if 'sequencing_type' not in analysis['meta']
        ]
        if crams_with_missing_seq_type:
            raise ValueError(
                f'CRAMs from dataset {self.dataset} did not have sequencing_type: {crams_with_missing_seq_type}'
            )

        # Filter the analyses based on input sequence type
        analyses = [
            analysis
            for analysis in analyses
            if analysis.get('meta')['sequencing_type'] in self.sequencing_type
        ]

        # For each sample ID, collect the analysis IDs and cram paths
        sample_cram_paths: dict[str, dict[int, str]] = defaultdict(dict)
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
                sample_id = analysis['sample_ids'][0]
            except KeyError:
                # Try getting the sample ID from the 'sample' meta field
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

        all_sample_analyses: dict[str, list[dict[str, int | str]]] = defaultdict(
            list
        )

        analyses = []
        for analysis_type in ANALYSIS_TYPES:
            if analysis_type == 'CRAM':
                continue
            for sample in samples_without_crams:
                analyses.extend(
                    query(  # pylint: disable=unsubscriptable-object
                        SAMPLE_ANALYSIS_QUERY,
                        {'sampleId': sample, 'analysisType': analysis_type},
                    )['sample']['analyses']
                )

        for analysis in analyses:
            try:
                # Try getting the sample IDs from the 'sample_ids' field
                samples = analysis['sample_ids']
            except KeyError:
                # Try getting the sample IDs from the 'sample' meta field
                try:
                    samples = analysis['meta']['sample']
                # Try getting the sample ID from the 'samples' meta field
                except KeyError:
                    samples = analysis['meta']['samples']

            if not isinstance(samples, list):
                samples = [
                    samples,
                ]

            for sample_id in samples:
                if sample_id not in samples_without_crams:
                    continue
                analysis_entry = {
                    'analysis_id': analysis.get('id'),
                    'analysis_type': analysis.get('type'),
                    'analysis_output': analysis.get('output'),
                    'timestamp_completed': analysis.get('timestampCompleted'),
                }
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

    async def check_for_uningested_or_moved_sequences(  # pylint: disable=R0914
        self,
        bucket_name: str,
        sequence_filepaths_filesizes: dict[int, list[tuple[str, int]]],
        completed_samples: dict[str, list[int]],
        seq_id_sample_id_map: dict[int, str],
        sample_id_internal_external_map: dict[str, str],
    ):
        """
        Compares the sequences in a Metamist dataset to the sequences in the main-upload bucket

        Input: The dataset name, the {sequence_id : read_paths} mapping, the sequence file types,
               and the sample internal -> external ID mapping.
        Returns: 1. Paths to sequences that have not yet been ingested - checks if any completed
                    sample external IDs are in the filename and includes this in output.
                 2. A dict mapping sequence IDs to GS filepaths that have been ingested,
                    but where the path in the bucket is different to the path for this
                    sequence in Metamist. If the filenames and file sizes match,
                    these are identified as sequence files that have been moved from
                    their original location to a new location.
                 3. The paths saved in metamist for the read data that has been moved. These paths go nowhere.
        """
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
        # These metamist paths lead nowhere so we can go ahead and ignore them
        metamist_paths_to_nowhere = set(sequence_paths_in_metamist).difference(
            set(sequence_paths_in_bucket)
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
        new_sequence_path_sizes = {}
        for path in uningested_sequence_paths:
            filename = os.path.basename(path)
            # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
            if filename in metamist_sequence_file_size_map.keys():
                filesize = await self.file_size(path)
                if filesize == metamist_sequence_file_size_map.get(filename):
                    ingested_and_moved_filepaths.append(
                        (path, metamist_sequence_file_path_map.get(filename))
                    )
                    new_sequence_path_sizes[path] = filesize
        logging.info(
            f'Found {len(ingested_and_moved_filepaths)} ingested files that have been moved'
        )

        # If the file has just been moved, we consider it ingested
        uningested_sequence_paths -= {
            bucket_path for bucket_path, _ in ingested_and_moved_filepaths
        }

        # Check the list of uningested paths to see if any of them contain sample IDs for ingested samples
        # This could happen when we ingest a fastq read pair for a sample, and additional read files were provided
        # but not ingested, such as bams and vcfs.
        uningested_reads: dict[str, list[tuple[str, str]]] = defaultdict(
            list, {k: [] for k in uningested_sequence_paths}
        )
        for sample_id, analysis_ids in completed_samples.items():
            try:
                sample_ext_id = sample_id_internal_external_map[sample_id]
                for uningested_sequence in uningested_sequence_paths:
                    if sample_ext_id not in uningested_sequence:
                        continue
                    uningested_reads[uningested_sequence].append(
                        (sample_id, sample_ext_id))
            except KeyError:
                logging.warning(
                    f'{sample_id} from analyses: {analysis_ids} not found in sample map.'
                )

        # flip the sequence id : reads mapping to identify sequence IDs by their read paths
        reads_sequences = {}
        for sequence_id, reads_sizes in sequence_filepaths_filesizes.items():
            for read_size in reads_sizes:
                reads_sequences[read_size[0]] = sequence_id

        # Collect the sequences for files that have been ingested and moved to a different bucket location
        sequences_moved_paths = []
        for bucket_path, metamist_path in ingested_and_moved_filepaths:
            seq_id = reads_sequences.get(metamist_path)
            sample_id = seq_id_sample_id_map.get(seq_id)
            analysis_ids = completed_samples.get(sample_id)
            if sample_id not in EXCLUDED_SAMPLES:
                filesize = new_sequence_path_sizes[bucket_path]
                sequences_moved_paths.append(
                    SequenceReportEntry(
                        sample_id=sample_id,
                        sequence_id=seq_id,
                        sequence_file_path=bucket_path,
                        analysis_ids=analysis_ids,
                        filesize=filesize,
                    )
                )

        return uningested_reads, sequences_moved_paths, metamist_paths_to_nowhere

    async def get_reads_to_delete_or_ingest(
        self,
        bucket_name: str,
        completed_samples: dict[str, list[int]],
        sequence_filepaths_filesizes: dict[int, list[tuple[str, int]]],
        seq_id_sample_id_map: dict[int, str],
        sample_id_internal_external_map: dict[str, str],
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
            metamist_paths_to_nowhere,
        ) = await self.check_for_uningested_or_moved_sequences(
            bucket_name,
            sequence_filepaths_filesizes,
            completed_samples,
            seq_id_sample_id_map,
            sample_id_internal_external_map,
        )

        # Create a mapping of sample ID: sequence ID - use defaultdict in case a sample has several sequences
        sample_id_seq_id_map: dict[str, list[int]] = defaultdict(list)
        for sequence, sample in seq_id_sample_id_map.items():
            sample_id_seq_id_map[sample].append(sequence)

        sequence_reads_to_delete = []
        for sample_id, analysis_ids in completed_samples.items():
            if sample_id in EXCLUDED_SAMPLES:
                continue
            seq_ids = sample_id_seq_id_map[sample_id]
            for seq_id in seq_ids:
                seq_read_paths = sequence_filepaths_filesizes[seq_id]
                for path, size in seq_read_paths:
                    if path in metamist_paths_to_nowhere:
                        continue
                    filesize = size
                    sequence_reads_to_delete.append(
                        SequenceReportEntry(
                            sample_id=sample_id,
                            sequence_id=seq_id,
                            sequence_file_path=path,
                            analysis_ids=analysis_ids,
                            filesize=filesize,
                        )
                    )

        reads_to_delete = sequence_reads_to_delete + moved_sequences_to_delete

        return reads_to_delete, reads_to_ingest

    @staticmethod
    def find_crams_for_reads_to_ingest(
        reads_to_ingest: dict[str, list],
        sample_cram_paths: dict[str, dict[int, str]],
    ) -> list[tuple[str, str, str, int, str]]:
        """
        Compares the external sample IDs for samples with completed CRAMs against the
        uningested read files. This may turn up results for cases where multiple read types
        have been provided for a sample, but only one type was ingested and used for alignment.
        """
        possible_sequence_ingests = []
        for sequence_path, samples in reads_to_ingest.items():
            if not samples:
                # If no samples detected in filename, add the path in an empty tuple
                possible_sequence_ingests.append((sequence_path, '', '', 0, ''))
                continue
            for sample in samples:
                # Else get the completed CRAM analysis ID and path for the sample
                sample_internal_id, sample_ext_id = sample
                sample_cram = sample_cram_paths[sample_internal_id]
                analysis_id = int(list(sample_cram.keys())[0])
                cram_path = sample_cram[analysis_id]
                possible_sequence_ingests.append(
                    (
                        sequence_path,
                        sample_ext_id,
                        sample_internal_id,
                        analysis_id,
                        cram_path,
                    )
                )

        return possible_sequence_ingests
