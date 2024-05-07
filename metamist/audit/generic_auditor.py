import logging
import os
from collections import defaultdict, namedtuple
from datetime import datetime
from functools import cache
from typing import Any

from gql.transport.requests import log as requests_logger

from metamist.audit.audithelper import AuditHelper
from metamist.graphql import gql, query_async

ANALYSIS_TYPES_QUERY = gql(
    """
    query analysisTypes {
        enum {
            analysisType
        }
    }
    """
)

QUERY_PARTICIPANTS_SAMPLES_SGS_ASSAYS = gql(
    """
        query DatasetData($datasetName: String!) {
            project(name: $datasetName) {
                participants {
                    id
                    externalId
                    samples {
                        id
                        externalId
                        sequencingGroups {
                            id
                            type
                            assays {
                                id
                                meta
                            }
                        }
                    }
                }
            }
        }
    """
)

QUERY_SG_ANALYSES = gql(
    """
        query sgAnalyses($dataset: String!, $sgIds: [String!], $analysisTypes: [String!]) {
          sequencingGroups(id: {in_: $sgIds}, project: {eq: $dataset}) {
            id
            analyses(status: {eq: COMPLETED}, type: {in_: $analysisTypes}, project: {eq: $dataset}) {
              id
              meta
              output
              type
              timestampCompleted
            }
          }
        }
    """
)

# Variable type definitions
AssayId = int
SampleId = str
SampleExternalId = str
ParticipantExternalId = str

AssayReportEntry = namedtuple(
    'AssayReportEntry',
    'sg_id assay_id assay_file_path analysis_id filesize',
)


@cache
async def get_analysis_types():
    """Return the list of analysis types from the enum table."""
    logging.getLogger().setLevel(logging.WARN)
    analysis_types = await query_async(ANALYSIS_TYPES_QUERY)
    logging.getLogger().setLevel(logging.INFO)
    return analysis_types['enum'][  # pylint: disable=unsubscriptable-object
        'analysisType'
    ]


class GenericAuditor(AuditHelper):
    """Auditor for cloud storage buckets"""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        dataset: str,
        sequencing_types: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        if not dataset:
            raise ValueError('Metamist dataset is required')

        super().__init__(search_paths=None)

        self.dataset = dataset
        self.sequencing_types = sequencing_types
        self.file_types = file_types
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        requests_logger.setLevel(logging.WARNING)

    async def get_participant_data_for_dataset(self) -> list[dict]:
        """
        Uses a graphQL query to return all participants in a Metamist dataset.
        Returned list includes all samples and assays associated with the participants.
        """

        logging.getLogger().setLevel(logging.WARN)
        participant_query_results = await query_async(
            QUERY_PARTICIPANTS_SAMPLES_SGS_ASSAYS, {'datasetName': self.dataset}
        )
        logging.getLogger().setLevel(logging.INFO)

        participant_data = participant_query_results['project']['participants']

        filtered_participants = []
        for participant in participant_data:
            if not participant['samples']:
                logging.info(
                    f'{self.dataset} :: Filtering participant {participant["id"]} ({participant["externalId"]}) as it has no samples.'
                )
                continue
            filtered_participants.append(participant)

        return filtered_participants

    @staticmethod
    def get_most_recent_analyses_by_sg(
        analyses_list: list[dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """
        Takes a list of completed analyses for a number of sequencing groups and returns the latest
        completed analysis for each sequencing group, creating a 1:1 mapping of SG to analysis.
        """
        most_recent_analysis_by_sg = {}

        for sg_analyses in analyses_list:
            sg_id = sg_analyses['id']
            analyses = sg_analyses['analyses']
            if not analyses:
                continue
            if len(analyses) == 1:
                most_recent_analysis_by_sg[sg_id] = analyses[0]
                continue

            sorted_analyses = sorted(
                analyses,
                key=lambda x: datetime.strptime(
                    x['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
                ),
            )
            most_recent_analysis_by_sg[sg_id] = sorted_analyses[-1]

        return most_recent_analysis_by_sg

    @staticmethod
    def map_internal_to_external_sample_ids(
        participants: list[dict],
    ) -> dict[SampleId, SampleExternalId]:
        """
        Returns the {internal sample id : external sample id} mapping for all participants in a Metamist dataset
        """
        return {
            sample['id']: sample['externalId']
            for participant in participants
            for sample in participant['samples']
        }

    def get_assay_map_from_participants(
        self, participants: list[dict]
    ) -> tuple[dict[str, str], dict[int, str], dict[Any, list[tuple[Any, Any]]]]:
        """
        Input the list of Metamist participant dictionaries from the 'QUERY_PARTICIPANTS_SAMPLES_SGS_ASSAYS' query

        Returns the mappings:
        1. { sg_id       : sample_id }
        2. { assay_id    : sg_id }
        3. { assay_id    : (read_filepath, read_filesize,) }
        """

        sg_sample_id_map = {}
        assay_sg_id_map = {}
        assay_filepaths_filesizes = defaultdict(list)

        sample_sgs = {
            sample['id']: sample['sequencingGroups']
            for participant in participants
            for sample in participant['samples']
        }
        for sample_id, sgs in sample_sgs.items():
            for sg in sgs:
                if sg['type'].lower() not in self.sequencing_types:
                    continue

                sg_sample_id_map[sg['id']] = sample_id
                for assay in sg['assays']:
                    reads = assay['meta'].get('reads')
                    if not reads:
                        logging.warning(
                            f'{self.dataset} :: SG {sg["id"]} assay {assay["id"]} has no reads field'
                        )
                        continue

                    if assay_sg_id_map.get(assay['id']):
                        raise ValueError(
                            f'{self.dataset} :: Assay {assay["id"]} has multiple SGs: {assay_sg_id_map[assay["id"]]} and {sg["id"]}'
                        )
                    assay_sg_id_map[assay['id']] = sg['id']

                    if isinstance(reads, dict):
                        assay_filepaths_filesizes[assay['id']].append(
                            (
                                reads.get('location'),
                                reads.get('size'),
                            )
                        )
                        continue

                    for read in reads:
                        if not isinstance(read, dict):
                            logging.error(
                                f'{self.dataset} :: Got {type(read)} read for SG {sg["id"]}, expected dict: {read}'
                            )
                            continue

                        assay_filepaths_filesizes[assay['id']].append(
                            (
                                read.get('location'),
                                read.get('size'),
                            )
                        )

        return sg_sample_id_map, assay_sg_id_map, assay_filepaths_filesizes

    async def get_analysis_cram_paths_for_dataset_sgs(
        self,
        assay_sg_id_map: dict[int, str],
    ) -> dict[str, dict[int, str]]:
        """
        Fetches all CRAMs for the list of sgs in the given dataset.
        Returns a dict mapping {sg_id : (analysis_id, cram_path) }
        """
        sg_ids = list(assay_sg_id_map.values())

        logging.getLogger().setLevel(logging.WARN)
        analyses_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {'dataset': self.dataset, 'sgId': sg_ids, 'analysisTypes': ['CRAM']},
        )
        logging.getLogger().setLevel(logging.INFO)

        analyses = analyses_query_result['sequencingGroups']
        analyses = self.get_most_recent_analyses_by_sg(analyses_list=analyses)

        # Report any crams missing the sequencing type
        crams_with_missing_seq_type = [
            analysis['id']
            for analysis in analyses.values()
            if 'sequencing_type' not in analysis['meta']
        ]
        if crams_with_missing_seq_type:
            raise ValueError(
                f'{self.dataset} :: CRAM analyses are missing sequencing_type field: {crams_with_missing_seq_type}'
            )

        # For each sg id, collect the analysis id and cram paths
        sg_cram_paths: dict[str, dict[int, str]] = defaultdict(dict)
        for sg_id, analysis in analyses.items():
            cram_path = analysis['output']
            if not cram_path.startswith('gs://') or not cram_path.endswith('.cram'):
                logging.warning(
                    f'Analysis {analysis["id"]} invalid output path: {analysis["output"]}'
                )
                continue

            sg_cram_paths[sg_id][analysis['id']] = analysis['output']

        return sg_cram_paths

    async def analyses_for_sgs_without_crams(self, sgs_without_crams: list[str]):
        """Checks if other completed analyses exist for samples without completed crams"""

        all_sg_analyses: dict[str, list[dict[str, int | str]]] = defaultdict(list)

        logging.getLogger().setLevel(logging.WARN)
        sg_analyse_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {
                'dataset': self.dataset,
                'sgIds': sgs_without_crams,
                'analysisTypes': [t for t in await get_analysis_types() if t != 'cram'],
            },
        )
        logging.getLogger().setLevel(logging.INFO)

        sg_analyses = sg_analyse_query_result['sequencingGroups']

        for sg_analysis in sg_analyses:
            sg_id = sg_analysis['id']

            for analysis in sg_analysis['analyses']:
                analysis_entry = {
                    'analysis_id': analysis['id'],
                    'analysis_type': analysis['type'],
                    'analysis_output': analysis['output'],
                    'timestamp_completed': analysis['timestampCompleted'],
                }
                all_sg_analyses[sg_id].append(analysis_entry)

        if all_sg_analyses:
            for sg_without_cram, completed_analyses in all_sg_analyses.items():
                for completed_analysis in completed_analyses:
                    logging.warning(
                        f'{self.dataset} :: SG {sg_without_cram} missing CRAM but has analysis {completed_analysis}'
                    )

    async def get_complete_and_incomplete_sgs(
        self,
        assay_sg_id_map: dict[int, str],
        sg_cram_paths: dict[str, dict[int, str]],
    ) -> dict[str, Any]:
        """
        Returns a dictionary containing two categories of sequencing groups:
         - the completed sgs which have finished aligning and have a cram, as a dict mapping
            the sg_id to the analysis_ids
         - the incomplete sgs where the alignment hasn't completed and no cram exists, as a list
        """
        # Get all the unique cram paths to check
        cram_paths = set()
        for analyses in sg_cram_paths.values():
            cram_paths.update(list(analyses.values()))

        # Check the analysis CRAM paths actually exist in the bucket
        buckets_subdirs = self.get_gcs_bucket_subdirs_to_search(list(cram_paths))
        crams_in_bucket = self.find_files_in_gcs_buckets_subdirs(
            buckets_subdirs,
            ('cram',),
        )

        # Incomplete SGs initialised as the SGs without a completed CRAM
        incomplete_sgs = set(assay_sg_id_map.values()).difference(
            set(sg_cram_paths.keys())
        )

        # Completed SGs have a CRAM file in the bucket that matches the path in Metamist
        completed_sgs = {}
        for sg_id, analysis in sg_cram_paths.items():
            for analysis_id, cram_path in analysis.items():
                if cram_path in crams_in_bucket:
                    completed_sgs[sg_id] = analysis_id
                    continue
                incomplete_sgs.update(sg_id)

        if incomplete_sgs:
            logging.warning(
                f'{self.dataset} :: {len(incomplete_sgs)} SGs without CRAMs found: {list(incomplete_sgs)}'
            )
            await self.analyses_for_sgs_without_crams(list(incomplete_sgs))

        return {'complete': completed_sgs, 'incomplete': list(incomplete_sgs)}

    async def check_for_uningested_or_moved_assays(  # pylint: disable=R0914
        self,
        bucket_name: str,
        assay_filepaths_filesizes: dict[int, list[tuple[str, int]]],
        completed_sgs: dict[str, list[int]],
        sg_sample_id_map: dict[str, str],
        assay_sg_id_map: dict[int, str],
        sample_internal_external_id_map: dict[str, str],
    ):
        """
        Compares the assays read files in a Metamist dataset to the read files found in the
        main-upload bucket.

        Input:  The upload bucket name, the {assay_id : (read_path, read_size)} mapping,
                the completed SGs, the { SG_ID : sample_ID } mapping, the { assay_id : SG_ID } mapping,
                and the { sample_id : sample_external_id } mapping.

        Returns: 1. Paths to assays that have not yet been ingested - checks if any completed
                    sample external IDs are in the filename and includes this in output.
                 2. A dict mapping assay IDs to GS filepaths that have been ingested,
                    but where the path in the bucket is different to the path for this
                    assay in Metamist. If the filenames and file sizes match,
                    these are identified as assay files that have been moved from
                    their original location to a new location.
                 3. The assay read files that have been deleted/moved.
        """
        # Get all the paths to assay data anywhere in the main-upload bucket
        assay_paths_in_bucket = self.find_assay_files_in_gcs_bucket(
            bucket_name, self.file_types
        )

        # Flatten all the Metamist assay file paths and sizes into a single list
        assay_paths_sizes_in_metamist: list[tuple[str, int]] = []
        for assay in assay_filepaths_filesizes.values():
            assay_paths_sizes_in_metamist.extend(assay)

        # Find the paths that exist in the bucket and not in metamist
        assay_paths_in_metamist = [
            path_size[0] for path_size in assay_paths_sizes_in_metamist
        ]
        uningested_assay_paths = set(assay_paths_in_bucket).difference(
            set(assay_paths_in_metamist)
        )
        # Find the paths that exist in metamist and not in the bucket
        metamist_paths_to_nowhere = set(assay_paths_in_metamist).difference(
            set(assay_paths_in_bucket)
        )

        # Strip the metamist paths into just filenames
        # Map each file name to its file size and path
        metamist_assay_file_size_map = {
            os.path.basename(path_size[0]): path_size[1]
            for path_size in assay_paths_sizes_in_metamist
        }
        metamist_assay_file_path_map = {
            os.path.basename(path_size[0]): path_size[0]
            for path_size in assay_paths_sizes_in_metamist
        }

        # Identify if any paths are to files that have actually just been moved
        # by checking if they are in the bucket but not metamist
        ingested_and_moved_filepaths = []
        new_assay_path_sizes = {}
        for path in uningested_assay_paths:
            filename = os.path.basename(path)
            # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
            if filename in metamist_assay_file_size_map.keys():
                filesize = await self.file_size(path)
                if filesize == metamist_assay_file_size_map.get(filename):
                    ingested_and_moved_filepaths.append(
                        (path, metamist_assay_file_path_map.get(filename))
                    )
                    new_assay_path_sizes[path] = filesize
        logging.info(
            f'Found {len(ingested_and_moved_filepaths)} ingested files that have been moved'
        )

        # If the file has just been moved, we consider it ingested
        uningested_assay_paths -= {
            bucket_path for bucket_path, _ in ingested_and_moved_filepaths
        }

        # Check the list of uningested paths to see if any of them contain sample ids for ingested samples
        # This could happen when we ingest a fastq read pair for a sample, and additional read files were provided
        # but not ingested, such as bams and vcfs.
        uningested_reads: dict[str, list[tuple[str, str, str]]] = defaultdict(
            list, {k: [] for k in uningested_assay_paths}
        )
        for sg_id, analysis_ids in completed_sgs.items():
            try:
                sample_id = sg_sample_id_map[sg_id]
                sample_ext_id = sample_internal_external_id_map[sample_id]
                for uningested_assay in uningested_assay_paths:
                    if sample_ext_id not in uningested_assay:
                        continue
                    uningested_reads[uningested_assay].append(
                        (sg_id, sample_id, sample_ext_id)
                    )
            except KeyError:
                logging.warning(
                    f'{sg_id} from analyses: {analysis_ids} not found in SG-sample map.'
                )

        # flip the assay id : reads mapping to identify assays by their reads
        reads_assays = {}
        for assay_id, reads_sizes in assay_filepaths_filesizes.items():
            for read_size in reads_sizes:
                reads_assays[read_size[0]] = assay_id

        # Collect the assays for files that have been ingested and moved to a different bucket location
        assays_moved_paths = []
        for bucket_path, metamist_path in ingested_and_moved_filepaths:
            assay_id = reads_assays.get(metamist_path)
            sg_id = assay_sg_id_map.get(assay_id)
            analysis_id = completed_sgs.get(sg_id)
            if sg_id not in AuditHelper.EXCLUDED_SGS and analysis_id:
                filesize = new_assay_path_sizes[bucket_path]
                assays_moved_paths.append(
                    AssayReportEntry(
                        sg_id=sg_id,
                        assay_id=assay_id,
                        assay_file_path=bucket_path,
                        analysis_id=analysis_id,
                        filesize=filesize,
                    )
                )

        return uningested_reads, assays_moved_paths, metamist_paths_to_nowhere

    async def get_reads_to_delete_or_ingest(
        self,
        bucket_name: str,
        completed_sgs: dict[str, list[int]],
        assay_filepaths_filesizes: dict[int, list[tuple[str, int]]],
        sg_sample_id_map: dict[str, str],
        assay_sg_id_map: dict[int, str],
        sample_internal_external_id_map: dict[str, str],
    ) -> tuple[list, list]:
        """
        Inputs: 1. List of samples which have completed CRAMs
                2. Dictionary mapping of assay IDs to assay file paths and file sizes
                3. Dictionary mapping sample IDs to assay IDs
                4. Dictionary mapping assay IDs to sample IDs
        Returns a tuple of two lists, each containing assays IDs and assay file paths.
        The first containins reads which can be deleted, the second containing reads to ingest.
        The sample id, assay id, and analysis id (of completed cram) are included in the delete list.
        """
        # Check for uningested assay data that may be hiding or assay data that has been moved
        (
            reads_to_ingest,
            moved_assays_to_delete,
            metamist_paths_to_nowhere,
        ) = await self.check_for_uningested_or_moved_assays(
            bucket_name,
            assay_filepaths_filesizes,
            completed_sgs,
            sg_sample_id_map,
            assay_sg_id_map,
            sample_internal_external_id_map,
        )

        # Create a mapping of sg id: assay id - use defaultdict in case a sg has several assays
        sg_assays_id_map: dict[str, list[int]] = defaultdict(list)
        for assay_id, sg_id in assay_sg_id_map.items():
            sg_assays_id_map[sg_id].append(assay_id)

        assay_reads_to_delete = []
        for sg_id, analysis_id in completed_sgs.items():
            if sg_id in AuditHelper.EXCLUDED_SGS:
                continue
            assay_ids = sg_assays_id_map[sg_id]
            for assay_id in assay_ids:
                assay_read_paths = assay_filepaths_filesizes[assay_id]
                for path, size in assay_read_paths:
                    if path in metamist_paths_to_nowhere:
                        continue
                    filesize = size
                    assay_reads_to_delete.append(
                        AssayReportEntry(
                            sg_id=sg_id,
                            assay_id=assay_id,
                            assay_file_path=path,
                            analysis_id=analysis_id,
                            filesize=filesize,
                        )
                    )

        reads_to_delete = assay_reads_to_delete + moved_assays_to_delete

        return reads_to_delete, reads_to_ingest

    @staticmethod
    def find_crams_for_reads_to_ingest(
        reads_to_ingest: dict[str, list],
        sg_cram_paths: dict[str, dict[int, str]],
    ) -> list[tuple[str, str, str, str, int, str]]:
        """
        Compares the external sample IDs for samples with completed CRAMs against the
        uningested read files. This may turn up results for cases where multiple read types
        have been provided for a sample, but only one type was ingested and used for alignment.
        """
        possible_assay_ingests = []
        for assay_path, sample_tuples in reads_to_ingest.items():
            if not sample_tuples:
                # If no samples detected in filename, add the path in an empty tuple
                possible_assay_ingests.append((assay_path, '', '', '', 0, ''))
                continue
            for sample_tuple in sample_tuples:
                # Else get the completed CRAM analysis id and path for the sample
                sg_id, sample_id, sample_external_id = sample_tuple
                sg_cram = sg_cram_paths[sg_id]
                analysis_id = int(list(sg_cram.keys())[0])
                cram_path = sg_cram[analysis_id]
                possible_assay_ingests.append(
                    (
                        assay_path,
                        sg_id,
                        sample_id,
                        sample_external_id,
                        analysis_id,
                        cram_path,
                    )
                )

        return possible_assay_ingests
