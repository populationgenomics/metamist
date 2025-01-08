import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from cpg_utils.config import config_retrieve, dataset_path
from gql.transport.requests import log as requests_logger

from metamist.audit.audithelper import AuditHelper, FILE_TYPES_MAP
from metamist.graphql import gql, query_async

handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False



QUERY_PARTICIPANTS_SAMPLES_SGS = gql(
    """
        query DatasetData($datasetName: String!, $seqTypes: [String!], $seqTechs: [String!]) {
            project(name: $datasetName) {
                participants {
                    id
                    externalId
                    samples {
                        id
                        externalId
                        sequencingGroups(type: {in_: $seqTypes}, technology: {in_: $seqTechs}) {
                            id
                            type
                        }
                    }
                }
            }
        }
    """
)

QUERY_DATASET_SGS = gql(
    """
        query DatasetData($datasetName: String!, $seqTypes: [String!], $seqTechs: [String!]) {
            project(name: $datasetName) {
                sequencingGroups(type: {in_: $seqTypes}, technology: {in_: $seqTechs}) {
                    id
                    type
                    technology
                    sample {
                        id
                        externalId
                        participant {
                            id
                            externalId
                        }
                    }
                    assays {
                        id
                        meta
                        sample {
                            id
                            externalId
                            participant {
                                id
                                externalId
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
        query sgAnalyses($dataset: String!, $sgIds: [String!], $analysisTypes: [String!], $seqTechs: [String!]) {
          sequencingGroups(id: {in_: $sgIds}, project: {eq: $dataset}) {
            id
            analyses(status: {eq: COMPLETED}, type: {in_: $analysisTypes}, project: {eq: $dataset}) {
              id
              meta
              outputs
              type
              timestampCompleted
            }
          }
        }
    """
)

# Variable type definitions
AnalysisId = int
AssayId = int
ParticipantId = int
ParticipantExternalId = str
SampleId = str
SampleExternalId = str
SequencingGroupId = str


class AuditReportEntry:
    """Class to hold the data for an audit report entry"""

    def __init__(
        self,
        file_path: str,
        filesize: int,
        sg_id: str | None = None,
        assay_id: int | None = None,
        cram_analysis_id: int | None = None,
        cram_file_path: str | None = None,
        sample_id: str | None = None,
        sample_external_id: str | None = None,
        participant_id: int | None = None,
        participant_external_id: str | None = None,
    ):
        self.file_path = file_path
        self.filesize = filesize
        self.sg_id = sg_id
        self.assay_id = assay_id
        self.cram_analysis_id = cram_analysis_id
        self.cram_file_path = cram_file_path
        self.sample_id = sample_id
        self.sample_external_id = sample_external_id
        self.participant_id = participant_id
        self.participant_external_id = participant_external_id


class ParticipantData:
    """Class to hold the data for a participant"""
    
    def __init__(
        self,
        id: ParticipantId,
        external_id: ParticipantExternalId,
    ):
        self.id = id
        self.external_id = external_id

class SampleData:
    """Class to hold the data for a sample"""

    def __init__(
        self,
        id: SampleId,
        external_id: SampleExternalId,
        participant: ParticipantData,
    ):
        self.id = id
        self.external_id = external_id
        self.participant = participant


class AssayData:
    """Class to hold the data for an assay"""

    def __init__(
        self,
        id: AssayId,
        read_files_paths_sizes: list[tuple[str, int]],
        sample: SampleData,
    ):
        self.id = id
        self.read_files_paths_sizes = read_files_paths_sizes
        self.sample = sample

class SequencingGroupData:
    """Class to hold the data for a sequencing group"""

    def __init__(
        self, 
        id: str,
        sequencing_type: str,
        sequencing_technology: str,
        sample: SampleData,
        assays: list[AssayData],
    ):
        self.id = id
        self.sequencing_type = sequencing_type
        self.sequencing_technology = sequencing_technology
        self.sample = sample
        self.assays = assays


class GenericAuditor(AuditHelper):
    """Auditor for cloud storage buckets"""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        dataset: str,
        sequencing_types: list[str],
        sequencing_technologies: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        # Initialize dataset
        self.dataset = dataset or config_retrieve(['workflow', 'dataset'])
        if not self.dataset:
            raise ValueError('Metamist dataset is required')

        # Validate sequencing types
        if sequencing_types == ('all',):
            self.sequencing_types = self.all_sequencing_types
        else:
            invalid_types = [st for st in sequencing_types if st not in self.all_sequencing_types]
            if invalid_types:
                raise ValueError(
                    f'Input sequencing types "{invalid_types}" must be in the allowed types: {self.all_sequencing_types}'
                )
            self.sequencing_types = sequencing_types

        # Validate file types
        if file_types in (('all',), ('all_reads',)):
            self.file_types = FILE_TYPES_MAP[file_types[0]]
        else:
            invalid_files = [ft for ft in file_types if ft not in FILE_TYPES_MAP]
            if invalid_files:
                raise ValueError(
                    f'Input file types "{invalid_files}" must be in the allowed types: {", ".join(FILE_TYPES_MAP.keys())}'
                )
            self.file_types = file_types

        # Set remaining attributes
        self.sequencing_technologies = sequencing_technologies
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        # Calculate bucket name
        self.bucket_name = dataset_path(dataset=self.dataset, category='upload')

        super().__init__(search_paths=None)
        requests_logger.setLevel(logging.WARNING)
    
    async def get_sgs_for_dataset(self) -> list[SequencingGroupData]:
        """
        Fetches all sequencing groups for the given dataset, including the assays for each sequencing group.
        
        Returns a list of SequencingGroupData objects.
        """
        logger.info(f'{self.dataset} :: Fetching SG assays for {self.sequencing_types} sequencing types')
        dataset_sgs_query_result = await query_async(
            QUERY_DATASET_SGS, 
            {'datasetName': self.dataset, 'seqTypes': self.sequencing_types, 'seqTechs': self.sequencing_technologies},
        )
        dataset_sgs = dataset_sgs_query_result['project']['sequencingGroups']
        
        return [self.get_sg_data(sg) for sg in dataset_sgs]
    
    
    def get_sg_data(self, sg: dict[str, Any]) -> SequencingGroupData:
        """Parse a sequencing group dictionary into a SequencingGroupData object"""
        return SequencingGroupData(
            id=sg['id'],
            sequencing_type=sg['type'],
            sequencing_technology=sg['technology'],
            sample=SampleData(
                id=sg['sample']['id'],
                external_id=sg['sample']['externalId'],
                participant=ParticipantData(
                    id=sg['sample']['participant']['id'],
                    external_id=sg['sample']['participant']['externalId'],
                ),
            ),
            assays=[
                self.parse_assay_data(assay) for assay in sg['assays']
            ],
        )
        
        
    def parse_assay_data(self, assay: dict[str, Any]) -> AssayData:
        """Parse an assay dictionary into an AssayData object"""
        reads = assay['meta']['reads']
        if isinstance(assay['meta']['reads'], dict):
            reads = [reads]
        
        reads_files_paths_sizes = []
        for read in reads:
            reads_files_paths_sizes.append(
                (
                    read['location'],
                    read['size'],
                )
            )
            if 'secondaryFiles' in read:
                for secondary_file in read['secondaryFiles']:
                    reads_files_paths_sizes.append(
                        (
                            secondary_file['location'],
                            secondary_file['size'],
                        )
                    )
            
        return AssayData(
            id=assay['id'],
            read_files_paths_sizes=reads_files_paths_sizes,
            sample=SampleData(
                id=assay['sample']['id'],
                external_id=assay['sample']['externalId'],
                participant=ParticipantData(
                    id=assay['sample']['participant']['id'],
                    external_id=assay['sample']['participant']['externalId'],
                ),
            ),
        )
        
        
    def get_latest_analyses_by_sg(
        self,
        all_sg_analyses: list[dict[str, Any]],
    ) -> dict[SequencingGroupId, dict[str, Any]]:
        """
        Takes a list of completed analyses for a number of sequencing groups and returns the latest
        completed analysis for each sequencing group, creating a 1:1 mapping of SG to analysis.
        """
        latest_analysis_by_sg = {}

        for sg_analyses in all_sg_analyses:
            sg_id = sg_analyses['id']
            analyses = sg_analyses['analyses']
            if not analyses:
                continue
            if len(analyses) == 1:
                latest_analysis_by_sg[sg_id] = analyses[0]
                continue

            sorted_analyses = sorted(
                analyses,
                key=lambda x: datetime.strptime(
                    x['timestampCompleted'], '%Y-%m-%dT%H:%M:%S'
                ),
            )
            latest_analysis_by_sg[sg_id] = sorted_analyses[-1]

        # Check the analysis meta data for the sequencing type
        self.check_analyses_seq_type(list(latest_analysis_by_sg.values()))
        
        return latest_analysis_by_sg


    def check_analyses_seq_type(
        self,
        analyses: list[dict[str, Any]],
    ):
        """Check the analysis meta data for the sequencing type"""
        analyses_with_missing_seq_type = [
            (analysis['id'], analysis['type'])
            for analysis in analyses
            if 'sequencing_type' not in analysis['meta']
        ]
        if analyses_with_missing_seq_type:
            raise ValueError(
                f'{self.dataset} :: Analyses are missing sequencing_type field: {analyses_with_missing_seq_type}'
            )


    async def get_analysis_cram_paths_for_dataset_sgs(
        self,
        sequencing_groups: list[SequencingGroupData],
    ) -> dict[SequencingGroupId, dict[AnalysisId, str]]:
        """
        Fetches all CRAMs for the list of sgs in the given dataset.
        Returns a dict mapping {sg_id : (cram_analysis_id, cram_path) }
        """
        sg_ids = [sg.id for sg in sequencing_groups]
        logging.info(f'{self.dataset} :: Fetching CRAM analyses for {len(set(sg_ids))} SGs')

        sg_analyses_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {'dataset': self.dataset, 'sgId': sg_ids, 'analysisTypes': ['CRAM']},
        )

        sg_analyses = sg_analyses_query_result['sequencingGroups']
        latest_analysis_by_sg = self.get_latest_analyses_by_sg(all_sg_analyses=sg_analyses)

        # For each sg id, collect the analysis id and cram paths
        sg_cram_paths: dict[str, dict[int, str]] = defaultdict(dict)
        for sg_id, analysis in latest_analysis_by_sg.items():
            cram_path = analysis['outputs']['path']
            if not cram_path.startswith('gs://') or not cram_path.endswith('.cram'):
                logging.warning(
                    f'Analysis {analysis["id"]} invalid output path: {analysis["output"]}'
                )
                continue

            sg_cram_paths[sg_id] = {analysis['id']: cram_path}

        return sg_cram_paths


    async def check_for_non_cram_analyses(self, sgs_without_crams: list[str]) -> None:
        """Checks if other completed analyses exist for sequencing groups without a completed cram analysis"""
        sg_analyse_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {
                'dataset': self.dataset,
                'sgIds': sgs_without_crams,
                'analysisTypes': [t for t in self.all_analysis_types if t != 'cram'],
            },
        )
        sg_analyses = sg_analyse_query_result['sequencingGroups']

        for sg_analysis in sg_analyses:
            sg_id = sg_analysis['id']
            if not sg_analysis['analyses']:
                continue
            logging.warning(
                f'{self.dataset} :: SG {sg_id} missing CRAM but has analyses:'
            )
            for analysis in sg_analysis['analyses']:
                logging.warning(
                    f'{analysis["id"]} - {analysis["type"]} - {analysis["outputs"].get("path")}'
                )


    async def get_complete_and_incomplete_sgs(
        self,
        sequencing_groups: list[SequencingGroupData],
        sg_cram_paths: dict[SequencingGroupId, dict[AnalysisId, str]],
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
        buckets_prefixes = self.get_gcs_buckets_and_prefixes_from_paths(list(cram_paths))
        crams_in_bucket = self.find_files_in_gcs_buckets_prefixes(
            buckets_prefixes,
            ('cram',),
        )

        # Incomplete SGs initialised as the SGs without a completed CRAM
        incomplete_sgs = set([sg.id for sg in sequencing_groups]).difference(
            set(sg_cram_paths.keys())
        )

        # Completed SGs have a CRAM file in the bucket that matches the path in Metamist analysis record
        # Incomplete SGs have a CRAM analysis record in Metamist but are not found at that path in the bucket
        completed_sgs = {}
        for sg_id, analysis in sg_cram_paths.items():
            for cram_analysis_id, cram_path in analysis.items():
                if cram_path in crams_in_bucket:
                    completed_sgs[sg_id] = cram_analysis_id
                else:
                    logging.warning(
                        f'{self.dataset} :: SG {sg_id} has CRAM analysis: {cram_analysis_id} - but file not found at path: {cram_path}'
                    )
                    incomplete_sgs.update(sg_id)

        if incomplete_sgs:
            logging.warning(
                f'{self.dataset} :: {len(incomplete_sgs)} SGs without CRAMs found: {list(incomplete_sgs)}'
            )
            logging.warning('Checking if any other analyses exist for these SGs, which would be unexpected...')
            await self.check_for_non_cram_analyses(list(incomplete_sgs))

        return {'complete': completed_sgs, 'incomplete': list(incomplete_sgs)}


    async def check_for_uningested_or_moved_assays(  # pylint: disable=R0914
        self,
        bucket_name: str,
        sequencing_groups: list[SequencingGroupData],
        completed_sgs: dict[SequencingGroupId, list[AnalysisId]],
        assay_id_to_paths_and_sizes: dict[AssayId, list[tuple[str, int]]],
    ) -> tuple[list[AuditReportEntry], list[AuditReportEntry], set[str]]:
        """
        Compares the assays read files in a Metamist dataset to the read files found in the
        upload bucket.

        Input:  
            - bucket_name: The name of the GCS bucket to check
            - sequencing_groups: A list of SequencingGroupData objects
            - completed_sgs: A dict mapping sg_ids to analysis_ids for completed CRAM analyses
            - assay_id_to_paths_and_sizes: A dict mapping assay IDs to lists of tuples of read file paths and sizes

        Returns: 1. A list of audit report records for reads that have not been ingested,
                    but where a known sample ID exists in the read file path.
                 2. A list of audit report records for reads that have been ingested,
                    but have been moved to a different location in the bucket.
                 3. A set of string paths to the assay read files that have been 
                    deleted/moved from their original location in Metamist.
        """
        # Get a list of all the paths and sizes of assay files recorded in Metamist
        metamist_assay_paths_sizes: list[tuple[str, int]] = [
            path_size for assay in assay_id_to_paths_and_sizes.values() for path_size in assay
        ]
        metamist_assay_paths = set(
            [path for path, _ in metamist_assay_paths_sizes]
        )

        # Get a list of all the paths and sizes of assay files anywhere in the upload bucket
        bucket_assay_paths_sizes = self.find_assay_files_in_gcs_bucket(
            bucket_name, self.file_types
        )
        bucket_assay_paths = set(bucket_assay_paths_sizes.keys())
        
        # Find the paths that exist in the bucket and not in Metamist
        uningested_assay_paths = set(bucket_assay_paths).difference(
            set(metamist_assay_paths)
        )
        # Find the paths that exist in Metamist and not in the bucket
        metamist_paths_to_nowhere = set(metamist_assay_paths).difference(
            set(bucket_assay_paths)
        )

        # Strip the metamist paths into just filenames
        # Map each file name to its file size and path
        # This is used to identify if any files have been moved
        metamist_assay_file_size_map = {
            os.path.basename(path): {'size': size, 'path': path}
            for path, size in metamist_assay_paths_sizes
        }

        # Check if any of the uningested paths are actually files that have been moved
        ingested_reads_that_were_moved = self.check_if_assay_files_were_moved(
            uningested_assay_paths,
            metamist_assay_file_size_map,
            bucket_assay_paths_sizes,
        )
        
        # Check if any of the uningested paths contain sample IDs for completed SGs 
        uningested_reads = self.check_uningested_assays_for_sample_ids(
            sequencing_groups,
            uningested_assay_paths,
            bucket_assay_paths_sizes,
            completed_sgs,
        )

        return uningested_reads, ingested_reads_that_were_moved, metamist_paths_to_nowhere
    

    def check_if_assay_files_were_moved(
        self,
        sequencing_groups: list[SequencingGroupData],
        completed_sgs: dict[str, list[int]],
        uningested_assay_paths: set[str],
        assay_id_to_paths_and_sizes: dict[int, list[tuple[str, int]]],
        metamist_assay_paths_sizes: dict[str, dict[str, Any]],
        bucket_assay_paths_sizes: dict[str, int],
    ) -> list[AuditReportEntry]: 
        """
        Identify if any paths are to files that have actually just been moved
        by checking if they are in the bucket but not Metamist. If they are,
        check if the file size is the same as the file in Metamist. If so,
        assume the file has been moved and add it to the list of ingested and moved
        files.
        
        Returns a tuple of two lists, the first containing the paths of ingested and moved files,
        the second containing the assay report data for these files
        """
        ingested_and_moved_filepaths = []
        new_assay_path_sizes = {}
        for bucket_path in uningested_assay_paths:
            filename = os.path.basename(bucket_path)
            # If the file in the bucket has the exact same name and size as one in metamist, assume its the same
            if filename in metamist_assay_paths_sizes:
                metamist_file_path = metamist_assay_paths_sizes[filename]['path']
                metamist_file_size = metamist_assay_paths_sizes[filename]['size']
                bucket_file_size = bucket_assay_paths_sizes[bucket_path]
                if bucket_file_size == metamist_file_size:
                    ingested_and_moved_filepaths.append(
                        {
                            'bucket_path': bucket_path,
                            'metamist_path': metamist_file_path,
                            'size': bucket_file_size,
                        }
                    )
                    new_assay_path_sizes[bucket_path] = bucket_file_size
                else:
                    logging.warning(
                        f'Uningested file at {bucket_path} ({bucket_file_size}) is similar to file in Metamist: {metamist_file_path} ({metamist_file_size}) but has different size'
                    )
        logging.info(
            f'Found {len(ingested_and_moved_filepaths)} ingested files that have been moved'
        )
        
        # If the file has just been moved, we consider it ingested
        uningested_assay_paths.remove(
            {bucket_path for bucket_path, _ in ingested_and_moved_filepaths}
        )
        
        # flip the assay id : reads mapping to identify assays by their reads
        read_file_path_to_assay_id = {}
        for assay_id, reads_sizes in assay_id_to_paths_and_sizes.items():
            for read, _ in reads_sizes:
                read_file_path_to_assay_id[read] = assay_id

        assay_sg_id_map = {assay.id: sg.id for sg in sequencing_groups for assay in sg.assays}
        
        assays_moved_paths = []
        for ingested_and_moved_path in ingested_and_moved_filepaths:

            assay_id = read_file_path_to_assay_id.get(ingested_and_moved_path['metamist_path'])
            
            sg_id = assay_sg_id_map.get(assay_id)
            cram_analysis_id = completed_sgs.get(sg_id)[0] if sg_id in completed_sgs else None
            
            if sg_id in self.excluded_sequencing_groups or not cram_analysis_id:
                continue
            
            sg = self.get_sequencing_group_data_by_id(sg_id, sequencing_groups)
            if not sg:
                continue

            assays_moved_paths.append(
                AuditReportEntry(
                    file_path=ingested_and_moved_path['bucket_path'],
                    filesize=ingested_and_moved_path['filesize'],
                    sg_id=sg_id,
                    assay_id=assay_id,
                    cram_analysis_id=cram_analysis_id,
                    sample_id=sg.sample.id,
                    sample_external_id=sg.sample.external_id,
                    participant_id=sg.sample.participant.id,
                    participant_external_id=sg.sample.participant.external_id,
                )
            )

        return assays_moved_paths
    
    def get_sequencing_group_data_by_id(
        self,
        sg_id: str,
        sequencing_groups: list[SequencingGroupData],
    ):
        """Get the sequencing group data for a given sg_id"""
        for sg in sequencing_groups:
            if sg.id == sg_id:
                return sg
        return None


    def check_uningested_assays_for_sample_ids(
        self,
        sequencing_groups: list[SequencingGroupData],
        uningested_assay_paths: set[str],
        bucket_assay_paths_sizes: dict[str, int],
        completed_sgs: dict[SequencingGroupId, list[AnalysisId]],
    ) -> list[AuditReportEntry]:
        """
        Combs through the list of uningested assay paths to see if any of them contain sample ids for completed SGs.
        Can happen when we ingest a fastq read pair for a sample, and additional read files were provided (e.g. bams, vcfs).
        If there are extra files for a completed SG, we should either ingest them or delete them.
        """
        sg_sample_map = {sg.id: sg.sample for sg in sequencing_groups}
        uningested_reads = []
        for sg_id, analysis_ids in completed_sgs.items():
            try:
                sample = sg_sample_map[sg_id]
                for uningested_read_file in uningested_assay_paths:
                    if sample.external_id not in uningested_read_file or sample.participant.external_id not in uningested_read_file:
                        continue
                    uningested_reads.append(
                        AuditReportEntry(
                            file_path=uningested_read_file,
                            filesize=bucket_assay_paths_sizes[uningested_read_file],
                            sg_id=sg_id,
                            cram_analysis_id=analysis_ids[0],
                            sample_id=sample.id, 
                            sample_external_id=sample.external_id,
                            participant_id=sample.participant.id,
                            participant_external_id=sample.participant.external_id,
                        )
                    )
            except KeyError:
                logging.warning(
                    f'{sg_id} from analyses: {analysis_ids} not found in SG-sample map.'
                )
        
        return uningested_reads
        
    
    async def get_reads_to_delete_or_ingest(
        self,
        bucket_name: str,
        sequencing_groups: list[SequencingGroupData],
        completed_sgs: dict[SequencingGroupId, list[AnalysisId]],
        assay_id_to_paths_and_sizes: dict[AssayId, list[tuple[str, int]]],
    ) -> tuple[list[AuditReportEntry], list[AuditReportEntry]]:
        """
        Inputs: 
            - bucket_name: The name of the GCS bucket to check
            - sequencing_groups: A list of SequencingGroupData objects
            - completed_sgs: A dict mapping sg_ids to analysis_ids for completed CRAM analyses
            - assay_id_to_paths_and_sizes: A dict mapping assay IDs to lists of tuples of read file paths and sizes
        
        Returns two lists, each containing AuditReportEntry objects.
        The first containins reads which can be deleted, the second containing reads to ingest.
        The sample id, assay id, and analysis id (of completed cram) are included in the delete list.
        """
        # Check for uningested assay data that may be hiding or assay data that has been moved
        (
            reads_to_ingest,
            moved_assay_report_entries,
            metamist_paths_to_nowhere,
        ) = await self.check_for_uningested_or_moved_assays(
            bucket_name,
            sequencing_groups,
            completed_sgs,
            assay_id_to_paths_and_sizes,
        )

        # Create a mapping of sg id: assay ids
        sg_assays_id_map = {sg.id: [assay.id for assay in sg.assays] for sg in sequencing_groups}
        
        # Create a list of assay report entries for the moved assays
        assay_reads_to_delete: list[AuditReportEntry] = []
        for sg_id, cram_analysis_id in completed_sgs.items():
            if sg_id in self.excluded_sequencing_groups:
                continue
            sg = self.get_sequencing_group_data_by_id(sg_id, sequencing_groups)
            assay_ids = sg_assays_id_map[sg_id]
            for assay_id in assay_ids:
                assay_read_paths = assay_id_to_paths_and_sizes[assay_id]
                for path, size in assay_read_paths:
                    if path in metamist_paths_to_nowhere: # Already deleted
                        continue

                    assay_reads_to_delete.append(
                        AuditReportEntry(
                            file_path=path,
                            filesize=size,
                            sg_id=sg_id,
                            assay_id=assay_id,
                            cram_analysis_id=cram_analysis_id,
                            sample_id=sg.sample.id,
                            sample_external_id=sg.sample.external_id,
                            participant_id=sg.sample.participant.id,
                            participant_external_id=sg.sample.participant.external_id,
                        )
                    )

        reads_to_delete = assay_reads_to_delete + moved_assay_report_entries

        return reads_to_delete, reads_to_ingest


    @staticmethod
    def find_crams_for_reads_to_ingest(
        reads_to_ingest: list[AuditReportEntry],
        sg_cram_paths: dict[str, dict[int, str]],
    ) -> list[AuditReportEntry]:
        """
        Compares the external sample IDs for SGs with completed CRAMs against the
        uningested read files. This may turn up results for cases where multiple read types
        have been provided for a sample, but only one type was ingested and used for alignment.
        """
        possible_assay_ingests: list[AuditReportEntry] = []
        for read_to_ingest in reads_to_ingest:
            if not read_to_ingest.sample_id:
                # If no sample id was detected in the filename, add the path with no further checks
                possible_assay_ingests.append(
                    AuditReportEntry(
                        file_path=read_to_ingest.file_path,
                        filesize=read_to_ingest.filesize,
                    )
                )
                continue

            # Else get the completed CRAM analysis id
            sg_cram = sg_cram_paths[read_to_ingest.sg_id]
            cram_path = sg_cram[read_to_ingest.cram_analysis_id]
            possible_assay_ingests.append(
                AuditReportEntry(
                    file_path=read_to_ingest.file_path,
                    filesize=read_to_ingest.filesize,
                    sg_id=read_to_ingest.sg_id,
                    assay_id=read_to_ingest.assay_id,
                    cram_analysis_id=read_to_ingest.cram_analysis_id,
                    cram_file_path=cram_path,
                    sample_id=read_to_ingest.sample_id,
                    sample_external_id=read_to_ingest.sample_external_id,
                    participant_id=read_to_ingest.participant_id,
                    participant_external_id=read_to_ingest.participant_external_id,
                )
            )

        return possible_assay_ingests
