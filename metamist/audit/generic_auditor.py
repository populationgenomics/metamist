import logging
import os
from datetime import datetime
from typing import Any

from cpg_utils.config import config_retrieve, dataset_path
from gql.transport.requests import log as requests_logger

from metamist.audit.audithelper import (
    AuditHelper,
    FILE_TYPES_MAP,
    ReadFileData,
    AssayData,
    ParticipantData,
    SampleData,
    SequencingGroupData,
    AuditReportEntry,
    SequencingGroupId,
)
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
        # Initialize the auditor
        self.dataset = self.validate_dataset(dataset)
        self.sequencing_types = self.validate_sequencing_types(sequencing_types)
        self.file_types = self.validate_file_types(file_types)

        # Set remaining attributes
        self.sequencing_technologies = sequencing_technologies
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        # Calculate bucket name
        self.bucket_name = self.get_bucket_name(self.dataset, 'upload')

        super().__init__()
        requests_logger.setLevel(logging.WARNING)

    def validate_dataset(self, dataset: str) -> str:
        """Validate the input dataset"""
        if not dataset:
            dataset = config_retrieve(['workflow', 'dataset'])
        if not dataset:
            raise ValueError('Metamist dataset is required')
        return dataset

    def validate_sequencing_types(self, sequencing_types: list[str]) -> list[str]:
        """Validate the input sequencing types"""
        if sequencing_types == ('all',):
            return self.all_sequencing_types
        invalid_types = [
            st for st in sequencing_types if st not in self.all_sequencing_types
        ]
        if invalid_types:
            raise ValueError(
                f'Input sequencing types "{invalid_types}" must be in the allowed types: {self.all_sequencing_types}'
            )
        return sequencing_types

    def validate_file_types(self, file_types: tuple[str]) -> tuple[str]:
        """Validate the input file types"""
        if file_types in (('all',), ('all_reads',)):
            return FILE_TYPES_MAP[file_types[0]]
        invalid_file_types = [ft for ft in file_types if ft not in FILE_TYPES_MAP]
        if invalid_file_types:
            raise ValueError(
                f'Input file types "{invalid_file_types}" must be in the allowed types: {", ".join(FILE_TYPES_MAP.keys())}'
            )
        return file_types

    def get_bucket_name(self, dataset: str, category: str) -> str:
        """Get the bucket name for the given dataset and category"""
        test = config_retrieve(['workflow', 'access_level']) == 'test'
        bucket: str = dataset_path(
            suffix='', dataset=dataset, category=category, test=test
        )
        if not bucket:
            raise ValueError(
                f'No bucket found for dataset {dataset} and category {category}'
            )
        return bucket.removeprefix('gs://').removesuffix('/')

    async def get_sgs_for_dataset(self) -> list[SequencingGroupData]:
        """
        Fetches all sequencing groups for the given dataset, including the assays for each sequencing group.

        Returns a list of SequencingGroupData objects.
        """
        logger.info(
            f'{self.dataset} :: Fetching SG assays for {self.sequencing_types} sequencing types'
        )
        dataset_sgs_query_result = await query_async(
            QUERY_DATASET_SGS,
            {
                'datasetName': self.dataset,
                'seqTypes': self.sequencing_types,
                'seqTechs': self.sequencing_technologies,
            },
        )
        dataset_sgs = dataset_sgs_query_result['project']['sequencingGroups']

        return [self.get_sg_data(sg) for sg in dataset_sgs]

    def get_sg_data(self, sg: dict[str, Any]) -> SequencingGroupData:
        """Parse a sequencing group dictionary into a SequencingGroupData object"""
        return SequencingGroupData(
            id_=sg['id'],
            sequencing_type=sg['type'],
            sequencing_technology=sg['technology'],
            sample=SampleData(
                id_=sg['sample']['id'],
                external_id=sg['sample']['externalId'],
                participant=ParticipantData(
                    id_=sg['sample']['participant']['id'],
                    external_id=sg['sample']['participant']['externalId'],
                ),
            ),
            assays=[self.parse_assay_data(assay) for assay in sg['assays']],
        )

    def parse_assay_data(self, assay: dict[str, Any]) -> AssayData:
        """Parse an assay dictionary into an AssayData object"""
        reads: list[dict] = assay['meta']['reads']
        if isinstance(assay['meta']['reads'], dict):
            reads = [reads]

        read_files = []
        for read in reads:
            read_files.append(
                ReadFileData(
                    filepath=read['path'],
                    filesize=read['size'],
                    checksum=read['checksum'],
                )
            )
            if read.get('secondaryFiles'):
                for secondary_file in read['secondaryFiles']:
                    read_files.append(
                        ReadFileData(
                            filepath=secondary_file['path'],
                            filesize=secondary_file['size'],
                            checksum=secondary_file['checksum'],
                        )
                    )

        return AssayData(
            id_=assay['id'],
            read_files=read_files,
            sample=SampleData(
                id_=assay['sample']['id'],
                external_id=assay['sample']['externalId'],
                participant=ParticipantData(
                    id_=assay['sample']['participant']['id'],
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

    async def update_sequencing_groups_with_crams(
        self,
        sequencing_groups: list[SequencingGroupData],
    ):
        """
        Updates the sequencing group data in-place with the CRAM analysis id and path for each SG.
        """
        sg_ids = [sg.id for sg in sequencing_groups]
        logging.info(
            f'{self.dataset} :: Fetching CRAM analyses for {len(set(sg_ids))} SGs'
        )

        sg_analyses_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {'dataset': self.dataset, 'sgId': sg_ids, 'analysisTypes': ['CRAM']},
        )
        crams_by_sg = self.get_latest_analyses_by_sg(
            all_sg_analyses=sg_analyses_query_result['sequencingGroups']
        )

        # Update the sequencing group data with the CRAM analysis id and path
        for seq_group in sequencing_groups:
            sg_id = seq_group.id
            if sg_id not in crams_by_sg:
                continue
            seq_group.cram_analysis_id = crams_by_sg[sg_id]['id']
            seq_group.cram_file_path = crams_by_sg[sg_id]['outputs']['path']

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

    async def check_sg_crams(
        self,
        sequencing_groups: list[SequencingGroupData],
    ) -> dict[str, SequencingGroupData]:
        """
        Returns a dictionary containing two categories of sequencing groups:
         - the completed sgs which have finished aligning and have a cram, as a dict mapping
            the sg_id to the analysis_ids
         - the incomplete sgs where the alignment hasn't completed and no cram exists, as a list
        """
        # Get all the unique cram paths to check
        cram_paths = set(
            sg.cram_file_path for sg in sequencing_groups if sg.cram_file_path
        )

        # Check the analysis CRAM paths actually exist in the bucket
        buckets_and_prefixes_to_search = self.get_gcs_buckets_and_prefixes_from_paths(
            cram_paths
        )
        crams_in_bucket = self.find_files_in_gcs_buckets_prefixes(
            buckets_prefixes=buckets_and_prefixes_to_search,
            file_types=('.cram',),
        )

        # Incomplete SGs initialised as the SGs without a completed CRAM
        incomplete_sgs = [sg for sg in sequencing_groups if not sg.cram_file_path]

        # Completed SGs have a CRAM file in the bucket that matches the path in Metamist analysis record
        # Incomplete SGs have a CRAM analysis record in Metamist but are not found at that path in the bucket
        completed_sgs = []
        for sg in sequencing_groups:
            if sg.cram_file_path in crams_in_bucket:
                completed_sgs.append(sg)
            else:
                logging.warning(
                    f'{self.dataset} :: {sg.id} has CRAM analysis: {sg.cram_analysis_id} - but file not found at path: {sg.cram_file_path}'
                )
                incomplete_sgs.append(sg)

        if incomplete_sgs:
            logging.warning(
                f'{self.dataset} :: {len(incomplete_sgs)} SGs without CRAMs found: {sorted([sg.id for sg in incomplete_sgs])}'
            )
            logging.warning(
                'Checking if any other analyses exist for these SGs, which would be unexpected...'
            )
            await self.check_for_non_cram_analyses([sg.id for sg in incomplete_sgs])

        return {'complete': completed_sgs, 'incomplete': incomplete_sgs}

    async def get_audit_report_records_for_reads_to_delete_and_reads_to_ingest(  # pylint: disable=R0914
        self,
        sequencing_groups: list[SequencingGroupData],
    ) -> tuple[list[AuditReportEntry], list[AuditReportEntry]]:
        """
        Compares the read files in a Metamist dataset to the read files found in the
        upload bucket, and decides which files should be deleted and which should be ingested.

        Input:
            - sequencing_groups: A list of SequencingGroupData objects

        Returns: 1. A list of audit report records for read files that can be deleted
                 2. A list of audit report records for read files that should be ingested
        """
        # Get a list of all the paths and sizes of read files recorded in Metamist assay records
        read_files_in_metamist = [
            read_file
            for sg in sequencing_groups
            for assay in sg.assays
            for read_file in assay.read_files
        ]
        # Find all the read files in the bucket
        read_files_in_bucket = self.get_read_file_blobs_in_gcs_bucket(
            self.bucket_name, self.file_types
        )

        # The files in Metamist which are not in the bucket are assumed to have been deleted
        already_deleted_read_files = {
            read.filepath for read in read_files_in_metamist
        }.difference({read.filepath for read in read_files_in_bucket})

        ingested_reads_that_were_moved = self.find_moved_reads(
            read_files_in_bucket, read_files_in_metamist
        )
        uningested_reads = self.get_uningested_reads(
            read_files_in_metamist, read_files_in_bucket, ingested_reads_that_were_moved
        )

        # Report the reads that can be deleted
        uningested_reads_to_delete_report_entries, reads_to_ingest_report_entries = (
            self.report_uningested_reads(
                sequencing_groups,
                uningested_reads,
            )
        )
        moved_reads_to_delete_report_entries = (
            self.report_ingested_files_that_have_been_moved(
                sequencing_groups,
                ingested_reads_that_were_moved,
            )
        )
        ingested_reads_to_delete_report_entries = self.report_ingested_reads_to_delete(
            sequencing_groups,
            already_deleted_read_files,
        )

        # Concatenate the lists of report entries for the reads to delete
        reads_to_delete_report_entries = (
            moved_reads_to_delete_report_entries
            + uningested_reads_to_delete_report_entries
            + ingested_reads_to_delete_report_entries
        )

        return reads_to_delete_report_entries, reads_to_ingest_report_entries

    def report_ingested_reads_to_delete(
        self,
        sequencing_groups: list[SequencingGroupData],
        already_deleted_read_paths: set[str],
    ) -> list[AuditReportEntry]:
        """
        Generates a list of audit report entries for the read files that have been ingested
        and can safely be deleted.
        """
        ingested_reads_to_delete_report_entries = []
        for sg in sequencing_groups:
            if sg.id in self.excluded_sequencing_groups or not (
                sg.cram_analysis_id and sg.cram_file_path
            ):
                continue
            for assay in sg.assays:
                for read in assay.read_files:
                    if read.filepath in already_deleted_read_paths:
                        continue

                    ingested_reads_to_delete_report_entries.append(
                        AuditReportEntry(
                            filepath=read.filepath,
                            filesize=read.filesize,
                            sg_id=sg.id,
                            assay_id=assay.id,
                            cram_analysis_id=sg.cram_analysis_id,
                            sample_id=sg.sample.id,
                            sample_external_id=sg.sample.external_id,
                            participant_id=sg.sample.participant.id,
                            participant_external_id=sg.sample.participant.external_id,
                        )
                    )

        return ingested_reads_to_delete_report_entries

    def find_moved_reads(
        self,
        read_files_in_bucket: list[ReadFileData],
        read_files_in_metamist: list[ReadFileData],
    ) -> dict[str, ReadFileData]:
        """
        Check the files in the bucket and the files in Metamist to validate if any have been moved.
        Uses the checksums or the file size and file name to identify if a file has been moved.
        Moved files are those that have the same checksum or size and name, but different paths.

        Returns a dictionary of the moved files, with the Metamist path as the key and the ReadFileData object as the value.
        """
        metamist_read_files_by_filename = {
            os.path.basename(read.filepath): read for read in read_files_in_metamist
        }
        metamist_filenames_by_checksum: dict[str, str] = {
            read.checksum: os.path.basename(read.filepath)
            for read in read_files_in_metamist
        }

        moved_files = {}
        for read_file in read_files_in_bucket:
            if read_file.checksum in metamist_filenames_by_checksum:
                metamist_filename = metamist_filenames_by_checksum[read_file.checksum]
                metamist_filepath = metamist_read_files_by_filename[
                    metamist_filename
                ].filepath

                if read_file.filepath != metamist_filepath:
                    logging.warning(
                        f'File {read_file.filepath} has the same checksum as {metamist_filepath} but different path'
                    )
                    moved_files[metamist_filepath] = read_file
                    continue

            if os.path.basename(read_file.filepath) in metamist_read_files_by_filename:
                metamist_filepath = metamist_read_files_by_filename[
                    os.path.basename(read_file.filepath)
                ].filepath
                metamist_filesize = metamist_read_files_by_filename[
                    os.path.basename(read_file.filepath)
                ].filesize
                if (
                    read_file.filepath != metamist_filepath
                    and read_file.filesize == metamist_filesize
                ):
                    logging.warning(
                        f'File {read_file.filepath} has the same name as {metamist_filepath} but different path'
                    )
                    moved_files[metamist_filepath] = read_file
                elif (
                    read_file.filepath != metamist_filepath
                    and read_file.filesize != metamist_filesize
                ):
                    logging.warning(
                        f'File {read_file.filepath} has the same name as {metamist_filepath} but different path and size'
                    )
                continue

            logging.warning(f'File {read_file.filepath} not found in Metamist')

        logging.info(f'Found {len(moved_files)} ingested files that have been moved')

        return moved_files

    def report_ingested_files_that_have_been_moved(
        self,
        sequencing_groups: list[SequencingGroupData],
        ingested_reads_that_were_moved: dict[str, ReadFileData],
    ) -> list[AuditReportEntry]:
        """
        Identify if any paths are to files that have actually just been moved
        by checking if they are in the bucket but not Metamist.

        If they are, validate it's the same file by comparing the checksums or sizes.
        If they match, assume the file has been moved and add it to the list of
        ingested and moved files.

        Returns a list of AuditReportEntry objects for the moved files.
        """
        sgs_by_assay_id = {
            assay.id: sg for sg in sequencing_groups for assay in sg.assays
        }

        assays = [assay for sg in sequencing_groups for assay in sg.assays]
        read_file_path_to_assay_id = {
            read.filepath: assay.id for assay in assays for read in assay.read_files
        }

        moved_reads = []
        for metamist_path, read_file in ingested_reads_that_were_moved.items():
            assay_id = read_file_path_to_assay_id.get(metamist_path)

            sg = sgs_by_assay_id.get(assay_id)
            cram_analysis_id = sg.cram_analysis_id if sg else None

            if sg in self.excluded_sequencing_groups or not cram_analysis_id:
                continue

            moved_reads.append(
                AuditReportEntry(
                    filepath=read_file.filepath,
                    filesize=read_file.filesize,
                    sg_id=sg.id,
                    assay_id=assay_id,
                    cram_analysis_id=cram_analysis_id,
                    sample_id=sg.sample.id,
                    sample_external_id=sg.sample.external_id,
                    participant_id=sg.sample.participant.id,
                    participant_external_id=sg.sample.participant.external_id,
                )
            )

        return moved_reads

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

    def get_uningested_reads(
        self,
        read_files_in_metamist: list[ReadFileData],
        read_files_in_bucket: list[ReadFileData],
        read_files_to_exclude: list[ReadFileData] = None,
    ) -> list[ReadFileData]:
        """
        Get a list of read files in the bucket that are not in Metamist
        Optionally exclude a list of read files from the comparison.
        """
        read_paths_in_metamist = {read.filepath for read in read_files_in_metamist}
        if read_files_to_exclude:
            read_paths_to_exclude = {read.filepath for read in read_files_to_exclude}
            read_paths_in_metamist = read_paths_in_metamist.difference(
                read_paths_to_exclude
            )

        return [
            read
            for read in read_files_in_bucket
            if read.filepath not in read_paths_in_metamist
        ]

    def report_uningested_reads(
        self,
        sequencing_groups: list[SequencingGroupData],
        uningested_reads: list[ReadFileData],
    ) -> tuple[list[AuditReportEntry], list[AuditReportEntry]]:
        """
        Generates two lists of audit report entries for the uningested reads
            1. Reads which can be deleted.
            2. Reads which should be ingested.

        If the read file path contains a sample or participant external ID associated with a completed SG,
        it should be deleted.

        Otherwise, it should be ingested.
        """
        if not uningested_reads:
            return ([], [])

        completed_sgs = [
            sg for sg in sequencing_groups if sg.cram_analysis_id and sg.cram_file_path
        ]
        completed_sgs_by_sample_external_id = {
            sg.sample.external_id: sg for sg in completed_sgs
        }
        completed_sgs_by_participant_external_id = {
            sg.sample.participant.external_id: sg for sg in completed_sgs
        }

        reads_to_delete_report_entries = []
        reads_to_ingest_report_entries = []
        for read in uningested_reads:
            known_sample_id = None
            known_participant_id = None
            for sample_ext_id in completed_sgs_by_sample_external_id.keys():
                if sample_ext_id in read.filepath:
                    known_sample_id = sample_ext_id
                    break
            if not known_sample_id:
                for (
                    participant_ext_id
                ) in completed_sgs_by_participant_external_id.keys():
                    if participant_ext_id in read.filepath:
                        known_sample_id = completed_sgs_by_participant_external_id[
                            participant_ext_id
                        ].sample.external_id
                        known_participant_id = participant_ext_id
                        break
            else:
                known_participant_id = completed_sgs_by_sample_external_id[
                    known_sample_id
                ].sample.participant.external_id

            if known_sample_id and known_participant_id:
                sg = completed_sgs_by_sample_external_id[known_sample_id]
                reads_to_delete_report_entries.append(
                    AuditReportEntry(
                        filepath=read.filepath,
                        filesize=read.filesize,
                        sg_id=sg.id,
                        assay_id=None,  # Not relevant for uningested reads being deleted
                        cram_analysis_id=sg.cram_analysis_id,
                        cram_file_path=sg.cram_file_path,
                        sample_id=sg.sample.id,
                        sample_external_id=sg.sample.external_id,
                        participant_id=sg.sample.participant.id,
                        participant_external_id=sg.sample.participant.external_id,
                    )
                )

            else:
                # If no known ID was detected in the filename, add the path with no further checks
                reads_to_ingest_report_entries.append(
                    AuditReportEntry(
                        filepath=read.filepath,
                        filesize=read.filesize,
                    )
                )

        return reads_to_delete_report_entries, reads_to_ingest_report_entries

    async def get_reads_to_delete_or_ingest(
        self,
        sequencing_groups: list[SequencingGroupData],
    ) -> tuple[list[AuditReportEntry], list[AuditReportEntry]]:
        """
        Inputs:
            - sequencing_groups: A list of SequencingGroupData objects

        Returns two lists, each containing AuditReportEntry objects:
          1. Reads which can be deleted.
          2. Reads which should be ingested.

        The reads to delete are those that are associated with SGs that have completed CRAMs.
        The reads to ingest are those that are not associated with SGs that have completed CRAMs.
        """
        (
            reads_to_delete_report_entries,
            reads_to_ingest_report_entries,
        ) = await self.get_audit_report_records_for_reads_to_delete_and_reads_to_ingest(
            sequencing_groups,
        )

        return reads_to_delete_report_entries, reads_to_ingest_report_entries

    # @staticmethod
    # def find_crams_for_reads_to_ingest(
    #     reads_to_ingest: list[AuditReportEntry],
    #     sg_cram_paths: dict[str, dict[int, str]],
    # ) -> list[AuditReportEntry]:
    #     """
    #     Compares the external sample IDs for SGs with completed CRAMs against the
    #     uningested read files. This may turn up results for cases where multiple read types
    #     have been provided for a sample, but only one type was ingested and used for alignment.
    #     """
    #     possible_assay_ingests: list[AuditReportEntry] = []
    #     for read_to_ingest in reads_to_ingest:
    #         if not read_to_ingest.sample_id:
    #             # If no sample id was detected in the filename, add the path with no further checks
    #             possible_assay_ingests.append(
    #                 AuditReportEntry(
    #                     filepath=read_to_ingest.filepath,
    #                     filesize=read_to_ingest.filesize,
    #                 )
    #             )
    #             continue

    #         # Else get the completed CRAM analysis id
    #         sg_cram = sg_cram_paths[read_to_ingest.sg_id]
    #         cram_path = sg_cram[read_to_ingest.cram_analysis_id]
    #         possible_assay_ingests.append(
    #             AuditReportEntry(
    #                 filepath=read_to_ingest.filepath,
    #                 filesize=read_to_ingest.filesize,
    #                 sg_id=read_to_ingest.sg_id,
    #                 assay_id=read_to_ingest.assay_id,
    #                 cram_analysis_id=read_to_ingest.cram_analysis_id,
    #                 cram_file_path=cram_path,
    #                 sample_id=read_to_ingest.sample_id,
    #                 sample_external_id=read_to_ingest.sample_external_id,
    #                 participant_id=read_to_ingest.participant_id,
    #                 participant_external_id=read_to_ingest.participant_external_id,
    #             )
    #         )

    #     return possible_assay_ingests
