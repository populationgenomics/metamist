import logging
import os
from datetime import datetime
from typing import Any

from gql.transport.requests import log as requests_logger

from metamist.audit.audithelper import (
    logger,
    AuditHelper,
    ReadFileData,
    AssayData,
    ParticipantData,
    SampleData,
    SequencingGroupData,
    AuditReportEntry,
    SequencingGroupId,
)
from metamist.graphql import gql, query_async


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

# TODO
# For datasets with many sequencing groups, this query can be slow, time out, or return a 500 error.
# Implement a method to fetch sequencing group data in smaller batches by first fetching all sequencing group IDs,
# and then fetching the sequencing group data in smaller chunks.
# This will allow us to avoid the 500 error and improve performance for large datasets.
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
                        # Getting the sample and participant data for the assay is not needed
                        # sample {
                        #     id
                        #     externalId
                        #     participant {
                        #         id
                        #         externalId
                        #     }
                        # }
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
        excluded_prefixes: tuple[str] | None = None,
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(dataset=dataset)
        # Initialize the auditor
        self.sequencing_types = self.validate_sequencing_types(sequencing_types)
        self.file_types = self.validate_file_types(file_types)
        self.excluded_prefixes = excluded_prefixes

        # Set remaining attributes
        self.sequencing_technologies = sequencing_technologies
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        # Calculate bucket name
        self.bucket_name = self.get_bucket_name(self.dataset, 'upload')
        requests_logger.setLevel(logging.WARNING)

    async def get_sgs_for_dataset(self) -> list[SequencingGroupData]:
        """
        Fetches all sequencing groups for the given dataset, including the assays for each sequencing group.

        Returns a list of SequencingGroupData objects.
        """
        logger.info(
            f'{self.dataset} :: Fetching sequencing groups for \n  Sequencing Types:\n    {", ".join(sorted(self.sequencing_types))}\n  Sequencing Technologies:\n    {", ".join(sorted(self.sequencing_technologies))}'
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
        logger.info(f'{self.dataset} :: Got {len(dataset_sgs)} sequencing groups\n')

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
            try:
                read_files.append(self.parse_read_file(read))
            except KeyError:
                logger.warning(
                    f'Failed to parse read file: {read} for assay: {assay["id"]}'
                )
                continue
            if read.get('secondaryFiles'):
                for secondary_file in read['secondaryFiles']:
                    try:
                        read_files.append(self.parse_read_file(secondary_file))
                    except KeyError:
                        logger.warning(
                            f'Failed to parse secondary read file: {secondary_file} for assay: {assay["id"]}'
                        )
                        continue

        return AssayData(
            id_=assay['id'],
            read_files=read_files,
            # Filling the sample and participant data for the assay is just not needed
            
            # sample=SampleData(
            #     id_=assay['sample']['id'],
            #     external_id=assay['sample']['externalId'],
            #     participant=ParticipantData(
            #         id_=assay['sample']['participant']['id'],
            #         external_id=assay['sample']['participant']['externalId'],
            #     ),
            # ),
        )

    def parse_read_file(self, read: dict) -> ReadFileData:
        """Parse a list of read files from an assay dictionary"""
        return ReadFileData(
            filepath=read['location'],
            filesize=read['size'],
            checksum=read['checksum'] if 'checksum' in read else None,
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
        logger.info(
            f'{self.dataset} :: Fetching CRAM analyses for {len(sg_ids)} SGs...'
        )

        sg_analyses_query_result = await query_async(
            QUERY_SG_ANALYSES,
            {'dataset': self.dataset, 'sgIds': sg_ids, 'analysisTypes': ['CRAM']},
        )
        crams_by_sg = self.get_latest_analyses_by_sg(
            all_sg_analyses=sg_analyses_query_result['sequencingGroups']
        )
        logger.info(
            f'{self.dataset} :: Got {len(crams_by_sg)} CRAM analyses for {len(sg_ids)} SGs\n'
        )

        # Update the sequencing group data with the CRAM analysis id and path
        for seq_group in sequencing_groups:
            sg_id = seq_group.id
            if sg_id not in crams_by_sg:
                continue
            seq_group.cram_analysis_id = crams_by_sg[sg_id]['id']
            if isinstance(crams_by_sg[sg_id]['outputs'], str):
                seq_group.cram_file_path = crams_by_sg[sg_id]['outputs']
            else:
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
            logger.warning(
                f'{self.dataset} :: SG {sg_id} missing CRAM but has analyses:'
            )
            for analysis in sg_analysis['analyses']:
                logger.warning(
                    f'{self.dataset} :: {analysis["id"]} - {analysis["type"]} - {analysis["outputs"].get("path")}'
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

        # Completed SGs have a CRAM file in the bucket that matches the path in Metamist analysis record
        # Incomplete SGs have a CRAM analysis record in Metamist but are not found at that path in the bucket
        completed_sgs = []
        incomplete_sgs = []
        for sg in sequencing_groups:
            if sg.cram_file_path and sg.cram_file_path in crams_in_bucket:
                completed_sgs.append(sg)
                continue
            if sg.cram_file_path and sg.cram_file_path not in crams_in_bucket:
                logger.warning(
                    f'{self.dataset} :: {sg.id} has CRAM analysis: {sg.cram_analysis_id} - but file not found at path: {sg.cram_file_path}'
                )
            incomplete_sgs.append(sg)

        if incomplete_sgs:
            logger.warning(
                f'{self.dataset} :: {len(incomplete_sgs)} SGs without CRAMs found: {sorted([sg.id for sg in incomplete_sgs])}'
            )
            logger.warning(
                f'{self.dataset} :: Checking if any other analyses exist for these SGs, which would be unexpected...'
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
        logger.info(f'{self.dataset} :: Fetching read files in bucket {self.bucket_name}')
        read_files_in_bucket = self.get_read_file_blobs_in_gcs_bucket(
            self.bucket_name, self.file_types, self.excluded_prefixes
        )

        # The files in Metamist which are not in the bucket are assumed to have been deleted
        already_deleted_read_files = {
            read.filepath for read in read_files_in_metamist
        }.difference({read.filepath for read in read_files_in_bucket})

        logger.info(f'{self.dataset} :: Finding reads that have been moved or deleted')
        ingested_reads_that_were_moved = self.find_moved_reads(
            read_files_in_bucket, read_files_in_metamist
        )
        
        logger.info(f'{self.dataset} :: Finding uningested reads in bucket')
        uningested_reads = self.get_uningested_reads(
            read_files_in_metamist,
            read_files_in_bucket,
            ingested_reads_that_were_moved.values(),
        )

        logger.info(f'{self.dataset} :: Reporting on uningested reads to delete and ingest')
        # Report the reads that can be deleted
        uningested_reads_to_delete_report_entries, reads_to_ingest_report_entries = (
            self.report_uningested_reads(
                sequencing_groups,
                uningested_reads,
            )
        )
        logger.info(f'{self.dataset} :: Reporting on ingested reads that have been moved')
        moved_reads_to_delete_report_entries = (
            self.report_ingested_files_that_have_been_moved(
                sequencing_groups,
                ingested_reads_that_were_moved,
            )
        )
        logger.info(f'{self.dataset} :: Reporting on ingested reads to delete')
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
                            cram_file_path=sg.cram_file_path,
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
                    logger.warning(
                        f'{self.dataset} :: FILE MOVE DETECTED\n  - Bucket file: {read_file.filepath}\n  {metamist_filepath} but different path'
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
                    logger.warning(
                        f'{self.dataset} :: FILE MOVE DETECTED\n  - Bucket file:  {read_file.filepath} ({read_file.filesize} bytes)\n  - Metamist file:  {metamist_filepath} ({metamist_filesize} bytes)\n'
                    )
                    moved_files[metamist_filepath] = read_file
                elif (
                    read_file.filepath != metamist_filepath
                    and read_file.filesize != metamist_filesize
                ):
                    logger.warning(
                        f'{self.dataset} :: FILE SIZE INCONSISTENCY\n  - Bucket file:  {read_file.filepath} ({read_file.filesize} bytes)\n  - Metamist file:  {metamist_filepath} ({metamist_filesize} bytes)\n'
                    )
                continue

        if moved_files:
            logger.info(
                f'Found {len(moved_files)} ingested files that have been moved.\n'
            )

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
                    cram_file_path=sg.cram_file_path,
                    sample_id=sg.sample.id,
                    sample_external_id=sg.sample.external_id,
                    participant_id=sg.sample.participant.id,
                    participant_external_id=sg.sample.participant.external_id,
                )
            )

        return moved_reads

    def get_uningested_reads(
        self,
        read_files_in_metamist: list[ReadFileData],
        read_files_in_bucket: list[ReadFileData],
        read_files_to_exclude: list[ReadFileData] | None = None,
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
            # Check if the read file path contains a sample or participant external ID associated with a completed SG
            # This is a naive check which assumes the external ID is in the file path
            # TODO: Improve this check to use a more robust method that uses known file naming conventions, e.g.
            # - extract the VCGS or Garvan format sample ID from the file path, and match this specifically
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
                # TODO
                # Be careful with this check, as it assumes that
                #  - If the participant external ID is from a completed SG, and the participant external ID is in the file path
                #  - Then the read file is associated with that completed SG. 
                # This is a naive check and may not always be correct!
                #  - Some datasets have multiple samples for the same participant, and the read file may not be associated with the sample in the file path.
                #  - E.g. a participant 'IND001' has a completed genome SG, and a bucket file path contains 'IND001' but is actually an exome read file for a different sample from the same participant.
                #    - In this case, the read file should not be deleted, but should be ingested.
                #  - Or, the read file may be from a different participant with the same external ID, e.g. 'IND001' in a different dataset.
                #  - Or, the read file may be from a different sample and just happens to have the same participant external ID in the file path (unlikely, but possible). 
                #  - This is a known issue with the current implementation, and should be improved in the future.
                sg = completed_sgs_by_sample_external_id[known_sample_id]
                if not sg.id:
                    raise ValueError(
                        f'{self.dataset} :: {known_sample_id} has no SG ID, cannot delete read file: {read.filepath}'
                    )
                logger.info(
                    f'{self.dataset} :: UNINGESTED FILE MATCHES A METAMIST RECORD\n - Bucket file:  {read.filepath}\nMatches completed SG {sg.id} - Sample: {sg.sample.external_id} - Participant: {sg.sample.participant.external_id}\n'
                )
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
