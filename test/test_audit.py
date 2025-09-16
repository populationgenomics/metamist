# noqa: B006

from datetime import datetime
import unittest
import unittest.mock

from metamist.audit.adapters import StorageClient
from metamist.audit.cli.upload_bucket_audit import AuditOrchestrator
from metamist.audit.data_access import MetamistDataAccess, GCSDataAccess
from metamist.audit.services import (
    AuditAnalyzer,
    FileMatchingService,
    ReportGenerator,
    AuditLogs,
)
from metamist.audit.models import (
    SequencingGroup,
    Assay,
    Analysis,
    AuditConfig,
    FileType,
    AuditResult,
    AuditReportEntry,
    FileMetadata,
)
from test.data.audit_fixtures.audit_test import (
    ANALYSES_WITH_MALFORMED_TIMESTAMPS_GQL_RESPONSE,
    SG_ASSAYS_RESPONSE_WITH_SECONDARY_FILES,
    ENUMS_QUERY_RESULT,
    SEQUENCING_GROUPS,
    SR_ILLUMINA_EXOME_SGS,
    ANALYSES,
    ALL_SG_ASSAYS_RESPONSE,
    SR_ILLUMINA_EXOME_SGS_ASSAYS_RESPONSE,
    ALL_SG_ANALYSES_RESPONSE,
    SR_ILLUMINA_EXOME_SGS_CRAM_RESPONSE,
)
from test.data.audit_fixtures.bucket_files import UPLOAD_BUCKET_FILES, MAIN_BUCKET_FILES
from test.testbase import run_as_sync
from types import SimpleNamespace
from cpg_utils import to_path


class TestAudit(unittest.TestCase):  # pylint: disable=too-many-instance-attributes, too-many-public-methods
    """Test the audit module"""

    def setUp(self):
        """Set up test fixtures."""
        # Start the storage client mock before creating GCSDataAccess
        self.storage_client_patcher = unittest.mock.patch(
            'metamist.audit.adapters.storage_client.storage.Client'
        )
        self.mock_storage_client = self.storage_client_patcher.start()

        # Mock get_bucket_name to avoid needing real config
        self.bucket_patcher = unittest.mock.patch.object(
            GCSDataAccess, 'get_bucket_name'
        )
        self.mock_get_bucket_name = self.bucket_patcher.start()
        self.mock_get_bucket_name.side_effect = [
            'cpg-dataset-main',
            'cpg-dataset-main-upload',
            'cpg-dataset-main-analysis',
        ]

        self.metamist_data_access = MetamistDataAccess()
        self.gcs_data_access = GCSDataAccess('dataset', 'test')
        self.file_matcher = FileMatchingService()
        self.audit_analyzer = AuditAnalyzer()
        self.audit_logs = AuditLogs('dataset', 'test')
        self.report_generator = ReportGenerator(self.gcs_data_access, self.audit_logs)

    def tearDown(self):
        """Clean up test fixtures."""
        self.storage_client_patcher.stop()
        self.bucket_patcher.stop()

    def test_file_matching_service(self):
        """Test FileMatchingService"""
        self.assertIsInstance(self.file_matcher, FileMatchingService)
        self.assertIsInstance(self.audit_analyzer.file_matcher, FileMatchingService)

    def test_gcs_data_access_setup(self):
        """
        First test that GCSDataAccess initializes correctly with mocked buckets
        since these are used frequently in the tests.
        """
        # Verify that the bucket names are set correctly from our mocks
        self.assertEqual(self.gcs_data_access.main_bucket, 'cpg-dataset-main')
        self.assertEqual(self.gcs_data_access.upload_bucket, 'cpg-dataset-main-upload')
        self.assertEqual(
            self.gcs_data_access.analysis_bucket, 'cpg-dataset-main-analysis'
        )
        self.assertEqual(self.gcs_data_access.dataset, 'dataset')
        self.assertEqual(self.gcs_data_access.gcp_project, 'test')

    # ===== METAMIST DATA ACCESS TESTS =====
    # These test the data access layer - mapping GraphQL responses to domain models
    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_validate_enums(self, mock_query_async):
        """Test MetamistDataAccess.validate_enums"""
        mock_query_async.return_value = ENUMS_QUERY_RESULT
        config = AuditConfig(
            dataset='dataset',
            sequencing_types=('genome', 'exome'),
            sequencing_technologies=('short-read', 'long-read'),
            sequencing_platforms=('illumina', 'pacbio', 'ont'),
            analysis_types=('cram', 'vcf'),
            file_types=tuple(list(FileType)),
        )
        result = await self.metamist_data_access.validate_metamist_enums(config=config)
        self.assertEqual(config, result)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_validate_enums_failure(self, mock_query_async):
        """Test MetamistDataAccess.validate_enums invalid input"""
        mock_query_async.return_value = ENUMS_QUERY_RESULT
        with self.assertRaises(ValueError):
            await self.metamist_data_access.validate_metamist_enums(
                config=AuditConfig(
                    dataset='dataset',
                    sequencing_types=('genome', 'exome'),
                    sequencing_technologies=(
                        'short-read',
                        'long-read',
                        'future-technology',
                    ),
                    sequencing_platforms=('illumina', 'pacbio', 'ont'),
                    analysis_types=('cram', 'vcf'),
                    file_types=tuple(list(FileType)),
                )
            )

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_all_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_sequencing_groups"""
        mock_query_async.return_value = ALL_SG_ASSAYS_RESPONSE

        result = await self.metamist_data_access.get_sequencing_groups(
            dataset='dataset',
            sequencing_types=['genome', 'exome'],
            sequencing_technologies=['short-read', 'long_read'],
            sequencing_platforms=['illumina', 'pacbio', 'ont'],
        )
        self.assertEqual(len(result), 7)
        for sg in result:
            self.assertIsInstance(sg, SequencingGroup)
            for assay in sg.assays:
                self.assertIsInstance(assay, Assay)
                for rf in assay.read_files:
                    self.assertIsInstance(rf, FileMetadata)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_sequencing_groups_with_secondary_files(self, mock_query_async):
        """Test MetamistDataAccess.get_sequencing_groups"""
        mock_query_async.return_value = SG_ASSAYS_RESPONSE_WITH_SECONDARY_FILES

        result = await self.metamist_data_access.get_sequencing_groups(
            dataset='dataset',
            sequencing_types=['genome', 'exome'],
            sequencing_technologies=['short-read', 'long_read'],
            sequencing_platforms=['illumina', 'pacbio', 'ont'],
        )
        self.assertEqual(len(result[0].assays[0].read_files), 2)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_filtered_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_sequencing_groups filtered"""
        mock_query_async.return_value = SR_ILLUMINA_EXOME_SGS_ASSAYS_RESPONSE

        result = await self.metamist_data_access.get_sequencing_groups(
            dataset='dataset',
            sequencing_types=['exome'],
            sequencing_technologies=['short-read'],
            sequencing_platforms=['illumina'],
        )

        self.assertEqual(len(result), 1)
        sg = result[0]
        self.assertIsInstance(sg, SequencingGroup)
        self.assertEqual(sg.id, 'SG01_2')
        self.assertEqual(sg.technology, 'short-read')
        self.assertEqual(sg.platform, 'illumina')
        self.assertEqual(sg.sample.external_id, 'EXT001')
        self.assertEqual(sg.sample.participant.external_id, 'P001')

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query')
    async def test_get_dataset_cohort(self, mock_query):
        """Test MetamistDataAccess.get_dataset_cohort"""
        mock_query.return_value = {
            'project': {'cohorts': [{'id': 'C01', 'name': 'Cohort 1'}]}
        }
        cohort_id = self.metamist_data_access.graphql_client.get_dataset_cohort(
            'dataset', 'Cohort 1'
        )
        self.assertEqual(cohort_id, 'C01')

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query')
    async def test_get_dataset_cohort_create_new(self, mock_query):
        """Test MetamistDataAccess.get_dataset_cohort where none are found so we create new"""
        mock_query.side_effect = [
            {  # Query 1 - for the existing cohorts
                'project': {'cohorts': []}
            },
            {  # Query 2 - a mutation query to create a new cohort
                'cohort': {
                    'createCohortFromCriteria': {'id': 'C01', 'name': 'Cohort 1'}
                }
            },
        ]
        cohort_id = self.metamist_data_access.graphql_client.get_dataset_cohort(
            'dataset', 'Cohort 1'
        )
        self.assertEqual(cohort_id, 'C01')

    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query')
    def test_create_audit_deletion_analysis(self, mock_query):
        """Test MetamistDataAccess.create_audit_deletion_analysis"""
        mock_query.side_effect = [
            {'project': {'cohorts': [{'id': 'C01', 'name': 'Cohort 1'}]}},
            {'analysis': {'createAnalysis': {'id': 1}}},
        ]
        result = self.metamist_data_access.create_audit_deletion_analysis(
            dataset='dataset',
            cohort_name='Cohort 1',
            audited_report_name='files_to_delete',
            deletion_report_path='gs://cpg-dataset-main-analysis/audit_results/20250901_1633/files_to_delete.csv',
            stats={},
        )

        self.assertEqual(result, 1)

    # TODO
    # Test entities
    # test participant.external_id
    # test sample.external_id
    # test assay.get_total_size
    # test sequencing_group.get_total_read_size

    # TODO
    # test value_objects
    # FileType.extensions
    # FileType.all_read_extensions
    # FileType.all_extensions
    # ExternalIds.get_primary
    # ExternalIds.__getitem__
    # ExternalIds.values
    # AuditConfig.from_cli_args

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_analyses_for_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_analyses_for_sequencing_groups"""
        mock_query_async.return_value = ALL_SG_ANALYSES_RESPONSE

        result = await self.metamist_data_access.get_analyses_for_sequencing_groups(
            dataset='dataset',
            sg_ids=[sg.id for sg in SEQUENCING_GROUPS.values()],
            analysis_types=['cram', 'vcf'],
        )

        self.assertEqual(len(result), 7)
        for analysis in result:
            self.assertIsInstance(analysis, Analysis)
            self.assertIn(analysis.id, [1, 2, 3, 4, 5, 6, 7])
            self.assertIn(analysis.type, ['cram', 'vcf'])

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_analyses_alternate(self, mock_query_async):
        """Test MetamistDataAccess.get_analyses_for_sequencing_groups"""
        mock_query_async.return_value = ANALYSES_WITH_MALFORMED_TIMESTAMPS_GQL_RESPONSE

        result = await self.metamist_data_access.get_analyses_for_sequencing_groups(
            dataset='dataset',
            sg_ids=['SG101'],
            analysis_types=['cram'],
        )

        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Analysis)
        self.assertEqual(result[0].id, 101)
        self.assertEqual(result[0].timestamp_completed, datetime.min)
        self.assertEqual(
            result[0].output_path, 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram'
        )

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_filtered_analyses_for_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_analyses_for_sequencing_groups filtered"""
        mock_query_async.return_value = SR_ILLUMINA_EXOME_SGS_CRAM_RESPONSE

        result = await self.metamist_data_access.get_analyses_for_sequencing_groups(
            dataset='dataset',
            sg_ids=[sg.id for sg in SR_ILLUMINA_EXOME_SGS],
            analysis_types=['cram'],
        )

        self.assertEqual(len(result), 1)
        analysis = result[0]
        self.assertIsInstance(analysis, Analysis)
        self.assertEqual(analysis.id, 2)
        self.assertEqual(analysis.sequencing_group_id, 'SG01_2')
        self.assertEqual(analysis.type, 'cram')
        self.assertTrue(analysis.is_cram)
        self.assertEqual(
            analysis.output_path, 'gs://cpg-dataset-main/exome/cram/SG01_2.cram'
        )

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query')
    async def test_get_audit_deletion_analysis(self, mock_query):
        """Test MetamistDataAccess.get_audit_deletion_analyses"""
        mock_query.return_value = {
            'project': {
                'analyses': [
                    {
                        'id': 1,
                        'timestampCompleted': '2025-09-01T16:33:00',
                        'output': 'gs://cpg-dataset-main-analysis/audit_results/20250901_1633/deleted_file.csv',
                        'meta': {},
                    },
                    {
                        'id': 2,
                        'timestampCompleted': '2025-09-01T16:43:00',
                        'output': 'gs://cpg-dataset-main-analysis/audit_results/20250901_1643/deleted_file.csv',
                        'meta': {},
                    },
                ],
            },
        }

        result = self.metamist_data_access.get_audit_deletion_analysis(
            dataset='dataset',
            output_path='gs://cpg-dataset-main-analysis/audit_results/20250901_1633/deleted_file.csv',
        )

        self.assertIsInstance(result, dict)
        self.assertEqual(result['id'], 1)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query')
    async def test_get_audit_deletion_analysis_none(self, mock_query):
        """Test MetamistDataAccess.get_audit_deletion_analyses"""
        mock_query.return_value = {
            'project': {
                'analyses': [],
            },
        }

        result = self.metamist_data_access.get_audit_deletion_analysis(
            dataset='dataset',
            output_path='gs://cpg-dataset-main-analysis/audit_results/20250901_1633/deleted_file.csv',
        )

        self.assertIsNone(result)

    # ===== GCS DATA ACCESS TESTS =====
    # These test the data access layer - mapping GCS responses to domain models

    @unittest.mock.patch.object(GCSDataAccess, 'list_files_in_bucket')
    def test_list_read_files_in_bucket(self, mock_list_files):
        """Test listing the files in a bucket"""
        mock_list_files.return_value = [
            f
            for f in UPLOAD_BUCKET_FILES
            if str(f.filepath).endswith(FileType.all_read_extensions())
        ]

        blobs = self.gcs_data_access.list_files_in_bucket(
            bucket_name='cpg-dataset-main-upload',
            file_types=[FileType.FASTQ, FileType.BAM, FileType.CRAM],
        )

        self.assertEqual(len(blobs), 10)

    @unittest.mock.patch.object(GCSDataAccess, 'list_files_in_bucket')
    def test_list_all_files_in_bucket(self, mock_list_files):
        """Test listing the files in a bucket"""
        mock_list_files.return_value = [
            f
            for f in UPLOAD_BUCKET_FILES
            if str(f.filepath).endswith(FileType.all_extensions())
        ]

        blobs = self.gcs_data_access.list_files_in_bucket(
            bucket_name='cpg-dataset-main-upload',
            file_types=list(FileType),
        )

        self.assertEqual(len(blobs), 11)

    @unittest.mock.patch.object(StorageClient, 'check_blobs')
    def test_validate_cram_files(self, mock_check_blobs):
        """Test validating CRAM files"""
        mock_check_blobs.return_value = [f.filepath for f in MAIN_BUCKET_FILES[:2]]

        found, missing = self.gcs_data_access.validate_cram_files(
            cram_paths=[f.filepath for f in MAIN_BUCKET_FILES[:2]]
        )

        self.assertEqual(len(found), 2)
        self.assertEqual(len(missing), 0)
        self.assertEqual(found[0], to_path('gs://cpg-dataset-main/cram/SG01_1.cram'))
        self.assertEqual(
            found[1], to_path('gs://cpg-dataset-main/exome/cram/SG01_2.cram')
        )

    @unittest.mock.patch.object(StorageClient, 'check_blobs')
    def test_validate_cram_files_with_missing(self, mock_check_blobs):
        """Test validating CRAM files where one is missing"""
        # Mock check_blobs to return only 2 of the 3 requested files
        mock_check_blobs.return_value = [f.filepath for f in MAIN_BUCKET_FILES[:2]]

        found, missing = self.gcs_data_access.validate_cram_files(
            cram_paths=[f.filepath for f in MAIN_BUCKET_FILES[:3]]
        )

        self.assertEqual(len(found), 2)
        self.assertEqual(len(missing), 1)
        self.assertEqual(found[0], to_path('gs://cpg-dataset-main/cram/SG01_1.cram'))
        self.assertEqual(
            found[1], to_path('gs://cpg-dataset-main/exome/cram/SG01_2.cram')
        )
        self.assertEqual(missing[0], to_path('gs://cpg-dataset-main/cram/SG02.cram'))

    # ===== AUDIT ANALYZER TESTS =====
    # These test the core business logic - most complex tests

    def test_analyze_sequencing_groups_basic(self):
        """Test basic audit analysis"""
        # Test the core analysis
        result = self.audit_analyzer.analyze_sequencing_groups(
            list(SEQUENCING_GROUPS.values()),
            UPLOAD_BUCKET_FILES,
            list(ANALYSES.values()),
        )
        self.assertIsInstance(result, AuditResult)

        # Should have files to delete
        self.assertGreater(len(result.files_to_delete), 0)
        # Should have files to review (uningested file)
        self.assertGreater(len(result.files_to_review), 0)
        # Should mark SG as complete
        self.assertEqual(len(result.unaligned_sequencing_groups), 1)
        self.assertEqual(result.unaligned_sequencing_groups[0].id, 'SG04')

    def test_analyze_with_moved_files(self):
        """Test moved file detection"""
        analyses: list[Analysis] = []
        result = self.audit_analyzer.analyze_sequencing_groups(
            list(SEQUENCING_GROUPS.values()), UPLOAD_BUCKET_FILES, analyses
        )

        # Should detect the moved files
        self.assertGreater(len(result.moved_files), 0)
        self.assertEqual(
            result.moved_files[0].filepath,
            'gs://cpg-dataset-main-upload/unknown_files/unknown_R1.fq',
        )
        self.assertEqual(
            result.moved_files[1].filepath,
            'gs://cpg-dataset-main-upload/unknown_files/unknown_R2.fq',
        )

    def test_analyze_with_uningested_files(self):
        """Test uningested file detection"""
        analyses: list[Analysis] = []
        result = self.audit_analyzer.analyze_sequencing_groups(
            list(SEQUENCING_GROUPS.values()), UPLOAD_BUCKET_FILES, analyses
        )

        self.assertGreater(len(result.files_to_review), 0)
        for entry in result.files_to_review:
            self.assertIsNotNone(entry.filepath)

    # ===== FILE MATCHING SERVICE TESTS =====
    # These test the file matching logic
    def test_find_original_analysis_files(self):
        """Test matching Analysis files with bucket files based on checksum"""
        analyses = list(ANALYSES.values())
        self.file_matcher.find_original_analysis_files(analyses, MAIN_BUCKET_FILES)

        for analysis in analyses:
            if analysis.id == 7:
                self.assertIsNotNone(analysis.original_file)

    def test_find_uningested_files(self):
        """Test finding uningested files"""
        metamist_files: list[FileMetadata] = []
        bucket_files = UPLOAD_BUCKET_FILES[:1]

        moved_files = self.file_matcher.find_moved_files(metamist_files, bucket_files)
        self.assertEqual(len(moved_files), 0)

        uningested = self.file_matcher.find_uningested_files(
            metamist_files, bucket_files, moved_files
        )

        self.assertEqual(len(uningested), 1)
        self.assertEqual(
            uningested[0].filepath,
            to_path('gs://cpg-dataset-main-upload/2025-01-01/bams/EXT001.bam'),
        )

    # ===== INTEGRATION TESTS =====
    # These test multiple components working together

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_audit_config_instantiation_from_inputs(self, mock_query_async):
        """Test configuration creation from inputs"""
        config_args = SimpleNamespace(
            dataset='dataset',
            sequencing_types=('genome', 'exome'),
            sequencing_technologies=('short-read', 'long-read'),
            sequencing_platforms=('illumina', 'pacbio', 'ont'),
            analysis_types=('cram', 'vcf'),
            file_types=tuple(list(FileType)),
            excluded_prefixes=('<EXCLUDED_PREFIX>',),
            results_folder='results_folder',
        )
        mock_query_async.return_value = ENUMS_QUERY_RESULT

        config = AuditConfig.from_cli_args(config_args)
        self.assertIsInstance(config, AuditConfig)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    @unittest.mock.patch.object(GCSDataAccess, 'list_files_in_bucket')
    async def test_full_audit_workflow_write_reports(
        self, mock_list_files, mock_query_async
    ):
        """Integration test - combines multiple components"""
        mock_query_async.side_effect = [
            # First call: get sequencing groups
            ALL_SG_ASSAYS_RESPONSE,
            # Second call: get analyses for sequencing groups
            ALL_SG_ANALYSES_RESPONSE,
        ]
        mock_list_files.return_value = UPLOAD_BUCKET_FILES

        orchestrator = AuditOrchestrator(
            self.metamist_data_access,
            self.gcs_data_access,
            self.audit_analyzer,
            ReportGenerator(self.gcs_data_access, AuditLogs('dataset', 'test')),
            AuditLogs('dataset', 'test'),
        )

        config = AuditConfig(
            dataset='dataset',
            sequencing_types=('genome', 'exome'),
            sequencing_technologies=('short-read', 'long-read'),
            sequencing_platforms=('illumina', 'pacbio', 'ont'),
            analysis_types=('cram', 'vcf'),
            file_types=tuple(list(FileType)),
            excluded_prefixes=('<EXCLUDED_PREFIX>',),
        )

        # This would normally write files, so mock that too
        with unittest.mock.patch.object(
            ReportGenerator, 'write_audit_reports'
        ) as mock_write_reports:
            await orchestrator.run_audit(config)
            self.assertTrue(mock_write_reports.called)

    # ===== REPORT GENERATOR SERVICE TESTS =====
    # These test the file matching logic
    def test_write_audit_reports(self):
        """Test the ReportGenerator.write_audit_reports method."""
        audit_result = AuditResult(
            files_to_delete=[
                AuditReportEntry(filepath='gs://cpg-dataset-main-upload/file1.bam')
            ],
            files_to_review=[
                AuditReportEntry(filepath='gs://cpg-dataset-main-upload/file2.bam')
            ],
            moved_files=[
                AuditReportEntry(filepath='gs://cpg-dataset-main-upload/file3.bam')
            ],
            unaligned_sequencing_groups=[SEQUENCING_GROUPS['SG05']],
        )
        config = AuditConfig(
            dataset='dataset',
            sequencing_types=('genome', 'exome'),
            sequencing_technologies=('short-read', 'long-read'),
            sequencing_platforms=('illumina', 'pacbio', 'ont'),
            analysis_types=('cram', 'vcf'),
            file_types=tuple(list(FileType)),
            excluded_prefixes=('<EXCLUDED_PREFIX>',),
        )
        with unittest.mock.patch.object(
            ReportGenerator, '_write_csv_report'
        ) as mock_write_csv_report:
            self.report_generator.write_audit_reports(audit_result, config)
            self.assertTrue(mock_write_csv_report.called)

    # write_log_file
    # get_report_rows
    # get_report_rows_from_name
    # get_report_stats
    # get_report_entries_stats
    # generate_summary_statistics
    # AuditReportEntry from_report_dict
    # AuditReportEntry to_report_dict
    # AuditReportEntry fieldsname
    # AuditReportEntry update_action
    # AuditReportEntry update_review_comment

    # TODO
    # Review audit results tests
    # parse_filter_expressions
    # evalutate_filter_expression
    # review_audit_report
    # review_filtered_files
    # main

    # TODO
    # Delete from audit results tests
    # delete_from_audit_results
    # delete_files_from_report
    # upsert_deleted_files_analysis
    # main
