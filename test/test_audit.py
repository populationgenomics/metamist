# noqa: B006

import unittest
import unittest.mock
from test.testbase import run_as_sync

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
    Analysis,
    Assay,
    Sample,
    Participant,
    ExternalIds,
    ReadFile,
    FileMetadata,
    FilePath,
    AuditConfig,
    FileType,
    AuditResult,
)

from cpg_utils import to_path


class TestAudit(unittest.TestCase):  # pylint: disable=too-many-instance-attributes
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
        self.audit_analyzer = AuditAnalyzer()
        self.file_matcher = FileMatchingService()

    def tearDown(self):
        """Clean up test fixtures."""
        self.storage_client_patcher.stop()
        self.bucket_patcher.stop()

    # ===== METAMIST DATA ACCESS TESTS =====
    # These test the data access layer - mapping GraphQL responses to domain models

    def test_gcs_data_access_setup(self):
        """Test that GCSDataAccess initializes correctly with mocked buckets"""
        # Verify that the bucket names are set correctly from our mocks
        self.assertEqual(self.gcs_data_access.main_bucket, 'cpg-dataset-main')
        self.assertEqual(self.gcs_data_access.upload_bucket, 'cpg-dataset-main-upload')
        self.assertEqual(
            self.gcs_data_access.analysis_bucket, 'cpg-dataset-main-analysis'
        )
        self.assertEqual(self.gcs_data_access.dataset, 'dataset')
        self.assertEqual(self.gcs_data_access.gcp_project, 'test')

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_sequencing_groups"""
        mock_query_async.return_value = {
            'project': {
                'sequencingGroups': [
                    {
                        'id': 'CPGaaa',
                        'type': 'genome',
                        'technology': 'short-read',
                        'platform': 'illumina',
                        'sample': {
                            'id': 'XPG123',
                            'externalIds': {'': 'EXT123'},
                            'participant': {'id': 1, 'externalIds': {'': 'P01'}},
                        },
                        'assays': [
                            {
                                'id': 1,
                                'meta': {
                                    'reads': [
                                        {
                                            'location': 'gs://cpg-dataset-main-upload/R1.fq',
                                            'size': 11,
                                            'checksum': 'abc123',
                                        },
                                        {
                                            'location': 'gs://cpg-dataset-main-upload/R2.fq',
                                            'size': 12,
                                            'checksum': 'def456',
                                        },
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        }

        # Test the new method
        result = await self.metamist_data_access.get_sequencing_groups(
            dataset='test',
            sequencing_types=['genome'],
            sequencing_technologies=['short-read'],
            sequencing_platforms=['illumina'],
        )

        # Assertions using new domain models
        self.assertEqual(len(result), 1)
        sg = result[0]
        self.assertIsInstance(sg, SequencingGroup)
        self.assertEqual(sg.id, 'CPGaaa')
        self.assertEqual(sg.type, 'genome')
        self.assertEqual(len(sg.assays), 1)

        assay = sg.assays[0]
        self.assertEqual(assay.id, 1)
        self.assertEqual(len(assay.read_files), 2)

        read_file_1 = assay.read_files[0]
        self.assertEqual(read_file_1.filepath.uri, 'gs://cpg-dataset-main-upload/R1.fq')
        self.assertEqual(read_file_1.filesize, 11)

        read_file_2 = assay.read_files[1]
        self.assertEqual(read_file_2.filepath.uri, 'gs://cpg-dataset-main-upload/R2.fq')
        self.assertEqual(read_file_2.filesize, 12)

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    async def test_get_analyses_for_sequencing_groups(self, mock_query_async):
        """Test MetamistDataAccess.get_analyses_for_sequencing_groups"""
        mock_query_async.return_value = {
            'project': {
                'sequencingGroups': [
                    {
                        'id': 'CPGaaa',
                        'analyses': [
                            {
                                'id': 1,
                                'type': 'cram',
                                'status': 'completed',
                                'meta': {
                                    'sequencing_type': 'genome',
                                },
                                'output': 'gs://cpg-dataset-main/cram/CPGaaa.cram',
                                'timestampCompleted': '2023-05-11T16:33:00',
                            },
                        ],
                    },
                ]
            }
        }

        result = await self.metamist_data_access.get_analyses_for_sequencing_groups(
            dataset='test', sg_ids=['CPGaaa'], analysis_types=['cram']
        )

        self.assertEqual(len(result), 1)
        analysis = result[0]
        self.assertIsInstance(analysis, Analysis)
        self.assertEqual(analysis.id, 1)
        self.assertEqual(analysis.type, 'cram')
        self.assertEqual(analysis.sequencing_group_id, 'CPGaaa')
        self.assertTrue(analysis.is_cram)
        self.assertEqual(
            analysis.output_file.filepath.uri, 'gs://cpg-dataset-main/cram/CPGaaa.cram'
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
            dataset='test',
            output_path='gs://cpg-dataset-main-analysis/audit_results/20250901_1633/deleted_file.csv',
        )

        self.assertIsInstance(result, dict)
        self.assertEqual(result['id'], 1)

    # ===== GCS DATA ACCESS TESTS =====
    # These test the data access layer - mapping GCS responses to domain models

    @unittest.mock.patch.object(GCSDataAccess, 'list_files_in_bucket')
    def test_list_files_in_bucket(self, mock_list_files):
        """Test listing the files in a bucket"""
        mock_list_files.return_value = [
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main-upload/read1.fq'))
            ),
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main-upload/read2.fq'))
            ),
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main-upload/uningested.fq'))
            ),
        ]

        blobs = self.gcs_data_access.list_files_in_bucket(
            bucket_name='cpg-dataset-main-upload',
            file_types=[
                FileType.FASTQ,
            ],
        )

        self.assertEqual(len(blobs), 3)
        self.assertEqual(
            blobs[0].filepath,
            FilePath(to_path('gs://cpg-dataset-main-upload/read1.fq')),
        )
        self.assertEqual(
            blobs[1].filepath,
            FilePath(to_path('gs://cpg-dataset-main-upload/read2.fq')),
        )
        self.assertEqual(
            blobs[2].filepath,
            FilePath(to_path('gs://cpg-dataset-main-upload/uningested.fq')),
        )

    @unittest.mock.patch.object(StorageClient, 'check_blobs')
    def test_validate_cram_files(self, mock_check_blobs):
        """Test validating CRAM files"""
        mock_check_blobs.return_value = [
            FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram')),
            FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram')),
        ]

        found, missing = self.gcs_data_access.validate_cram_files(
            cram_paths=[
                FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram')),
                FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram')),
            ]
        )

        self.assertEqual(len(found), 2)
        self.assertEqual(len(missing), 0)
        self.assertEqual(
            found[0], FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram'))
        )
        self.assertEqual(
            found[1], FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram'))
        )

    @unittest.mock.patch.object(StorageClient, 'check_blobs')
    def test_validate_cram_files_with_missing(self, mock_check_blobs):
        """Test validating CRAM files where one is missing"""
        # Mock check_blobs to return only 2 of the 3 requested files
        mock_check_blobs.return_value = [
            FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram')),
            FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram')),
        ]

        found, missing = self.gcs_data_access.validate_cram_files(
            cram_paths=[
                FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram')),
                FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram')),
                FilePath(to_path('gs://cpg-dataset-main/cram/CPGccc.cram')),
            ]
        )

        self.assertEqual(len(found), 2)
        self.assertEqual(len(missing), 1)
        self.assertEqual(
            found[0], FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram'))
        )
        self.assertEqual(
            found[1], FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram'))
        )
        self.assertEqual(
            missing[0], FilePath(to_path('gs://cpg-dataset-main/cram/CPGccc.cram'))
        )

    # ===== AUDIT ANALYZER TESTS =====
    # These test the core business logic - most complex tests

    def test_analyze_sequencing_groups_basic(self):
        """Test basic audit analysis"""
        sequencing_groups = [
            SequencingGroup(
                id='CPGaaa',
                type='genome',
                technology='short-read',
                platform='illumina',
                sample=Sample(
                    id='XPG123',
                    external_ids=ExternalIds({'': 'EXT123'}),
                    participant=Participant(
                        id=1, external_ids=ExternalIds({'': 'P01'})
                    ),
                ),
                assays=[
                    Assay(
                        id=1,
                        read_files=[
                            ReadFile(
                                FileMetadata(
                                    filepath=FilePath(
                                        to_path('gs://cpg-dataset-main-upload/read1.fq')
                                    ),
                                    filesize=11,
                                    checksum='abc123',
                                )
                            ),
                            ReadFile(
                                FileMetadata(
                                    filepath=FilePath(
                                        to_path('gs://cpg-dataset-main-upload/read2.fq')
                                    ),
                                    filesize=12,
                                    checksum='def456',
                                )
                            ),
                        ],
                    )
                ],
            )
        ]

        bucket_files = [
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main-upload/read1.fq')),
                filesize=11,
                checksum='abc123',
            ),
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main-upload/read2.fq')),
                filesize=12,
                checksum='def456',
            ),
            FileMetadata(
                filepath=FilePath(
                    to_path('gs://cpg-dataset-main-upload/uningested.fq')
                ),
                filesize=20,
                checksum='xyz789',
            ),
        ]

        analyses = [
            Analysis(
                id=1,
                type='cram',
                sequencing_group_id='CPGaaa',
                output_file=FileMetadata(
                    filepath=FilePath(
                        to_path('gs://cpg-dataset-main/cram/CPGaaa.cram')
                    ),
                ),
            )
        ]

        # Test the core analysis
        result = self.audit_analyzer.analyze_sequencing_groups(
            sequencing_groups, bucket_files, analyses
        )

        # Verify results
        self.assertIsInstance(result, AuditResult)

        # Should have files to delete (original reads, since CRAM exists)
        self.assertGreater(len(result.files_to_delete), 0)

        # Should have files to review (uningested file)
        self.assertGreater(len(result.files_to_review), 0)

        # Should mark SG as complete
        self.assertEqual(len(result.unaligned_sequencing_groups), 0)

    def test_analyze_with_moved_files(self):
        """Test moved file detection"""
        sequencing_groups = [
            SequencingGroup(
                id='CPGaaa',
                type='genome',
                technology='short-read',
                platform='illumina',
                sample=Sample(
                    id='XPG123',
                    external_ids=ExternalIds({'': 'EXT123'}),
                    participant=Participant(
                        id=1, external_ids=ExternalIds({'': 'P01'})
                    ),
                ),
                assays=[
                    Assay(
                        id=1,
                        read_files=[
                            ReadFile(
                                FileMetadata(
                                    filepath=FilePath(
                                        to_path('gs://cpg-test-upload/dir1/read3.fq')
                                    ),
                                    filesize=12,
                                    checksum='hash123',
                                )
                            )
                        ],
                    )
                ],
                cram_analysis=Analysis(id=1, type='cram'),
            )
        ]

        # File was moved to a different directory
        bucket_files = [
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-test-upload/dir2/read3.fq')),
                filesize=12,
                checksum='hash123',  # Same checksum indicates it's the same file
            )
        ]

        analyses: list[Analysis] = []

        result = self.audit_analyzer.analyze_sequencing_groups(
            sequencing_groups, bucket_files, analyses
        )

        # Should detect the moved file
        self.assertGreater(len(result.moved_files), 0)
        moved_file = result.moved_files[0]
        self.assertEqual(moved_file.filepath, 'gs://cpg-test-upload/dir2/read3.fq')

    # ===== FILE MATCHING SERVICE TESTS =====
    # These test the file matching logic

    def test_find_moved_files(self):
        """Test find moved files"""

        read_files = [
            ReadFile(
                FileMetadata(
                    filepath=FilePath(to_path('gs://bucket/original.fq')),
                    filesize=100,
                    checksum='abc123',
                )
            )
        ]

        bucket_files = [
            FileMetadata(
                filepath=FilePath(to_path('gs://bucket/moved.fq')),
                filesize=100,
                checksum='abc123',
            )
        ]

        moved_files = self.file_matcher.find_moved_files(read_files, bucket_files)
        moved_file = list(moved_files.values())[0]

        self.assertEqual(len(moved_files), 1)
        self.assertEqual(
            moved_file.old_path, FilePath(to_path('gs://bucket/original.fq'))
        )
        self.assertEqual(moved_file.new_path, FilePath(to_path('gs://bucket/moved.fq')))

    def test_find_original_analysis_files(self):
        """Test matching Analysis files with bucket files based on checksum"""

        analyses = [
            Analysis(
                id=1,
                type='vcf',
                output_file=FileMetadata(
                    filepath=FilePath(to_path('gs://bucket/analysis1.vcf')),
                    filesize=200,
                    checksum='def456',
                ),
            )
        ]

        bucket_files = [
            FileMetadata(
                filepath=FilePath(to_path('gs://bucket/analysis1.vcf')),
                filesize=200,
                checksum='def456',
            )
        ]

        assert analyses[0].original_file is None

        self.file_matcher.find_original_analysis_files(analyses, bucket_files)

        assert analyses[0].original_file is not None

    def test_find_uningested_files(self):
        """Test finding uningested files"""

        metamist_files = [
            ReadFile(
                FileMetadata(
                    filepath=FilePath(to_path('gs://bucket/sample.fq')),
                    filesize=100,
                    checksum='abc123',
                )
            )
        ]

        bucket_files = [
            FileMetadata(
                filepath=FilePath(to_path('gs://bucket/dir/sample.fq')),
                filesize=100,
                checksum='abc123',
            ),
            FileMetadata(
                filepath=FilePath(to_path('gs://bucket/uningested.fq')),
                filesize=100,
                checksum='def456',
            ),
        ]

        moved_files = self.file_matcher.find_moved_files(metamist_files, bucket_files)

        self.assertEqual(len(moved_files), 1)

        uningested = self.file_matcher.find_uningested_files(
            metamist_files, bucket_files, moved_files
        )

        self.assertEqual(len(uningested), 1)
        self.assertEqual(
            uningested[0].filepath, FilePath(to_path('gs://bucket/uningested.fq'))
        )

    # ===== INTEGRATION TESTS =====
    # These test multiple components working together

    @run_as_sync
    @unittest.mock.patch('metamist.audit.adapters.graphql_client.query_async')
    @unittest.mock.patch.object(GCSDataAccess, 'list_files_in_bucket')
    async def test_full_audit_workflow(self, mock_list_files, mock_query_async):
        """Integration test - combines multiple components"""

        mock_query_async.side_effect = [
            # First call: get sequencing groups
            {
                'project': {
                    'sequencingGroups': [
                        {
                            'id': 'CPGaaa',
                            'type': 'genome',
                            'technology': 'short-read',
                            'platform': 'illumina',
                            'sample': {
                                'id': 'XPG123',
                                'externalIds': {'': 'EXT123'},
                                'participant': {'id': 1, 'externalIds': {'': 'P01'}},
                            },
                            'assays': [
                                {
                                    'id': 1,
                                    'meta': {
                                        'reads': [
                                            {
                                                'location': 'gs://cpg-dataset-main-upload/R1.fq',
                                                'size': 11,
                                                'checksum': 'abc123',
                                            },
                                            {
                                                'location': 'gs://cpg-dataset-main-upload/R2.fq',
                                                'size': 12,
                                                'checksum': 'def456',
                                            },
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
                }
            },
            # Second call: get analyses for sequencing groups
            {
                'project': {
                    'sequencingGroups': [
                        {
                            'id': 'CPGaaa',
                            'analyses': [
                                {
                                    'id': 1,
                                    'type': 'cram',
                                    'status': 'completed',
                                    'meta': {
                                        'sequencing_type': 'genome',
                                    },
                                    'output': 'gs://cpg-dataset-main/cram/CPGaaa.cram',
                                    'timestampCompleted': '2023-05-11T16:33:00',
                                },
                            ],
                        },
                    ]
                }
            },
        ]

        mock_list_files.return_value = [
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main/cram/CPGaaa.cram'))
            ),
            FileMetadata(
                filepath=FilePath(to_path('gs://cpg-dataset-main/cram/CPGbbb.cram'))
            ),
        ]

        orchestrator = AuditOrchestrator(
            self.metamist_data_access,
            self.gcs_data_access,
            self.audit_analyzer,
            ReportGenerator(self.gcs_data_access, AuditLogs('test', 'test')),
            AuditLogs('test', 'test'),
        )

        config = AuditConfig(
            dataset='test',
            sequencing_types=('genome',),
            sequencing_technologies=('short-read',),
            sequencing_platforms=('illumina',),
            analysis_types=('cram',),
            file_types=(FileType.FASTQ,),
        )

        # This would normally write files, so mock that too
        with unittest.mock.patch.object(
            ReportGenerator, 'write_audit_reports'
        ) as mock_write_reports:
            await orchestrator.run_audit(config)
            self.assertTrue(mock_write_reports.called)
