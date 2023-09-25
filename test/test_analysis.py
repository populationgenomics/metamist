# pylint: disable=invalid-overridden-method
from datetime import datetime, timedelta
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.utils import GenericFilter
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


class TestAnalysis(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.sl = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test01',
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'genome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'exome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                ],
            )
        )
        self.sample_id = sample.id
        self.genome_sequencing_group_id = sample.sequencing_groups[0].id
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id

    @run_as_sync
    async def test_get_analysis_by_id(self):
        """
        Test getting an analysis by id
        """
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        analysis = await self.al.get_analysis_by_id(analysis_id)
        self.assertEqual(analysis_id, analysis.id)
        self.assertEqual('cram', analysis.type)
        self.assertEqual(AnalysisStatus.COMPLETED, analysis.status)

    @run_as_sync
    async def test_add_cram(self):
        """
        Test adding an analysis of type CRAM
        """

        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        analyses = await self.connection.connection.fetch_all('SELECT * FROM analysis')
        analysis_sgs = await self.connection.connection.fetch_all(
            'SELECT * FROM analysis_sequencing_group'
        )

        self.assertEqual(1, len(analyses))
        self.assertEqual(analysis_id, analyses[0]['id'])
        self.assertEqual(1, analysis_sgs[0]['sequencing_group_id'])
        self.assertEqual(analyses[0]['id'], analysis_sgs[0]['analysis_id'])

    @run_as_sync
    async def test_get_analysis(self):
        """
        Test adding an analysis of type ANALYSIS_RUNNER
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={},
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                type=GenericFilter(eq='analysis-runner'),
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                output=None,
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author='testuser',
            )
        ]

        self.assertEqual(analyses, expected)

    @run_as_sync
    async def test_get_sequencing_group_file_sizes_single(self):
        """
        Test retrieval of sample file sizes over time
        """

        today = datetime.utcnow().date()

        # insert analysis
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        result = await self.al.get_sequencing_group_file_sizes(
            project_ids=[self.project_id]
        )
        expected = {
            'project': self.project_id,
            'sequencing_groups': {
                self.genome_sequencing_group_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 1024,
                    }
                ]
            },
        }

        # Check output is formatted correctly
        self.assertEqual(1, len(result))
        self.assertDictEqual(expected, result[0])

    @run_as_sync
    async def test_get_sequencing_group_file_sizes_single_sample_double_sg(self):
        """
        Test getting file sizes for sequencing groups
            1 sample, w/ 2 sequencing groups
        """
        today = datetime.utcnow().date()

        # Add genome cram
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        # Add exome cram
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.exome_sequencing_group_id],
                meta={'sequencing_type': 'exome', 'size': 3141},
            )
        )
        expected = {
            'project': self.project_id,
            'sequencing_groups': {
                self.genome_sequencing_group_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 1024,
                    }
                ],
                self.exome_sequencing_group_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 3141,
                    }
                ],
            },
        }

        # Assert that the exome size was added correctly
        result = await self.al.get_sequencing_group_file_sizes(
            project_ids=[self.project_id]
        )
        self.assertDictEqual(expected, result[0])

    @run_as_sync
    async def test_get_sequencing_group_file_sizes_exclusive_date_range(self):
        """
        Exclusive date range returning no data
        """
        today = datetime.utcnow().date()

        # Assert that if we select a date range outside of sample creation date
        # that is doesn't show up in the map
        yesterday = today - timedelta(days=3)
        result = await self.al.get_sequencing_group_file_sizes(
            project_ids=[self.project_id], end_date=yesterday
        )

        self.assertEqual([], result)

    @run_as_sync
    async def test_get_sequencing_group_file_sizes_newer_sample(self):
        """
        Update analysis for sequencing group, making sure we only return the later one
        """
        today = datetime.utcnow().date()

        # Add genome cram that's older
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        # Add another genome cram that's newer
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 11111},
            )
        )
        expected = {
            'project': self.project_id,
            'sequencing_groups': {
                self.genome_sequencing_group_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 11111,
                    }
                ]
            },
        }

        result = await self.al.get_sequencing_group_file_sizes(
            project_ids=[self.project_id]
        )
        self.assertDictEqual(expected, result[0])

    @run_as_sync
    async def test_get_sequencing_group_file_sizes_two_samples(self):
        """Test that it works for two samples"""
        today = datetime.utcnow().date()

        # Add another sample and it's analysis cram as well
        sample_2 = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test02',
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'genome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    )
                ],
            )
        )
        # add for sg 1
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        sequencing_group_2_id = sample_2.sequencing_groups[0].id
        await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sequencing_group_2_id],
                meta={'sequencing_type': 'genome', 'size': 987654321},
            )
        )

        expected = {
            'project': self.project_id,
            'sequencing_groups': {
                self.genome_sequencing_group_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 1024,
                    }
                ],
                sequencing_group_2_id: [
                    {
                        'start': today,
                        'end': None,
                        'size': 987654321,
                    }
                ],
            },
        }

        result = await self.al.get_sequencing_group_file_sizes(
            project_ids=[self.project_id]
        )
        self.assertDictEqual(expected, result[0])
