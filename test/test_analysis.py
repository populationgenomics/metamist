# pylint: disable=invalid-overridden-method
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.filters import GenericFilter
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


class TestAnalysis(DbIsolatedTest):
    """Test sample class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.sl = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)
        self.pl = ParticipantLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
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
    async def test_empty_query(self):
        """
        Test empty IDs to see the query construction
        """
        analyses = await self.al.query(AnalysisFilter(id=GenericFilter(in_=[])))
        self.assertEqual(len(analyses), 0)

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
                cohort_ids=[],
                output=None,
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            )
        ]

        self.assertEqual(analyses, expected)

    @run_as_sync
    async def test_get_sample_cram_path_map_for_seqr(self):
        """
        Exercise get_sample_cram_path_map_for_seqr()
        """

        part = await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'PEXT1'},
                    meta={},
                    samples=[SampleUpsertInternal(id=self.sample_id)],
                ),
            ],
        )

        id_map = await self.al.get_sample_cram_path_map_for_seqr(
            self.project_id, ['blood'], [part[0].id]
        )
        self.assertIsInstance(id_map, list)

    @run_as_sync
    async def test_get_sgs_by_analysis_id_with_no_eids(self):
        """
        Test get_sgs_by_analysis_id()
        """

        # duplicate here to ensure sgs don't have any external ids
        assay_meta = {
            'sequencing_type': 'genome',
            'sequencing_technology': 'short-read',
            'sequencing_platform': 'illumina',
        }
        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'test_sgs_aid'},
                type='blood',
                meta={},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta=assay_meta,
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta=assay_meta,
                            )
                        ],
                    ),
                ],
            )
        )

        genome_id = sample.sequencing_groups[0].id
        exome_id = sample.sequencing_groups[1].id

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[genome_id, exome_id],
                meta={},
            )
        )

        sgs_by_aid = await self.sgl.get_sequencing_groups_by_analysis_ids([a_id])
        self.assertIn(a_id, sgs_by_aid)
        sgs = sorted(sgs_by_aid[a_id], key=lambda sg: sg.id)

        self.assertEqual(sgs[0].id, genome_id)
        self.assertEqual(sgs[1].id, exome_id)
