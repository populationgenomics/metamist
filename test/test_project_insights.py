from db.python.layers import (
    AssayLayer,
    ParticipantLayer,
    ProjectInsightsLayer,
    SampleLayer,
)
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.testbase import DbIsolatedTest, run_as_sync


default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}


def get_test_participant():
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    )
                ],
            )
        ],
    )


class TestProjectInsights(DbIsolatedTest):
    """Test project insights class containing project insights endpoints"""

    maxDiff = None

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.partl = ParticipantLayer(self.connection)
        self.pil = ProjectInsightsLayer(self.connection)
        self.sampl = SampleLayer(self.connection)
        self.seql = AssayLayer(self.connection)

    @run_as_sync
    async def test_project_insights_summary(self):
        """Test getting the summaries for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.project, self.project_id)
        self.assertEqual(row.dataset, self.project_name)
        self.assertEqual(row.sequencing_type, 'genome')
        self.assertEqual(row.sequencing_technology, 'short-read')
        self.assertEqual(row.total_families, 0)
        self.assertEqual(row.total_participants, 1)
        self.assertEqual(row.total_samples, 1)
        self.assertEqual(row.total_sequencing_groups, 1)
        self.assertEqual(row.total_crams, 0)
        self.assertIsNone(row.latest_annotate_dataset)
        self.assertIsNone(row.latest_snv_es_index)
        self.assertIsNone(row.latest_sv_es_index)
        self.assertEqual(row.family_ids, [])
        self.assertEqual(len(row.participant_ids), 1)
        self.assertEqual(len(row.sample_ids), 1)

    @run_as_sync
    async def test_project_insights_details(self):
        """Test getting the details for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        # There's not enough data set up to usefully verify the result
        _ = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )
