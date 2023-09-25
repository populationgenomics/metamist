from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers import (
    AssayLayer,
    ParticipantLayer,
    SampleLayer,
    SequencingGroupLayer,
    WebLayer,
)
from models.enums import MetaSearchEntityPrefix
from models.models import (
    Assay,
    AssayInternal,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    ProjectSummaryInternal,
    SampleUpsertInternal,
    SearchItem,
    SequencingGroupUpsertInternal,
    WebProject,
)
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}


def data_to_class(data: dict | list) -> dict | list:
    """Convert the data into it's class using the _class field"""
    if isinstance(data, list):
        return [data_to_class(x) for x in data]

    if not isinstance(data, dict):
        return data

    cls = data.pop('_class', None)
    mapped_data = {k: data_to_class(v) for k, v in data.items()}

    if isinstance(cls, type):
        return cls(**mapped_data)

    return mapped_data


def merge(d1: dict, d2: dict):
    """Merges two dictionaries"""
    return dict(d1, **d2)


def get_test_participant():
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_id='Demeter',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id001',
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


def get_test_participant_2():
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_id='Meter',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id002',
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='Illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id002.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R1.fastq.gz',
                                            'size': 112,
                                        },
                                        {
                                            'basename': 'sample_id002.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R2.fastq.gz',
                                            'size': 112,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    'field with spaces': 'field with spaces',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    ),
                ],
            )
        ],
    )


SINGLE_PARTICIPANT_RESULT = ProjectSummaryInternal(
    project=WebProject(id=1, name='test', meta={}, dataset='test'),
    total_samples=1,
    total_samples_in_query=1,
    total_participants=1,
    total_sequencing_groups=1,
    total_assays=1,
    cram_seqr_stats={
        'genome': {
            'Sequences': '1',
            'Crams': '0',
            'Seqr': '0',
        }
    },
    batch_sequencing_group_stats={'M001': {'genome': '1'}},
    participants=[],
    participant_keys=[('external_id', 'Participant ID')],
    sample_keys=[
        ('id', 'Sample ID'),
        ('external_id', 'External Sample ID'),
        ('created_date', 'Created date'),
    ],
    sequencing_group_keys=[
        ('id', 'Sequencing Group ID'),
        ('platform', 'Platform'),
        ('technology', 'Technology'),
        ('type', 'Type'),
    ],
    assay_keys=[
        ('type', 'type'),
        ('meta.batch', 'batch'),
        ('meta.reads_type', 'reads_type'),
    ],
    seqr_links={},
    seqr_sync_types=[],
)


class TestWeb(DbIsolatedTest):
    """Test web class containing web endpoints"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.webl = WebLayer(self.connection)
        self.partl = ParticipantLayer(self.connection)
        self.sampl = SampleLayer(self.connection)
        self.seql = AssayLayer(self.connection)

    @run_as_sync
    async def test_project_summary_empty(self):
        """Test getting the summary for a project"""
        result = await self.webl.get_project_summary(token=0, grid_filter=[])

        # Expect an empty project
        expected = ProjectSummaryInternal(
            project=WebProject(id=1, name='test', meta={}, dataset='test'),
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequencing_groups=0,
            total_assays=0,
            batch_sequencing_group_stats={},
            cram_seqr_stats={},
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequencing_group_keys=[],
            assay_keys=[],
            seqr_links={},
            seqr_sync_types=[],
        )

        self.assertEqual(expected, result)

    @run_as_sync
    async def test_project_summary_single_entry(self):
        """Test project summary with a single participant with all fields"""
        # Now add a participant with a sample and sequence
        await self.partl.upsert_participants(participants=[get_test_participant()])

        result = await self.webl.get_project_summary(token=0, grid_filter=[])

        result.participants = []
        self.assertEqual(SINGLE_PARTICIPANT_RESULT, result)

    @run_as_sync
    async def test_project_summary_to_external(self):
        """Test project summary to_external function"""
        # Now add a participant with a sample and sequence
        await self.partl.upsert_participants(participants=[get_test_participant()])

        result = await self.webl.get_project_summary(token=0, grid_filter=[])

        ex_result = result.to_external(links=None)

        self.assertIsInstance(result.participants[0].samples[0].id, int)
        self.assertIsInstance(ex_result.participants[0].samples[0].id, str)
        self.assertEqual(
            result.participants[0].samples[0].id,
            sample_id_transform_to_raw(ex_result.participants[0].samples[0].id),
        )

        self.assertIsInstance(
            result.participants[0].samples[0].sequencing_groups[0].id, int
        )
        self.assertIsInstance(
            ex_result.participants[0].samples[0].sequencing_groups[0].id, str
        )
        self.assertEqual(
            result.participants[0].samples[0].sequencing_groups[0].id,
            sequencing_group_id_transform_to_raw(
                ex_result.participants[0].samples[0].sequencing_groups[0].id
            ),
        )

        self.assertIsInstance(
            result.participants[0].samples[0].sequencing_groups[0].assays[0],
            AssayInternal,
        )
        self.assertIsInstance(
            ex_result.participants[0].samples[0].sequencing_groups[0].assays[0], Assay
        )

    @run_as_sync
    async def project_summary_with_filter_with_results(self):
        """Project grid but with test filter, that shows results"""
        await self.partl.upsert_participants(participants=[get_test_participant()])

        filtered_result_success = await self.webl.get_project_summary(
            token=0,
            grid_filter=[
                SearchItem(
                    model_type=MetaSearchEntityPrefix.ASSAY,
                    query='M001',
                    field='batch',
                    is_meta=True,
                )
            ],
        )
        filtered_result_success.participants = []
        self.assertEqual(SINGLE_PARTICIPANT_RESULT, filtered_result_success)

    @run_as_sync
    async def project_summary_with_filter_no_results(self):
        """Project grid but with test filter, that doesn't have results"""
        filtered_result_empty = await self.webl.get_project_summary(
            token=0,
            grid_filter=[
                SearchItem(
                    model_type=MetaSearchEntityPrefix.ASSAY,
                    query='M002',
                    field='batch',
                    is_meta=True,
                )
            ],
        )
        empty_result = ProjectSummaryInternal(
            project=WebProject(id=1, name='test', meta={}, dataset='test'),
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequencing_groups=0,
            total_assays=0,
            batch_sequencing_group_stats={},
            cram_seqr_stats={},
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequencing_group_keys=[],
            assay_keys=[],
            seqr_links={},
            seqr_sync_types=[],
        )

        self.assertEqual(empty_result, filtered_result_empty)

    @run_as_sync
    async def test_project_summary_multiple_participants(self):
        """Try with multiple participants as some extra security"""
        await self.partl.upsert_participants(
            participants=[get_test_participant(), get_test_participant_2()]
        )

        expected_data_two_samples = ProjectSummaryInternal(
            project=WebProject(id=1, name='test', meta={}, dataset='test'),
            total_samples=2,
            total_samples_in_query=2,
            total_participants=2,
            total_sequencing_groups=2,
            total_assays=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequencing_group_stats={'M001': {'genome': '2'}},
            participants=[],  # data_to_class(expected_data_list),
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequencing_group_keys=[
                ('id', 'Sequencing Group ID'),
                ('platform', 'Platform'),
                ('technology', 'Technology'),
                ('type', 'Type'),
            ],
            assay_keys=[
                ('type', 'type'),
                ('meta.batch', 'batch'),
                ('meta.field with spaces', 'field with spaces'),
                ('meta.reads_type', 'reads_type'),
            ],
            seqr_links={},
            seqr_sync_types=[],
        )

        two_samples_result = await self.webl.get_project_summary(
            token=0, grid_filter=[]
        )

        two_samples_result.participants = []

        self.assertEqual(expected_data_two_samples, two_samples_result)

    @run_as_sync
    async def test_project_summary_multiple_participants_and_filter(self):
        """Try with multiple participants as some extra security"""
        await self.partl.upsert_participants(
            participants=[get_test_participant(), get_test_participant_2()]
        )

        expected_data_two_samples_filtered = ProjectSummaryInternal(
            project=WebProject(id=1, name='test', meta={}, dataset='test'),
            total_samples=2,
            total_samples_in_query=1,
            total_participants=2,
            total_sequencing_groups=2,
            total_assays=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequencing_group_stats={'M001': {'genome': '2'}},
            participants=[],  # data_to_class(expected_data_list_filtered),
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequencing_group_keys=[
                ('id', 'Sequencing Group ID'),
                ('platform', 'Platform'),
                ('technology', 'Technology'),
                ('type', 'Type'),
            ],
            assay_keys=[
                ('type', 'type'),
                ('meta.batch', 'batch'),
                ('meta.field with spaces', 'field with spaces'),
                ('meta.reads_type', 'reads_type'),
            ],
            seqr_links={},
            seqr_sync_types=[],
        )

        two_samples_result_filtered = await self.webl.get_project_summary(
            token=0,
            grid_filter=[
                SearchItem(
                    model_type=MetaSearchEntityPrefix.SAMPLE,
                    query='sample_id002',
                    field='external_id',
                    is_meta=False,
                )
            ],
        )
        two_samples_result_filtered.participants = []

        self.assertEqual(
            expected_data_two_samples_filtered, two_samples_result_filtered
        )

    @run_as_sync
    async def test_field_with_space(self):
        """Test filtering on a meta field with spaces"""
        await self.partl.upsert_participants(
            participants=[get_test_participant(), get_test_participant_2()]
        )
        print(await self.connection.connection.fetch_all('SELECT * FROM assay'))
        test_field_with_space = await self.webl.get_project_summary(
            token=0,
            grid_filter=[
                SearchItem(
                    model_type=MetaSearchEntityPrefix.ASSAY,
                    query='field wi',
                    field='field with spaces',
                    is_meta=True,
                )
            ],
        )
        self.assertEqual(1, len(test_field_with_space.participants))
        test_field_with_space.participants = []

        expected_data_two_samples_filtered = ProjectSummaryInternal(
            project=WebProject(id=1, name='test', meta={}, dataset='test'),
            total_samples=2,
            total_samples_in_query=1,
            total_participants=2,
            total_sequencing_groups=2,
            total_assays=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequencing_group_stats={'M001': {'genome': '2'}},
            participants=[],
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequencing_group_keys=[
                ('id', 'Sequencing Group ID'),
                ('platform', 'Platform'),
                ('technology', 'Technology'),
                ('type', 'Type'),
            ],
            assay_keys=[
                ('type', 'type'),
                ('meta.batch', 'batch'),
                ('meta.field with spaces', 'field with spaces'),
                ('meta.reads_type', 'reads_type'),
            ],
            seqr_links={},
            seqr_sync_types=[],
        )

        self.assertEqual(expected_data_two_samples_filtered, test_field_with_space)

    @run_as_sync
    async def test_project_summary_inactive_sequencing_group(self):
        """
        Insert a sequencing-group, archive it, then check that the summary
        doesn't return that sequencing group
        """
        participants = await self.partl.upsert_participants(
            participants=[get_test_participant()]
        )
        sg = participants[0].samples[0].sequencing_groups[0]
        assay_ids = [a.id for a in sg.assays]
        sglayer = SequencingGroupLayer(self.connection)
        new_sg_id = await sglayer.recreate_sequencing_group_with_new_assays(
            sequencing_group_id=sg.id,
            assays=assay_ids,
            meta={'new-meta': 'value'},
        )

        psummary = await self.webl.get_project_summary(grid_filter=[])

        summary_sgs = psummary.participants[0].samples[0].sequencing_groups
        self.assertEqual(1, len(summary_sgs))
        self.assertEqual(new_sg_id, summary_sgs[0].id)
