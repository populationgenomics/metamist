from db.python.layers.assay import AssayLayer, AssayUpsertInternal
from db.python.layers.participant import (
    ParticipantLayer,
    ParticipantUpsertInternal,
)
from db.python.layers.sample import SampleLayer, SampleUpsertInternal
from db.python.layers.sequencing_group import SequencingGroupUpsertInternal
from db.python.layers.web import (
    WebLayer,
    ProjectSummary,
    WebProject,
    SearchItem,
    MetaSearchEntityPrefix,
)
from test.testbase import DbIsolatedTest, run_as_sync

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


SINGLE_PARTICIPANT_UPSERT = ParticipantUpsertInternal(
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
                                    [
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
                                    ]
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

SINGLE_PARTICIPANT_RESULT = ProjectSummary(
    project=WebProject(id=1, name='test', meta={}, dataset='test'),
    total_samples=1,
    total_samples_in_query=1,
    total_participants=1,
    total_sequences=1,
    cram_seqr_stats={
        'genome': {
            'Sequences': '1',
            'Crams': '0',
            'Seqr': '0',
        }
    },
    batch_sequence_stats={'M001': {'genome': '1'}},
    participants=[],
    participant_keys=[('external_id', 'Participant ID')],
    sample_keys=[
        ('id', 'Sample ID'),
        ('external_id', 'External Sample ID'),
        ('created_date', 'Created date'),
    ],
    sequencing_group_keys=[
        ('id', 'Sequencing Group ID'),
        ('created_date', 'Created date'),
    ],
    assay_keys=[
        ('type', 'type'),
        ('technology', 'technology'),
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
        expected = ProjectSummary(
            project=WebProject(
                **{'id': 1, 'name': 'test', 'meta': {}, 'dataset': 'test'}
            ),
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequences=0,
            batch_sequence_stats={},
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
        await self.partl.upsert_participants(participants=[SINGLE_PARTICIPANT_UPSERT])

        result = await self.webl.get_project_summary(token=0, grid_filter=[])

        result.participants = []
        self.assertEqual(SINGLE_PARTICIPANT_RESULT, result)

    @run_as_sync
    async def project_summary_with_filter_with_results(self):
        await self.partl.upsert_participants(participants=[SINGLE_PARTICIPANT_UPSERT])

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
        empty_result = ProjectSummary(
            project=WebProject(
                **{'id': 1, 'name': 'test', 'meta': {}, 'dataset': 'test'}
            ),
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequences=0,
            batch_sequence_stats={},
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
        p2 = ParticipantUpsertInternal(
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
                                            **default_assay_meta,
                                        },
                                    ),
                                ],
                            ),
                        ],
                    )
                ],
            )


        await self.partl.upsert_participants(
            participants=[SINGLE_PARTICIPANT_UPSERT, p2]
        )

        expected_data_two_samples = ProjectSummary(
            project=WebProject(
                id=1, name='test', meta={}, dataset='test'
            ),
            total_samples=2,
            total_samples_in_query=2,
            total_participants=2,
            total_sequences=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequence_stats={'M001': {'genome': '2'}},
            participants=[],  # data_to_class(expected_data_list),
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequencing_group_keys=[
                ('id', 'Sequencing Group ID'),
                ('created_date', 'Created date'),
            ],
            assay_keys=[
                ('type', 'type'),
                ('technology', 'technology'),
                ('meta.batch', 'batch'),
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

        # expected_data_list_filtered: list = [
        #     merge(
        #         participant,
        #         {
        #             '_class': NestedParticipant,
        #             'id': 2,
        #             'families': [],
        #             'samples': [
        #                 merge(
        #                     sample,
        #                     {
        #                         '_class': NestedSample,
        #                         'id': 2,
        #                         'created_date': str(date.today()),
        #                         'sequences': [
        #                             merge(
        #                                 sequence,
        #                                 {
        #                                     '_class': NestedSequence,
        #                                     'id': 2,
        #                                 },
        #                             )
        #                             for sequence in sample.get('sequences')
        #                         ],
        #                     },
        #                 )
        #                 for sample in participant.get('samples')
        #             ],
        #         },
        #     )
        #     for participant in new_data
        # ]

        expected_data_two_samples_filtered = ProjectSummary(
            project=WebProject(
                **{'id': 1, 'name': 'test', 'meta': {}, 'dataset': 'test'}
            ),
            total_samples=2,
            total_samples_in_query=1,
            total_participants=2,
            total_sequences=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequence_stats={'M001': {'genome': '2'}},
            participants=[],  # data_to_class(expected_data_list_filtered),
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequencing_group_keys=[
                ('id', 'Sequencing Group ID'),
                ('created_date', 'Created date'),
            ],
            assay_keys=[
                ('type', 'type'),
                ('technology', 'technology'),
                ('meta.batch', 'batch'),
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

        # self.assertEqual(
        #     expected_data_two_samples_filtered, two_samples_result_filtered
        # )
        for k, v in two_samples_result_filtered.__dict__.items():
            if k == 'participants':
                continue

            self.assertEqual(v, expected_data_two_samples_filtered.__dict__[k])
