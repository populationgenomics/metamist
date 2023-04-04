from datetime import date

from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.web import (
    WebLayer,
    ProjectSummary,
    NestedParticipant,
    NestedSample,
    NestedSequence,
    WebProject,
)
from db.python.layers.participant import (
    ParticipantLayer,
    ParticipantUpsertInternal,
)
from db.python.layers.sample import SampleLayer, SampleUpsertInternal
from db.python.layers.assay import AssayLayer, AssayUpsertInternal
from db.python.layers.sequencing_group import SequencingGroupUpsertInternal

from models.enums import SampleType


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
    async def test_project_summary(self):
        """Test getting the summary for a project"""
        result = await self.webl.get_project_summary(token=None)

        # Expect an empty project
        expected = ProjectSummary(
            project=WebProject(
                **{'id': 1, 'name': 'test', 'meta': {}, 'dataset': 'test'}
            ),
            total_samples=0,
            total_participants=0,
            total_sequences=0,
            batch_sequence_stats={},
            cram_seqr_stats={},
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequence_keys=[],
            seqr_links={},
        )

        self.assertEqual(expected, result)

        # Now add a participant with a sample and sequence
        data = [
            ParticipantUpsertInternal(
                external_id='Demeter',
                meta={},
                samples=[
                    SampleUpsertInternal(
                        external_id='sample_id001',
                        meta={},
                        type=SampleType.BLOOD,
                        sequencing_groups=[
                            SequencingGroupUpsertInternal(
                                type='genome',
                                technology='short-read',
                                platform=None,
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
                                        },
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            )
        ]

        await self.partl.upsert_participants(participants=data)

        result = await self.webl.get_project_summary(token=None)

        expected = ProjectSummary(
            project=WebProject(
                **{'id': 1, 'name': 'test', 'meta': {}, 'dataset': 'test'}
            ),
            total_samples=1,
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
            sequence_keys=[
                ('type', 'type'),
                ('technology', 'technology'),
                ('meta.batch', 'batch'),
                ('meta.reads_type', 'reads_type'),
            ],
            seqr_links={},
        )
        self.assertEqual(True)
        # TODO: fix this test
        self.assertEqual(expected, result)
