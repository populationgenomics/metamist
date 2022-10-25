from datetime import date

from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.web import (
    WebLayer,
    ProjectSummary,
    NestedParticipant,
    NestedSample,
    NestedSequence,
)
from db.python.layers.participant import (
    ParticipantLayer,
    ParticipantUpsert,
    ParticipantUpsertBody,
)
from db.python.layers.sample import SampleBatchUpsert, SampleLayer
from db.python.layers.sequence import SampleSequenceLayer, SequenceUpsert

from models.enums.sample import SampleType
from models.enums.sequencing import SequenceStatus, SequenceType


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
        self.seql = SampleSequenceLayer(self.connection)

    @run_as_sync
    async def test_project_summary(self):
        """Test getting the summary for a project"""
        result = await self.webl.get_project_summary(token=None)

        # Expect an empty project
        expected = ProjectSummary(
            total_samples=0,
            total_participants=0,
            sequence_stats={},
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequence_keys=[],
        )

        self.assertEqual(expected, result)

        # Now add a participant with a sample and sequence
        data = [
            {
                '_class': ParticipantUpsert,
                'external_id': 'Demeter',
                'meta': {},
                'samples': [
                    {
                        '_class': SampleBatchUpsert,
                        'external_id': 'sample_id001',
                        'meta': {},
                        'type': SampleType.BLOOD,
                        'sequences': [
                            {
                                '_class': SequenceUpsert,
                                'type': SequenceType.GENOME,
                                'status': SequenceStatus.UPLOADED,
                                'meta': {
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
                                },
                            },
                        ],
                    }
                ],
            },
        ]

        body = ParticipantUpsertBody(participants=data_to_class(data))
        await self.partl.batch_upsert_participants(participants=body)

        # Switch to response classes
        expected_data: list = [
            merge(
                participant,
                {
                    '_class': NestedParticipant,
                    'id': 1,
                    'families': [],
                    'samples': [
                        merge(
                            sample,
                            {
                                '_class': NestedSample,
                                'id': 1,
                                'created_date': str(date.today()),
                                'sequences': [
                                    merge(
                                        sequence,
                                        {
                                            '_class': NestedSequence,
                                            'id': 1,
                                        },
                                    )
                                    for sequence in sample.get('sequences')
                                ],
                            },
                        )
                        for sample in participant.get('samples')
                    ],
                },
            )
            for participant in data
        ]

        result = await self.webl.get_project_summary(token=None)

        expected = ProjectSummary(
            total_samples=1,
            total_participants=1,
            sequence_stats={
                'genome': {
                    'Sequences': '1',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            participants=data_to_class(expected_data),
            participant_keys=[('external_id', 'Participant ID')],
            sample_keys=[
                ('id', 'Sample ID'),
                ('external_id', 'External Sample ID'),
                ('created_date', 'Created date'),
            ],
            sequence_keys=[('type', 'type'), ('meta.reads_type', 'reads_type')],
        )

        self.assertEqual(expected, result)
