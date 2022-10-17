from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.participant import (
    ParticipantLayer,
    ParticipantUpsert,
    ParticipantUpsertBody,
)
from db.python.layers.sample import SampleUpsert, SequenceUpsert
from models.enums import SequenceType, SampleType, SequenceStatus


class TestUpsert(DbIsolatedTest):
    """
    Test upsert functionality in SM
    """

    @run_as_sync
    async def test_insert_participants(self):
        """
        Test inserting participants, samples and sequences, and make sure they're correctly linked.

        Tests the other side of:
            tests.test_parse_generic_metadata:TestParseGenericMetadata.test_rows_with_participants
        """
        all_participants = [
            ParticipantUpsert.construct(
                **{
                    'external_id': 'Demeter',
                    'meta': {},
                    'samples': [
                        SampleUpsert.construct(
                            **{
                                'external_id': 'sample_id001',
                                'meta': {},
                                'sequences': [
                                    SequenceUpsert.construct(
                                        **{
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
                                            'status': SequenceStatus('uploaded'),
                                            'type': SequenceType('genome'),
                                        }
                                    ),
                                    SequenceUpsert.construct(
                                        **{
                                            'meta': {
                                                'reads': [
                                                    [
                                                        {
                                                            'basename': 'sample_id001.exome.filename-R1.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id001.exome.filename-R1.fastq.gz',
                                                            'size': 111,
                                                        },
                                                        {
                                                            'basename': 'sample_id001.exome.filename-R2.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id001.exome.filename-R2.fastq.gz',
                                                            'size': 111,
                                                        },
                                                    ]
                                                ],
                                                'reads_type': 'fastq',
                                            },
                                            'status': SequenceStatus('uploaded'),
                                            'type': SequenceType('exome'),
                                        }
                                    ),
                                ],
                                'type': SampleType('blood'),
                            }
                        )
                    ],
                }
            ),
            ParticipantUpsert.construct(
                **{
                    'external_id': 'Apollo',
                    'meta': {},
                    'samples': [
                        SampleUpsert.construct(
                            **{
                                'external_id': 'sample_id002',
                                'meta': {},
                                'sequences': [
                                    SequenceUpsert.construct(
                                        **{
                                            'meta': {
                                                'reads': [
                                                    [
                                                        {
                                                            'basename': 'sample_id002.filename-R1.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id002.filename-R1.fastq.gz',
                                                            'size': 111,
                                                        },
                                                        {
                                                            'basename': 'sample_id002.filename-R2.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id002.filename-R2.fastq.gz',
                                                            'size': 111,
                                                        },
                                                    ]
                                                ],
                                                'reads_type': 'fastq',
                                            },
                                            'status': SequenceStatus('uploaded'),
                                            'type': SequenceType('genome'),
                                        }
                                    )
                                ],
                                'type': SampleType('blood'),
                            }
                        ),
                        SampleUpsert.construct(
                            **{
                                'external_id': 'sample_id004',
                                'meta': {},
                                'sequences': [
                                    SequenceUpsert.construct(
                                        **{
                                            'meta': {
                                                'reads': [
                                                    [
                                                        {
                                                            'basename': 'sample_id004.filename-R1.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id004.filename-R1.fastq.gz',
                                                            'size': 111,
                                                        },
                                                        {
                                                            'basename': 'sample_id004.filename-R2.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id004.filename-R2.fastq.gz',
                                                            'size': 111,
                                                        },
                                                    ]
                                                ],
                                                'reads_type': 'fastq',
                                            },
                                            'status': SequenceStatus('uploaded'),
                                            'type': SequenceType('genome'),
                                        }
                                    )
                                ],
                                'type': SampleType('blood'),
                            }
                        ),
                    ],
                }
            ),
            ParticipantUpsert.construct(
                **{
                    'external_id': 'Athena',
                    'meta': {},
                    'samples': [
                        SampleUpsert.construct(
                            **{
                                'external_id': 'sample_id003',
                                'meta': {},
                                'sequences': [
                                    SequenceUpsert.construct(
                                        **{
                                            'meta': {
                                                'reads': [
                                                    [
                                                        {
                                                            'basename': 'sample_id003.filename-R1.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id003.filename-R1.fastq.gz',
                                                            'size': 111,
                                                        },
                                                        {
                                                            'basename': 'sample_id003.filename-R2.fastq.gz',
                                                            'checksum': None,
                                                            'class': 'File',
                                                            'location': '/path/to/sample_id003.filename-R2.fastq.gz',
                                                            'size': 111,
                                                        },
                                                    ]
                                                ],
                                                'reads_type': 'fastq',
                                            },
                                            'status': SequenceStatus('uploaded'),
                                            'type': SequenceType('genome'),
                                        }
                                    )
                                ],
                                'type': SampleType('blood'),
                            }
                        )
                    ],
                }
            ),
        ]

        body = ParticipantUpsertBody.construct(participants=all_participants)
        # Table interfaces
        pt = ParticipantLayer(self.connection)

        await pt.batch_upsert_participants(body)

        expected_sample_eid_to_participant_eid = {
            sample.external_id: participant.external_id
            for participant in all_participants
            for sample in participant.samples
        }

        db_participants = await self.connection.connection.fetch_all(
            'SELECT * FROM participant'
        )
        self.assertEqual(3, len(db_participants))
        self.assertEqual('Demeter', db_participants[0]['external_id'])
        self.assertEqual('Apollo', db_participants[1]['external_id'])
        self.assertEqual('Athena', db_participants[2]['external_id'])

        participant_id_map = {p['external_id']: p['id'] for p in db_participants}

        db_samples = await self.connection.connection.fetch_all('SELECT * FROM sample')
        self.assertEqual(4, len(db_samples))
        for db_sample in db_samples:
            self.assertIsNotNone(db_sample['external_id'])
            self.assertIsNotNone(db_sample['participant_id'])
            # get expected_participant_id from the db_sample external_id
            expected_participant_eid = expected_sample_eid_to_participant_eid.get(
                db_sample['external_id']
            )
            self.assertEqual(
                participant_id_map[expected_participant_eid],
                db_sample['participant_id'],
            )
