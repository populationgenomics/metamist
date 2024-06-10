from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.participant import ParticipantLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)

default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}

all_participants = [
    ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
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
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
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
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                ],
                type='blood',
            )
        ],
    ),
    ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Apollo'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id002'},
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
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
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                ],
                type='blood',
            ),
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id004'},
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
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
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    )
                ],
                type='blood',
            ),
        ],
    ),
    ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Athena'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id003'},
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
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
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    )
                ],
                type='blood',
            )
        ],
    ),
]


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

        # Table interfaces
        pt = ParticipantLayer(self.connection)

        await pt.upsert_participants(all_participants, open_transaction=False)

        expected_sample_eid_to_participant_eid = {
            sample_eid: participant_eid
            for participant in all_participants
            for participant_eid in participant.external_ids.values()
            for sample in participant.samples
            for sample_eid in sample.external_ids.values()
        }

        db_participants = await self.connection.connection.fetch_all(
            'SELECT * FROM participant_external_id ORDER BY participant_id'
        )
        self.assertEqual(3, len(db_participants))
        self.assertEqual('Demeter', db_participants[0]['external_id'])
        self.assertEqual('Apollo', db_participants[1]['external_id'])
        self.assertEqual('Athena', db_participants[2]['external_id'])

        participant_id_map = {p['external_id']: p['participant_id'] for p in db_participants}

        db_samples = await self.connection.connection.fetch_all(
            """
            SELECT s.participant_id, seid.external_id
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE seid.name = :PRIMARY_EXTERNAL_ORG
            ORDER BY s.id
            """,
            {'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG},
        )
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

        db_sequencing_groups = await self.connection.connection.fetch_all(
            'SELECT * FROM sequencing_group'
        )
        self.assertEqual(5, len(db_sequencing_groups))
        for db_sg in db_sequencing_groups:
            self.assertIsNotNone(db_sg['sample_id'])
            self.assertIsNotNone(db_sg['type'])

        db_sequencing = await self.connection.connection.fetch_all(
            'SELECT * FROM assay'
        )
        self.assertEqual(5, len(db_sequencing))
        for db_sg in db_sequencing_groups:
            self.assertIsNotNone(db_sg['sample_id'])
            # self.assertIsNotNone(db_sg['type'])
