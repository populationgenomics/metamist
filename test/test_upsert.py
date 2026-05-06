import pytest

from db.python.connect import Connection
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
                        assays=[],
                    )
                ],
                type='blood',
            )
        ],
    ),
]


class TestUpsert:
    """
    Test upsert functionality in SM
    """

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_insert_participants(self, connection_with_project: Connection):
        """
        Test inserting participants, samples and sequences, and make sure they're correctly linked.

        Tests the other side of:
            tests.test_parse_generic_metadata:TestParseGenericMetadata.test_rows_with_participants
        """

        # Table interfaces
        pt = ParticipantLayer(connection_with_project)

        await pt.upsert_participants(all_participants)

        expected_sample_eid_to_participant_eid = {
            sample_eid: participant_eid
            for participant in all_participants
            for participant_eid in (participant.external_ids or {}).values()
            for sample in participant.samples or []
            for sample_eid in (sample.external_ids or {}).values()
        }

        db_participants = await (
            await connection_with_project.pg_connection.execute(
                'SELECT * FROM participant_external_id ORDER BY participant_id'
            )
        ).fetchall()
        assert len(db_participants) == 3
        assert db_participants[0]['external_id'] == 'Demeter'
        assert db_participants[1]['external_id'] == 'Apollo'
        assert db_participants[2]['external_id'] == 'Athena'

        participant_id_map = {
            p['external_id']: p['participant_id'] for p in db_participants
        }

        db_samples = await (
            await connection_with_project.pg_connection.execute(
                """
            SELECT s.participant_id, seid.external_id
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE seid.name = %(PRIMARY_EXTERNAL_ORG)s
            ORDER BY s.id
            """,
                {'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG},
            )
        ).fetchall()
        assert len(db_samples) == 4
        for db_sample in db_samples:
            assert db_sample['external_id'] is not None
            assert db_sample['participant_id'] is not None
            # get expected_participant_id from the db_sample external_id
            expected_participant_eid = expected_sample_eid_to_participant_eid.get(
                db_sample['external_id']
            )
            assert (
                participant_id_map[expected_participant_eid]
                == db_sample['participant_id']
            )

        db_sequencing_groups = await (
            await connection_with_project.pg_connection.execute(
                'SELECT * FROM sequencing_group'
            )
        ).fetchall()
        assert len(db_sequencing_groups) == 5
        for db_sg in db_sequencing_groups:
            assert db_sg['sample_id'] is not None
            assert db_sg['type'] is not None

        db_assays = await (
            await connection_with_project.pg_connection.execute('SELECT * FROM assay')
        ).fetchall()

        assert len(db_assays) == 4
        for db_a in db_assays:
            assert db_a['sample_id'] is not None
            assert db_a['type'] is not None

        db_participant_no_assays = await (
            await connection_with_project.pg_connection.execute(
                """
            SELECT COUNT(DISTINCT a.id) AS cnt
            FROM sample AS s
            INNER JOIN participant AS p ON p.id = s.participant_id
            INNER JOIN participant_external_id AS pei ON p.id = pei.participant_id
            LEFT JOIN assay AS a ON a.sample_id = s.id
            WHERE pei.external_id = 'Athena'
            """
            )
        ).fetchone()

        assert db_participant_no_assays and db_participant_no_assays['cnt'] == 0

        db_participant_has_assays = await (
            await connection_with_project.pg_connection.execute(
                """
            SELECT COUNT(DISTINCT a.id) AS cnt
            FROM sample AS s
            INNER JOIN participant AS p ON p.id = s.participant_id
            INNER JOIN participant_external_id AS pei ON p.id = pei.participant_id
            LEFT JOIN assay AS a ON a.sample_id = s.id
            WHERE pei.external_id = 'Apollo'
            """
            )
        ).fetchone()

        assert db_participant_has_assays and db_participant_has_assays['cnt'] == 2
