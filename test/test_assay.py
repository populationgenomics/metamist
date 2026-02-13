"""Tests for the Assay layer and table classes."""

from collections import defaultdict

import pytest
from psycopg.errors import UniqueViolation

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.assay import AssayFilter, AssayTable
from db.python.utils import NotFoundError
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
)
from models.models.sequencing_group import SequencingGroupUpsertInternal


DEFAULT_SEQUENCING_META = {
    'sequencing_type': 'genome',
    'sequencing_platform': 'short-read',
    'sequencing_technology': 'illumina',
}


@pytest.fixture
async def sample_id(
    connection_with_project: Connection,
) -> int:
    """
    Create a sample directly in the database for testing assays.
    This is a temporary fixture until sample layer is migrated.
    @TODO replace this when ready
    """
    project_id = connection_with_project.project_id

    conn = connection_with_project.pg_connection
    # Create audit_log entry first
    cur = await conn.execute(
        t"""
        INSERT INTO audit_log (author, auth_project)
        VALUES ('test', {project_id})
        RETURNING id
        """
    )
    row = await cur.fetchone()
    assert row is not None
    audit_log_id = row['id']

    cur = await conn.execute(
        t"""
        INSERT INTO sample (project, type, active, meta, author, audit_log_id)
        VALUES ({project_id}, 'blood', true, '{{"Testing": "test_assay"}}', 'test', {audit_log_id})
        RETURNING id
        """
    )
    row = await cur.fetchone()
    assert row is not None
    sample_id = row['id']

    # Insert the external ID
    await conn.execute(
        t"""
        INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
        VALUES ({project_id}, {sample_id}, 'default', 'TESTING001', {audit_log_id})
        """
    )

    return sample_id


@pytest.fixture
async def metabolomics_assay_type(
    connection_with_project: Connection,
) -> None:
    """
    Create the 'metabolomics' assay type in the database.
    This is a temporary fixture until the enum tables are migrated
    @TODO replace this when ready
    """
    await connection_with_project.pg_connection.execute(
        """
        INSERT INTO assay_type (id, name, audit_log_id)
        VALUES ('metabolomics', 'Metabolomics', 1)
        ON CONFLICT (id) DO NOTHING
        """
    )


@pytest.mark.asyncio
class TestAssay:
    """Test assay class."""

    async def test_not_found_assay(self, connection_with_project: Connection) -> None:
        """Test the NotFoundError when getting an invalid assay ID."""
        with pytest.raises(NotFoundError):
            await AssayLayer(connection_with_project).get_assay_by_id(-1)

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_upsert_assay(
        self,
        connection_with_project: Connection,
        sample_id: int,
    ) -> None:
        """Test inserting an assay, and check all values are inserted correctly."""
        assay_layer = AssayLayer(connection_with_project)
        external_ids = {'default': 'SEQ01', 'collaborator2': 'CBSEQ_1'}
        meta = {
            '1': 1,
            'nested': {'nested': 'dict'},
            'alpha': ['b', 'e', 't'],
            **DEFAULT_SEQUENCING_META,
        }
        upserted_assay = await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta=meta,
                external_ids=external_ids,
            )
        )

        assay = await assay_layer.get_assay_by_id(assay_id=upserted_assay.id)

        assert upserted_assay.id == assay.id
        assert sample_id == int(assay.sample_id)
        assert assay.type == 'sequencing'
        assert external_ids == assay.external_ids
        assert meta == assay.meta

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_insert_assays_for_each_type(
        self, connection_with_project: Connection, sample_id: int
    ) -> None:
        """Test inserting assays, and check all values are inserted correctly."""
        assay_layer = AssayLayer(connection_with_project)
        meta = {
            '1': 1,
            'nested': {'nested': 'dict'},
            'alpha': ['b', 'e', 't'],
            **DEFAULT_SEQUENCING_META,
        }
        sequencing_types = ['sequencing', 'metabolomics']
        assays = await assay_layer.upsert_assays(
            [
                AssayUpsertInternal(
                    external_ids={'eid': f'external_id_{_type}'},
                    sample_id=sample_id,
                    type='sequencing',
                    meta=meta,
                )
                for _type in sequencing_types
            ]
        )
        assay_ids = [a.id for a in assays]

        conn = connection_with_project.pg_connection
        cur = await conn.execute(t'SELECT type FROM assay WHERE id = ANY({assay_ids})')
        rows = await cur.fetchall()

        inserted_types = set(r['type'] for r in rows)

        assert len(sequencing_types) == len(assay_ids)
        assert len(inserted_types) == 1

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_clashing_external_ids(
        self, connection_with_project: Connection, sample_id: int
    ) -> None:
        """Test that should fail when 2nd assay is inserted with same external_id."""
        assay_layer = AssayLayer(connection_with_project)
        external_ids = {'default': 'clashing'}
        await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta={**DEFAULT_SEQUENCING_META},
                external_ids=external_ids,
            )
        )

        async def count_assays() -> int:
            conn = connection_with_project.pg_connection
            cur = await conn.execute('SELECT COUNT(*) as cnt FROM assay')
            row = await cur.fetchone()
            assert row is not None
            return row['cnt']

        assert await count_assays() == 1

        with pytest.raises(UniqueViolation):
            await assay_layer.upsert_assay(
                AssayUpsertInternal(
                    sample_id=sample_id,
                    type='sequencing',
                    meta={**DEFAULT_SEQUENCING_META},
                    external_ids=external_ids,
                )
            )

        # make sure the transaction unwinds the insert second assay if the external_id clashes
        assert await count_assays() == 1

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_insert_clashing_external_ids_multiple(
        self, connection_with_project: Connection, sample_id: int
    ) -> None:
        """Test inserting multiple assays with clashing external IDs fails correctly."""
        assay_layer = AssayLayer(connection_with_project)
        external_ids = {'default': 'clashing'}

        async def count_assays() -> int:
            conn = connection_with_project.pg_connection
            cur = await conn.execute('SELECT COUNT(*) as cnt FROM assay')
            row = await cur.fetchone()
            assert row is not None
            return row['cnt']

        assert await count_assays() == 0

        with pytest.raises(UniqueViolation):
            await assay_layer.upsert_assays(
                [
                    AssayUpsertInternal(
                        # both get the same external_ids
                        external_ids=external_ids,
                        sample_id=sample_id,
                        type='sequencing',
                        meta={**DEFAULT_SEQUENCING_META},
                    )
                    for _ in range(2)
                ]
            )

        assert await count_assays() == 0

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_getting_assay_by_external_id(
        self,
        connection_with_project: Connection,
        sample_id: int,
    ) -> None:
        """Test get different assays by multiple IDs."""
        assay_layer = AssayLayer(connection_with_project)
        project_id = connection_with_project.project_id
        seq1 = await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta={**DEFAULT_SEQUENCING_META},
                external_ids={'default': 'SEQ01', 'other': 'EXT_SEQ1'},
            )
        )
        seq2 = await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta={**DEFAULT_SEQUENCING_META},
                external_ids={'default': 'SEQ02'},
            )
        )

        fquery_1 = AssayFilter(
            external_id=GenericFilter(eq='SEQ01'),
            project=GenericFilter(eq=project_id),
        )
        assert seq1.id == (await assay_layer.query(fquery_1))[0].id
        fquery_2 = AssayFilter(
            external_id=GenericFilter(eq='EXT_SEQ1'),
            project=GenericFilter(eq=project_id),
        )
        assert seq1.id == (await assay_layer.query(fquery_2))[0].id
        fquery_3 = AssayFilter(
            external_id=GenericFilter(eq='SEQ02'),
            project=GenericFilter(eq=project_id),
        )
        assert seq2.id == (await assay_layer.query(fquery_3))[0].id

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_query(self, connection_with_project: Connection) -> None:
        """Test query_assays in different combinations."""
        assay_layer = AssayLayer(connection_with_project)
        project_id = connection_with_project.project_id

        # Create sample directly for this test
        # @TODO remove this once we can create with sample layer
        conn = connection_with_project.pg_connection
        # Create audit_log entry first
        cur = await conn.execute(
            t"""
            INSERT INTO audit_log (author, auth_project)
            VALUES ('test', {project_id})
            RETURNING id
            """
        )
        row = await cur.fetchone()
        assert row is not None
        audit_log_id = row['id']

        cur = await conn.execute(
            t"""
            INSERT INTO sample (project, type, active, meta, author, audit_log_id)
            VALUES ({project_id}, 'blood', true, '{{"collection-year": "2022"}}', 'test', {audit_log_id})
            RETURNING id
            """
        )
        row = await cur.fetchone()
        assert row is not None
        sample_id_for_test = row['id']

        await conn.execute(
            t"""
            INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
            VALUES ({project_id}, {sample_id_for_test}, 'default', 'SAM_TEST_QUERY', {audit_log_id})
            """
        )

        seqs = await assay_layer.upsert_assays(
            [
                AssayUpsertInternal(
                    sample_id=sample_id_for_test,
                    type='sequencing',
                    meta={'unique': 'a', 'common': 'common', **DEFAULT_SEQUENCING_META},
                    external_ids={'default': 'SEQ01'},
                ),
                AssayUpsertInternal(
                    sample_id=sample_id_for_test,
                    type='sequencing',
                    meta={'unique': 'b', 'common': 'common', **DEFAULT_SEQUENCING_META},
                    external_ids={'default': 'SEQ02'},
                ),
            ]
        )

        async def search_result_to_ids(filter_: AssayFilter) -> set[int]:
            filter_.project = GenericFilter(eq=project_id)
            assays = await assay_layer.query(filter_)
            return {s.id for s in assays}

        seq1_id = seqs[0].id
        seq2_id = seqs[1].id

        # sample_ids
        assert {seq1_id, seq2_id} == await search_result_to_ids(
            AssayFilter(sample_id=GenericFilter(in_=[sample_id_for_test]))
        )
        assert set() == await search_result_to_ids(
            AssayFilter(sample_id=GenericFilter(in_=[9_999_999]))
        )

        # external assay IDs
        assert {seq1_id} == await search_result_to_ids(
            AssayFilter(external_id=GenericFilter(eq='SEQ01'))
        )
        assert {seq1_id, seq2_id} == await search_result_to_ids(
            AssayFilter(
                external_id=GenericFilter(in_=['SEQ01', 'SEQ02']),
            )
        )

        # seq_meta
        # @TODO renable once meta filters are fixed
        # assert {seq2_id} == await search_result_to_ids(
        #     AssayFilter(meta={'unique': GenericFilter(eq='b')})
        # )
        # assert {seq1_id, seq2_id} == await search_result_to_ids(
        #     AssayFilter(meta={'common': GenericFilter(eq='common')})
        # )

        # sample meta
        # @TODO renable once meta filters are fixed
        # assert {seq1_id, seq2_id} == await search_result_to_ids(
        #     AssayFilter(sample_meta={'collection-year': GenericFilter(eq='2022')})
        # )
        # assert set() == await search_result_to_ids(
        #     AssayFilter(sample_meta={'unknown_key': GenericFilter(eq='2022')})
        # )

        # assay types
        assert {seq1_id, seq2_id} == await search_result_to_ids(
            AssayFilter(type=GenericFilter(in_=['sequencing']))
        )

        # combination
        # @TODO renable once meta filters are fixed
        # assert {seq2_id} == await search_result_to_ids(
        #     AssayFilter(
        #         sample_meta={'collection-year': GenericFilter(eq='2022')},
        #         external_id=GenericFilter(in_=['SEQ02']),
        #     )
        # )
        assert {seq1_id} == await search_result_to_ids(
            AssayFilter(
                external_id=GenericFilter(in_=['SEQ01']),
                type=GenericFilter(eq='sequencing'),
            )
        )

    @pytest.mark.skip(
        reason='Requires sequencing group layer which is not yet migrated'
    )
    async def test_query_by_sg_ids(
        self,
        connection_with_project: Connection,
    ) -> None:
        """Test query_assays by sequencing group IDs."""
        assay_layer = AssayLayer(connection_with_project)
        slayer = SampleLayer(connection_with_project)

        sample = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'SAM_TEST_QUERY'},
                type='blood',
                active=True,
                meta={'collection-year': '2022'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A1_1'},
                                meta={
                                    'batch': 'batch-1a',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A1_2'},
                                meta={
                                    'batch': 'batch-1b',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'sg2_1'},
                                meta={
                                    'batch': 'batch-2',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'sg2_2'},
                                meta={
                                    'batch': 'batch-2',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                ],
            )
        )

        sg_id = sample.sequencing_groups[0].id
        assay_ids_sg1 = {a.id for a in sample.sequencing_groups[0].assays}

        assays = await assay_layer.get_assays_for_sequencing_group_ids([sg_id])

        assert assay_ids_sg1 == {a.id for sgs_as in assays.values() for a in sgs_as}

        # subfilter
        assays_batch_1a = await assay_layer.get_assays_for_sequencing_group_ids(
            [sg_id],
            filter_=AssayFilter(
                meta={'batch': GenericFilter(eq='batch-1a')},
            ),
        )
        assert len(assays_batch_1a) == 1
        batch_1a_assay = next(iter(assays_batch_1a.values()))[0]
        assert batch_1a_assay.meta['batch'] == 'batch-1a'

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update(
        self,
        connection_with_project: Connection,
        sample_id: int,
    ) -> None:
        """Test updating an assay, and all fields are updated correctly."""
        assay_layer = AssayLayer(connection_with_project)
        # insert
        assay = await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta={'a': 1, 'b': 2, **DEFAULT_SEQUENCING_META},
                external_ids={
                    'default': 'SEQ01',
                    'untouched': 'UTC+1',
                    'to_delete': 'VALUE',
                },
            )
        )

        await assay_layer.upsert_assay(
            AssayUpsertInternal(
                id=assay.id,
                sample_id=sample_id,
                external_ids={
                    'default': 'NSQ_01',
                    'ext': 'EXTSEQ01',
                    'to_delete': None,
                },
                meta={'a': 2, 'c': True},
            )
        )

        update_assay = await assay_layer.get_assay_by_id(assay_id=assay.id)

        assert assay.id == update_assay.id
        assert sample_id == int(update_assay.sample_id)
        assert update_assay.type == 'sequencing'
        assert update_assay.external_ids == {
            'default': 'NSQ_01',
            'ext': 'EXTSEQ01',
            'untouched': 'UTC+1',
        }
        assert update_assay.meta == {
            'a': 2,
            'b': 2,
            'c': True,
            **DEFAULT_SEQUENCING_META,
        }

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update_type(
        self,
        connection_with_project: Connection,
        sample_id: int,
        metabolomics_assay_type: None,  # noqa: ARG002
    ) -> None:
        """Test update all assay statuses."""
        assay_layer = AssayLayer(connection_with_project)
        assay = await assay_layer.upsert_assay(
            AssayUpsertInternal(
                sample_id=sample_id,
                type='sequencing',
                meta={**DEFAULT_SEQUENCING_META},
                external_ids={},
            )
        )

        # cycle through all statuses, and check that works
        await assay_layer.upsert_assay(
            AssayUpsertInternal(id=assay.id, type='metabolomics')
        )

        cur = await connection_with_project.pg_connection.execute(
            t'SELECT type FROM assay WHERE id = {assay.id}'
        )
        row = await cur.fetchone()

        assert row is not None
        assert row['type'] == 'metabolomics'

    @pytest.mark.skip(
        reason='Requires sequencing group layer which is not yet migrated'
    )
    async def test_batch_statistics(
        self,
        connection_with_project: Connection,
    ) -> None:
        """Test batch statistics."""
        samples_to_insert = [
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'SAMPLE_1'},
                type='blood',
                active=True,
                meta={'collection-year': '2022'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG_1'},
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A1_1'},
                                meta={
                                    'batch': 'batch-1',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A1_2'},
                                meta={
                                    'batch': 'batch-1',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG_2'},
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A2_1'},
                                meta={
                                    'batch': 'batch-1',
                                    'sequencing_type': 'exome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A2_2'},
                                meta={
                                    'batch': 'batch-2',
                                    'sequencing_type': 'exome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                ],
            ),
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'SAMPLE_2'},
                type='blood',
                active=True,
                meta={'collection-year': '2022'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG_3'},
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A3_1'},
                                meta={
                                    'batch': 'batch-1',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A3_2'},
                                meta={
                                    'batch': 'batch-1',
                                    'sequencing_type': 'genome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        external_ids={'default': 'SG_4'},
                        type='transcriptome',
                        technology='short-read',
                        platform='illumina',
                        meta={'sgmeta': 'sgvalue'},
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A4_1'},
                                meta={
                                    'batch': 'batch-3',
                                    'sequencing_type': 'transcriptome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                            AssayUpsertInternal(
                                type='sequencing',
                                external_ids={'default': 'A4_2'},
                                meta={
                                    'batch': 'batch-3',
                                    'sequencing_type': 'transcriptome',
                                    'sequencing_platform': 'illumina',
                                    'sequencing_technology': 'short-read',
                                },
                            ),
                        ],
                    ),
                ],
            ),
        ]

        await SampleLayer(connection_with_project).upsert_samples(samples_to_insert)
        assay_table = AssayTable(connection_with_project)
        rows = await assay_table.get_assay_type_numbers_by_batch_for_project(
            connection_with_project.project_id
        )

        assays_in_batch: dict[str, int] = defaultdict(int)
        sgs_in_seq_type_batch: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        for r in rows:
            # sequencing_group_ids has format [(sg_id, assay_count), ...]
            assays_in_batch[r.batch] += sum(sg[1] for sg in r.sequencing_group_ids)
            sgs_in_seq_type_batch[r.batch][r.sequencing_type] += len(
                r.sequencing_group_ids
            )

        # aggregate to test, mostly to show there are multiple ways you
        # may want to aggregate this information
        assert assays_in_batch == {
            'batch-1': 5,
            'batch-2': 1,
            'batch-3': 2,
        }

        assert sgs_in_seq_type_batch == {
            'batch-1': {'genome': 2, 'exome': 1},
            'batch-2': {'exome': 1},
            'batch-3': {'transcriptome': 1},
        }
