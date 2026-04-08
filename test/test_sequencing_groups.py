from collections.abc import Callable
from datetime import date
from unittest import mock

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import SampleLayer, SequencingGroupLayer
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.conftest import GraphQLQueryFunction


DEFAULT_SEQUENCING_META = {
    'sequencing_type': 'genome',
    'sequencing_platform': 'short-read',
    'sequencing_technology': 'illumina',
}


@pytest.fixture
async def test_sample(connection_with_project: Connection) -> int:
    """
    Create a sample in the database that test sequencing groups can be attached to.
    """
    project_id = connection_with_project.project_id

    sample = SampleUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'EX_ID'},
        meta={'meta_key': 'meta_value'},
        type='blood',
        active=True,
    )

    await SampleLayer(connection_with_project).upsert_sample(sample, project=project_id)

    assert sample.id is not None
    return sample.id


@pytest.fixture
def sequencing_group_model(test_sample: int) -> SequencingGroupUpsertInternal:
    """
    Fixture that provides a SequencingGroupUpsertInternal for
    a simple sequencing group to be upserted into the database.
    """
    return SequencingGroupUpsertInternal(
        type='genome',
        technology='short-read',
        platform='ILLUMINA',
        meta={
            'meta-key': 'meta-value',
        },
        sample_id=test_sample,
        external_ids={'ext': 'some-ext-id'},
        assays=[
            AssayUpsertInternal(
                type='sequencing',
                external_ids={},
                meta={
                    'sequencing_type': 'genome',
                    'sequencing_platform': 'short-read',
                    'sequencing_technology': 'illumina',
                },
            )
        ],
    )


@pytest.fixture
def mock_date(monkeypatch) -> Callable[[str, date], mock.Mock]:
    """Fixture to configurably mock calls to `date.today` within the provided module"""

    def _mock_date(module: str, _date: date):
        mock_date = mock.Mock(wraps=date)
        mock_date.today.return_value = _date
        monkeypatch.setattr(module, mock_date)
        return mock_date

    return _mock_date


class TestSequencingGroup:
    """Test sequencing groups business logic"""

    @pytest.mark.asyncio
    async def test_empty_query(
        self,
        connection: Connection,
    ) -> None:
        """
        Test empty IDs to see the query construction
        """
        layer = SequencingGroupLayer(connection)
        sgs = await layer.query(SequencingGroupFilter(id=GenericFilter(in_=[])))

        assert len(sgs) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_insert_sequencing_group(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
    ):
        """Test inserting and fetching a sequencing group"""
        sg_layer = SequencingGroupLayer(connection_with_project)

        sg_upsert = await sg_layer.upsert_sequencing_groups([sequencing_group_model])
        assert sg_upsert[0].id is not None
        sg_id = sg_upsert[0].id
        sg = await sg_layer.get_sequencing_group_by_id(sg_id)
        assert sg.platform is not None

        inserted_sg = sg_upsert[0]
        assert inserted_sg.id == sg_id
        assert inserted_sg.type == sg.type
        assert inserted_sg.technology == sg.technology
        assert inserted_sg.platform is not None
        assert inserted_sg.platform.lower() == sg.platform.lower()
        assert inserted_sg.meta == sg.meta

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_sequencing_group(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
    ):
        """Test updating metadata on a sequencing group"""
        sg_layer = SequencingGroupLayer(connection_with_project)
        # Create the initial SG
        initial_sg = await sg_layer.upsert_sequencing_groups([sequencing_group_model])
        assert initial_sg[0].id

        # Create an updated model for upsert
        upsert_sg_model = SequencingGroupUpsertInternal(
            id=initial_sg[0].id,
            meta={'another-meta': 'field'},
        )

        # Check that id is being returned when sg created
        new_sg = await sg_layer.upsert_sequencing_groups([upsert_sg_model])
        assert new_sg[0].id == upsert_sg_model.id

        # Check that the update was mdae to the db
        sg_from_db = await sg_layer.get_sequencing_group_by_id(initial_sg[0].id)

        assert sg_from_db.meta == {'another-meta': 'field', 'meta-key': 'meta-value'}

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_auto_deprecation_of_old_sequencing_group(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
    ):
        """Test creating a sequencing-group, and test the old one is archived"""
        sample_id = sequencing_group_model.sample_id
        sg_layer = SequencingGroupLayer(connection_with_project)
        # Create the initial SG
        initial_sg = await sg_layer.upsert_sequencing_groups([sequencing_group_model])
        assert initial_sg[0].id is not None
        assert initial_sg[0].assays is not None

        new_upsert = SequencingGroupUpsertInternal(
            sample_id=initial_sg[0].sample_id,
            type='genome',
            technology='short-read',
            platform='ILLUMINA',
            meta={
                'meta-key': 'meta-value',
            },
            external_ids={},
            assays=[
                # include an empty assay with ID to ensure it gets added to the sg
                AssayUpsertInternal(
                    id=initial_sg[0].assays[0].id,
                ),
                # new assay to trigger deprecation
                AssayUpsertInternal(
                    type='sequencing',
                    external_ids={'second-key': 'second-sequencing-object'},
                    meta={
                        'sequencing_type': 'genome',
                        'sequencing_platform': 'short-read',
                        'sequencing_technology': 'illumina',
                    },
                ),
            ],
        )

        updated_sg = await sg_layer.upsert_sequencing_groups([new_upsert])

        old_sg = await sg_layer.get_sequencing_group_by_id(initial_sg[0].id)
        # now check the existing sequencing group was archived
        assert old_sg.archived

        # check that the "active" sequencing group is the new one
        active_sgs = await sg_layer.query(
            SequencingGroupFilter(
                sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                    id=GenericFilter(eq=sample_id)
                )
            )
        )

        assert all(not sg.archived for sg in active_sgs)
        assert len(active_sgs) == 1
        assert updated_sg[0].id == active_sgs[0].id

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_query_with_assay_metadata(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
    ):
        """Test searching with an assay metadata filter"""
        sg_layer = SequencingGroupLayer(connection_with_project)
        sgs_to_insert = [sequencing_group_model]

        # Add extra sequencing group
        sgs_to_insert.append(
            SequencingGroupUpsertInternal(
                sample_id=sequencing_group_model.sample_id,
                type='exome',
                technology='short-read',
                platform='ILLUMINA',
                meta={
                    'meta-key': 'meta-value',
                },
                external_ids={},
                assays=[
                    AssayUpsertInternal(
                        type='sequencing',
                        external_ids={},
                        meta={
                            'sequencing_type': 'exome',
                            'sequencing_platform': 'short-read',
                            'sequencing_technology': 'illumina',
                        },
                    )
                ],
            )
        )

        # Create in database
        inital_sgs = await sg_layer.upsert_sequencing_groups(sgs_to_insert)

        # Query for genome assay metadata
        sgs = await sg_layer.query(
            SequencingGroupFilter(
                assay=SequencingGroupFilter.SequencingGroupAssayFilter(
                    meta={'sequencing_type': GenericFilter(eq='genome')}
                )
            )
        )
        assert len(sgs) == 1
        assert sgs[0].id == inital_sgs[0].id

        # Query for exome assay metadata
        sgs = await sg_layer.query(
            SequencingGroupFilter(
                assay=SequencingGroupFilter.SequencingGroupAssayFilter(
                    meta={'sequencing_type': GenericFilter(eq='exome')}
                )
            )
        )
        assert len(sgs) == 1
        assert sgs[0].id == inital_sgs[1].id

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_query_with_empty_filters(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
    ):
        assert sequencing_group_model.assays is not None

        sample_layer = SampleLayer(connection_with_project)
        sg_layer = SequencingGroupLayer(connection_with_project)

        # Create a second sequencing group with different sample and assays for testing
        new_sample = SampleUpsertInternal(
            project=connection_with_project.project_id,
            type='saliva',
            active=True,
            external_ids={PRIMARY_EXTERNAL_ORG: 'EX_ID_2'},
            meta={'meta_key': 'meta_value'},
        )
        await sample_layer.upsert_sample(new_sample)

        new_sg = SequencingGroupUpsertInternal(
            type='exome',
            technology='long-read',
            platform='ILLUMINA',
            meta={
                'meta-key': 'meta-value',
            },
            sample_id=new_sample.id,
            external_ids={'ext': 'some-ext-id-2'},
            assays=[
                AssayUpsertInternal(
                    type='sequencing',
                    external_ids={},
                    meta={
                        'sequencing_type': 'exome',
                        'sequencing_platform': 'long-read',
                        'sequencing_technology': 'illumina',
                    },
                )
            ],
        )
        assert new_sg.assays is not None

        # Add both the fixture sg model and the new model to the db
        await sg_layer.upsert_sequencing_groups([sequencing_group_model, new_sg])

        # Assert that the sequencing groups and their assays are distinct
        assert len(sequencing_group_model.assays) == 1
        assert len(new_sg.assays) == 1
        assert sequencing_group_model.assays[0] != new_sg.assays[0]
        assert sequencing_group_model.sample_id != new_sg.sample_id

        # Test that empty sample and assay filters don't cause a narrowing-down of the results
        empty_filter = SequencingGroupFilter(
            sample=SequencingGroupFilter.SequencingGroupSampleFilter(),
            assay=SequencingGroupFilter.SequencingGroupAssayFilter(),
        )
        result = await sg_layer.query(empty_filter)
        result_ids = [r.id for r in result]

        assert len(result) == 2
        assert sequencing_group_model.id in result_ids
        assert new_sg.id in result_ids

        # Test that an assay filter does narrow down the result
        assay_filter = SequencingGroupFilter(
            assay=SequencingGroupFilter.SequencingGroupAssayFilter(
                id=GenericFilter(eq=new_sg.assays[0].id)
            )
        )
        result = await sg_layer.query(assay_filter)

        assert len(result) == 1
        assert result[0].id == new_sg.id

        # Test that a sample filter does narrow down the result
        sample_filter = SequencingGroupFilter(
            sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                id=GenericFilter(eq=new_sg.sample_id)
            )
        )
        result = await sg_layer.query(sample_filter)

        assert len(result) == 1
        assert result[0].id == new_sg.id

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.db_superuser
    async def test_query_with_creation_date(
        self,
        connection_with_project: Connection,
        test_sample: int,
    ):
        """Test fetching using a creation date filter"""
        sg_layer = SequencingGroupLayer(connection_with_project)

        # Insert a current sequencing group into the database with a pre-defined date that it is current from
        current_date = date(2026, 1, 1)
        current_query = t"""
            INSERT INTO sequencing_group
                (sample_id, type, technology, archived, sys_period)
            VALUES
                ({test_sample}, 'genome', 'short-read', false, tstzrange({current_date.isoformat()}, null))
        """
        conn = connection_with_project.pg_connection
        async with conn.transaction():
            # Disable the trigger so that the sys_period isn't overwritten
            await conn.execute(
                'ALTER TABLE main.sequencing_group DISABLE TRIGGER versioning_trigger'
            )
            await conn.execute(current_query)
            await conn.execute(
                'ALTER TABLE main.sequencing_group ENABLE TRIGGER versioning_trigger'
            )

        # Query for sequencing group with creation date current_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(eq=current_date))
        )
        assert len(sgs) == 1
        sg_id = sgs[0].id

        # Query for sequencing group with creation date before current_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(lt=current_date))
        )
        assert len(sgs) == 0

        # Query for sequencing group with creation date after current_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(gt=current_date))
        )
        assert len(sgs) == 0

        # Insert a historical sequencing group into the history table with a pre-defined period for which it was current
        historical_date = date(2025, 12, 1)
        historical_query = t"""
            INSERT INTO sequencing_group_history
                (id, sample_id, type, technology, archived, sys_period)
            VALUES
                (
                    {sg_id}, {test_sample}, 'genome', 'short-read', false,
                    tstzrange({historical_date.isoformat()}, {current_date.isoformat()})
                )
        """
        await conn.execute(historical_query)

        # Query for sequencing group with creation date historical_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(eq=historical_date))
        )
        assert len(sgs) == 1
        sg_id = sgs[0].id

        # Query for sequencing group with creation date before historical_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(lt=historical_date))
        )
        assert len(sgs) == 0

        # Query for sequencing group with creation date after historical_date
        sgs = await sg_layer.query(
            SequencingGroupFilter(created_on=GenericFilter(gt=historical_date))
        )
        assert len(sgs) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_query_finds_sgs_which_have_cram_analysis(
        self, connection_with_project: Connection, test_sample: int
    ):
        """Test querying for sequencing groups which have a cram or gvcf analysis"""
        sg_layer = SequencingGroupLayer(connection_with_project)

        test_sg_data = [
            {'type': 'genome', 'technology': 'short-read'},
            {'type': 'genome', 'technology': 'long-read'},
        ]

        # Firstly create two sequencing groups to attach analyses to
        insert_sgs = f"""
            INSERT INTO sequencing_group (sample_id, type, technology, archived)
            VALUES ({test_sample}, %(type)s, %(technology)s, false)
            RETURNING id"""

        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.executemany(insert_sgs, test_sg_data, returning=True)
            sg_ids = [(await cur.fetchone())['id'] async for _ in cur.results()]

        assert len(sg_ids) == 2

        # Create a cram and gvcf analysis
        insert_analyses = f"""
            INSERT INTO analysis (type, project, status)
            VALUES (%(type)s, {connection_with_project.project_id}, 'completed')
            RETURNING id"""

        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.execute(insert_analyses, {'type': 'cram'})
            cram_id = (await cur.fetchone())['id']
            await cur.execute(insert_analyses, {'type': 'gvcf'})
            gvcf_id = (await cur.fetchone())['id']

        # Attach the cram analysis to the first sg, gvcf analysis to the second sg
        insert_analysis_sequencing_group = """
            INSERT INTO analysis_sequencing_group (analysis_id, sequencing_group_id)
            VALUES (%(analysis_id)s, %(sg_id)s)"""

        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.execute(
                insert_analysis_sequencing_group,
                {'analysis_id': cram_id, 'sg_id': sg_ids[0]},
            )
            await cur.execute(
                insert_analysis_sequencing_group,
                {'analysis_id': gvcf_id, 'sg_id': sg_ids[1]},
            )

        # Query for cram analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_cram=True))
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[0]

        # Query for gvcf analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_gvcf=True))
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[1]

        # Query for both cram AND gvcf analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_gvcf=True, has_cram=True))
        assert len(sgs) == 0

        # Add first SG to gvcf analysis
        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.execute(
                insert_analysis_sequencing_group,
                {'analysis_id': gvcf_id, 'sg_id': sg_ids[0]},
            )

        # Query for both cram AND gvcf analysis now that first SG has gvcf analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_gvcf=True, has_cram=True))
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[0]

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_archiving_sequencing_groups(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
        graphql_query: GraphQLQueryFunction,
    ):
        """Check that sequencing groups can be archived from graphql"""
        sg_layer = SequencingGroupLayer(connection_with_project)
        sgs_to_insert = [sequencing_group_model]

        sgs_to_insert.append(
            SequencingGroupUpsertInternal(
                sample_id=sequencing_group_model.sample_id,
                type='genome',
                technology='long-read',
                platform='illumina',
                meta={'meta-1': 'test-1'},
                assays=[],
            )
        )
        sgs_to_insert.append(
            SequencingGroupUpsertInternal(
                sample_id=sequencing_group_model.sample_id,
                type='exome',
                technology='short-read',
                platform='illumina',
                meta={'meta-2': 'test-2'},
                assays=[],
            )
        )
        sgs_to_insert.append(
            SequencingGroupUpsertInternal(
                sample_id=sequencing_group_model.sample_id,
                type='exome',
                technology='long-read',
                platform='illumina',
                meta={'meta-3': 'test-3'},
                assays=[],
            )
        )

        sgs = await sg_layer.upsert_sequencing_groups(sgs_to_insert)
        assert sgs
        sg1 = sgs[0].to_external().id
        sg2 = sgs[1].to_external().id

        assert sgs[0].id is not None
        assert sgs[1].id is not None
        assert sg1 and sg2

        # Check that the sequencing groups aren't initially archived
        sgs_from_db = await sg_layer.get_sequencing_groups_by_ids(
            [sgs[0].id, sgs[1].id]
        )
        assert not sgs_from_db[0].archived
        assert not sgs_from_db[1].archived

        # Archive the sequencing groups
        archive_result = await graphql_query(
            """
            mutation ArchiveSeqGroups($ids: [String!]!) {
                sequencingGroup {
                    archiveSequencingGroups(sequencingGroupIds:$ids) {
                        id
                        archived
                    }
                }
            }
            """,
            {'ids': [sg1, sg2]},
        )

        archived_sgs = archive_result['data']['sequencingGroup'][
            'archiveSequencingGroups'
        ]

        assert len(archived_sgs) == 2
        assert archived_sgs[0]['id'] == sg1
        assert archived_sgs[0]['archived']
        assert archived_sgs[1]['id'] == sg2
        assert archived_sgs[1]['archived']

    @pytest.mark.asyncio
    async def test_history_no_sum(self, connection: Connection):
        """Test the trivial case where there are no sequencing groups."""
        sg_table = SequencingGroupTable(connection)
        result = await sg_table.get_sequencing_group_counts_by_month([])

        assert result == {}

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_history_sum_multiple_projects(
        self, connection_with_project: Connection, test_sample: int, mock_date
    ):
        """Test the case where type:technology combinations are summed and held for the same project."""
        # Mock today's date.
        today = date(year=2025, month=12, day=31)
        mock_date('db.python.tables.sequencing_group.date', today)

        # Create a second project and sample to attach sequencing groups to
        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.execute("""
                INSERT INTO project (name, dataset, meta)
                VALUES ('test-project-2', 'test-dataset-2', '{}')
                RETURNING id
            """)
            row = await cur.fetchone()
            assert row is not None
            project_2: int = row['id']

            await cur.execute(
                t"""
                INSERT INTO sample (project, type, active)
                VALUES ({project_2}, 'blood', true)
                RETURNING id
                """
            )
            row = await cur.fetchone()
            assert row is not None
            test_sample_2 = row['id']

        # Add some test data to the database
        test_data = [
            {
                'id': 1,
                'sample_id': test_sample,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1).isoformat(),
            },
            {
                'id': 2,
                'sample_id': test_sample,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 11, 1).isoformat(),
            },
            {
                'id': 3,
                'sample_id': test_sample_2,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1).isoformat(),
            },
            {
                'id': 4,
                'sample_id': test_sample_2,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 11, 1).isoformat(),
            },
        ]

        test_data_query = f"""
            INSERT INTO sequencing_group_history
                (id, sample_id, type, technology, archived, sys_period)
            VALUES
                (%(id)s, %(sample_id)s, %(type)s, %(technology)s, false, tstzrange(%(sg_date)s, '{today.isoformat()}'))"""

        # Insert the test data to the DB
        conn = connection_with_project.pg_connection
        async with conn.transaction(), conn.cursor() as cur:
            await cur.executemany(test_data_query, test_data)

        assert connection_with_project.project_id is not None
        sg_table = SequencingGroupTable(connection_with_project)
        result = await sg_table.get_sequencing_group_counts_by_month(
            [connection_with_project.project_id, project_2]
        )

        assert result == {
            connection_with_project.project_id: {
                date(2025, 10, 1): {
                    'genome|||short-read': 1,
                },
                date(2025, 11, 1): {
                    'genome|||short-read': 2,
                },
                date(2025, 12, 1): {
                    'genome|||short-read': 2,
                },
            },
            project_2: {
                date(2025, 10, 1): {
                    'genome|||short-read': 1,
                },
                date(2025, 11, 1): {
                    'genome|||short-read': 2,
                },
                date(2025, 12, 1): {
                    'genome|||short-read': 2,
                },
            },
        }

    @pytest.mark.asyncio
    @pytest.mark.db_superuser
    @pytest.mark.project_roles(['writer'])
    async def test_history_partial_sum(
        self, connection_with_project: Connection, test_sample: int, mock_date
    ):
        """Test the case where less types are present initially and more are added over time."""
        # Mock today's date.
        today = date(year=2025, month=12, day=31)
        mock_date('db.python.tables.sequencing_group.date', today)

        # Define some test data
        test_data = [
            {
                'id': 1,
                'sample_id': test_sample,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1).isoformat(),
            },
            {
                'id': 2,
                'sample_id': test_sample,
                'type': 'genome',
                'technology': 'long-read',
                'sg_date': date(2025, 11, 1).isoformat(),
            },
            {
                'id': 3,
                'sample_id': test_sample,
                'type': 'chip',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1).isoformat(),
            },
            {
                'id': 4,
                'sample_id': test_sample,
                'type': 'chip',
                'technology': 'long-read',
                'sg_date': date(2025, 11, 1).isoformat(),
            },
        ]

        test_data_query = f"""
            INSERT INTO sequencing_group_history
                (id, sample_id, type, technology, archived, sys_period)
            VALUES
                (%(id)s, %(sample_id)s, %(type)s, %(technology)s, false, tstzrange(%(sg_date)s, '{today.isoformat()}'))"""

        # Insert the test data to the DB
        conn = connection_with_project.pg_connection
        async with conn.transaction(), conn.cursor() as cur:
            await cur.executemany(test_data_query, test_data)

        assert connection_with_project.project_id is not None
        sg_table = SequencingGroupTable(connection_with_project)
        result = await sg_table.get_sequencing_group_counts_by_month(
            [connection_with_project.project_id]
        )

        expected_output = {
            connection_with_project.project_id: {
                date(2025, 10, 1): {
                    'genome|||short-read': 1,
                    'chip|||short-read': 1,
                },
                date(2025, 11, 1): {
                    'genome|||short-read': 1,
                    'genome|||long-read': 1,
                    'chip|||short-read': 1,
                    'chip|||long-read': 1,
                },
                date(2025, 12, 1): {
                    'genome|||short-read': 1,
                    'genome|||long-read': 1,
                    'chip|||short-read': 1,
                    'chip|||long-read': 1,
                },
            }
        }

        assert result == expected_output


@pytest.mark.asyncio
class TestAssayMetaFiltersGraphQL:
    """Test generic filters in a GraphQL context against real database"""

    @pytest.fixture(autouse=True)
    async def setup(
        self,
        connection_with_project: Connection,
    ):
        # Insert sample into test project, and get sample_id
        sample_layer = SampleLayer(connection_with_project)

        def get_external_ids(number: int) -> dict[str, str]:
            return {'default': f'SEQ{number:02d}', 'collaborator2': f'CBSEQ_{number}'}

        def get_assay_meta_for_read(read):
            return {
                '1': 1,
                'alpha': ['b', 'e', 't'],
                'reads': [read],
                'nested': [[read]],
                'nested_dict': {'inner': [read]},
                **DEFAULT_SEQUENCING_META,
            }

        self.reads = [
            {'size': 10000, 'basename': 'gs://bucket/foobar.fastq.gz'},
            {'size': 5000, 'basename': 'gs://bucket/foobar2.fastq.gz'},
            {'size': 20000, 'basename': 'gs://bucket/foobar3.fq.gz'},
        ]

        await sample_layer.upsert_sample(
            SampleUpsertInternal(
                project=connection_with_project.project_id,
                external_ids={PRIMARY_EXTERNAL_ORG: 'TEST_META_FILTER'},
                type='blood',
                active=True,
                meta={'test_key': 'test_value'},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform=f'illumina{i + 1:02d}',
                        meta={'sg_key': 'sg_value'},
                        external_ids=get_external_ids(i + 1),
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta=get_assay_meta_for_read(read),
                                external_ids=get_external_ids(i + 1),
                            )
                        ],
                    )
                    for i, read in enumerate(self.reads)
                ],
            )
        )

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_graphql_assay_filter(
        self,
        connection_with_project: Connection,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test that demonstrates how GraphQL basic sequencing group filtering would work.
        """

        assert connection_with_project.project is not None
        project_name = str(connection_with_project.project.name)

        # Insert sequencing group and assay data
        _query = """
            query SgByAssayMeta($project: StrGraphQLFilter!) {
                sequencingGroups(project: $project) {
                    id
                    type
                    assays {
                        meta
                    }
                }
            }
        """
        variables = {
            'project': {'eq': project_name},
        }

        results = await graphql_query(_query, variables=variables)

        assert 'data' in results
        assert 'sequencingGroups' in results['data']
        results = results['data']['sequencingGroups']

        # Verify that the filter correctly handles JSON path-like keys
        assert len(results) == len(self.reads)
        for r in results:
            assert r.get('id')
            assert r.get('type')

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_graphql_assay_meta_filter(
        self,
        connection_with_project: Connection,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test that demonstrates how GraphQL assayMeta filtering would work.
        """
        assert connection_with_project.project is not None
        project_name = str(connection_with_project.project.name)

        # Insert sequencing group and assay data
        _query = """
            query SgByAssayMeta($project: StrGraphQLFilter!, $assayMeta: JSON!) {
                sequencingGroups(project: $project, assayMeta: $assayMeta) {
                    id
                    type
                    assays {
                        meta
                    }
                }
            }
        """
        variables = {
            'project': {'eq': project_name},
            'assayMeta': {
                'reads[*].basename': {'contains': 'fastq.gz'},
                'reads[*].size': {'gte': 10000},
            },
        }

        results = await graphql_query(_query, variables=variables)

        assert 'data' in results
        assert 'sequencingGroups' in results['data']
        results = results['data']['sequencingGroups']

        # Verify that the filter correctly handles JSON path-like keys
        assert len(results) > 0
        for r in results:
            assert r.get('id')
            assert r.get('type')
            assert 'assays' in r
            for assay in r['assays']:
                assert 'meta' in assay
                assert 'reads' in assay['meta']
                for read in assay['meta']['reads']:
                    assert 'basename' in read
                    assert 'size' in read
                    assert 'fastq.gz' in read['basename']
                    assert read['size'] >= 10000

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_graphql_assay_meta_filter_nested(
        self,
        connection_with_project: Connection,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test that demonstrates how GraphQL assayMeta filtering would work.
        """
        assert connection_with_project.project is not None
        project_name = str(connection_with_project.project.name)

        _query = """
            query SgByAssayMeta($project: StrGraphQLFilter!, $assayMeta: JSON!) {
                sequencingGroups(project: $project, assayMeta: $assayMeta) {
                    id
                    type
                    assays {
                        meta
                    }
                }
            }
        """
        variables = {
            'project': {'eq': project_name},
            'assayMeta': {
                'nested[*][*].basename': {'contains': 'fastq.gz'},
                'nested[*][*].size': {'gte': 10000},
            },
        }

        results = await graphql_query(_query, variables=variables)

        assert 'data' in results
        assert 'sequencingGroups' in results['data']
        results = results['data']['sequencingGroups']

        # Verify that the filter correctly handles JSON path-like keys
        def assert_reads_results(results):
            assert len(results) > 0
            for r in results:
                assert r.get('id')
                assert r.get('type')
                assert 'assays' in r
                for assay in r['assays']:
                    assert 'meta' in assay
                    assert 'reads' in assay['meta']
                    for read in assay['meta']['reads']:
                        assert 'basename' in read
                        assert 'size' in read
                        assert 'fastq.gz' in read['basename']
                        assert read['size'] >= 10000

        assert_reads_results(results)

        # Repeat with the nested dictionary
        _query = """
            query SgByAssayMeta($project: StrGraphQLFilter!, $assayMeta: JSON!) {
                sequencingGroups(project: $project, assayMeta: $assayMeta) {
                    id
                    type
                    assays {
                        meta
                    }
                }
            }
        """
        variables = {
            'project': {'eq': project_name},
            'assayMeta': {
                'nested_dict.inner[*].basename': {'contains': 'fastq.gz'},
                'nested_dict.inner[*].size': {'gte': 10000},
            },
        }

        results = await graphql_query(_query, variables=variables)

        assert 'data' in results
        assert 'sequencingGroups' in results['data']
        results = results['data']['sequencingGroups']

        assert_reads_results(results)
