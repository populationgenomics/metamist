import pytest

from collections.abc import Callable
from datetime import date
from unittest import mock

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import AnalysisLayer, SequencingGroupLayer
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupFilter, SequencingGroupTable
from models.enums.analysis import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.conftest import GraphQLQueryFunction

@pytest.fixture
async def test_sample(connection_with_project: Connection) -> int:
    """
    Create a sample directly in the database for testing sequencing groups.
    This is a temporary fixture until sample layer is migrated.
    @TODO replace this when ready
    """
    project_id = connection_with_project.project_id

    conn = connection_with_project.pg_connection
    # Create audit_log entry first
    create_audit_log = t"""
        INSERT INTO audit_log (author, auth_project)
        VALUES ('test', {project_id})
        RETURNING id"""
    cur = await conn.execute(create_audit_log)
    row = await cur.fetchone()
    assert row is not None
    audit_log_id = row['id']

    insert_sample = t"""
        INSERT INTO sample 
            (project, meta, type, active, author, audit_log_id)
        VALUES ({project_id}, '{{"meta_key": "meta_value"}}', 'blood', true, 'test_aurthor', {audit_log_id})
        RETURNING id;"""
    
    cur = await conn.execute(insert_sample)
    row = await cur.fetchone()
    assert row is not None
    sample_id = row['id']

    insert_external_id = t"""
        INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
        VALUES ({project_id}, {sample_id}, 'default', 'TESTING001', {audit_log_id})
        """
    await conn.execute(insert_external_id)

    return sample_id

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
        sg_id = sg_upsert[0].id
        sg = await sg_layer.get_sequencing_group_by_id(sg_id)

        inserted_sg = sg_upsert[0]
        assert inserted_sg.id == sg_id
        assert inserted_sg.type == sg.type
        assert inserted_sg.technology == sg.technology
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

        assert {'another-meta': 'field', 'meta-key': 'meta-value'} == sg_from_db.meta

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
        assert old_sg.archived == True

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
    @pytest.mark.skip(
        reason='Querying JSON keys is not yet implemented'
    ) # TODO: Implement this test when querying JSON keys is implemented
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
            await conn.execute('ALTER TABLE main.sequencing_group DISABLE TRIGGER versioning_trigger')
            await conn.execute(current_query)
            await conn.execute('ALTER TABLE main.sequencing_group ENABLE TRIGGER versioning_trigger')

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
        self,
        connection_with_project: Connection,
        test_sample: int
    ):
        """Test querying for sequencing groups which have a cram or gvcf analysis"""
        sg_layer = SequencingGroupLayer(connection_with_project)


        test_sg_data = [
            {   
                'type': 'genome',
                'technology': 'short-read'
            },
            {
                'type': 'genome',
                'technology': 'long-read'
            }
        ]

        # Firstly create two sequencing groups to attach analyses to
        insert_sgs = f"""
            INSERT INTO sequencing_group (sample_id, type, technology)
            VALUES ({test_sample}, %(type)s, %(technology)s)
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
            await cur.execute(insert_analysis_sequencing_group, {'analysis_id': cram_id, 'sg_id': sg_ids[0]})
            await cur.execute(insert_analysis_sequencing_group, {'analysis_id': gvcf_id, 'sg_id': sg_ids[0]})

        # Query for cram analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_cram=True))
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[0]

        # Query for gvcf analysis
        sgs = await sg_layer.query(SequencingGroupFilter(has_gvcf=True))
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[1].id

        # Query for both cram AND gvcf analysis
        sgs = await sg_layer.query(
            SequencingGroupFilter(has_gvcf=True, has_cram=True)
        )
        assert len(sgs) == 0

        # Add first SG to gvcf analysis
        async with connection_with_project.pg_connection.cursor() as cur:
            await cur.execute(insert_analysis_sequencing_group, {'analysis_id': gvcf_id, 'sg_id': sg_ids[0]})

        # Query for both cram AND gvcf analysis now that first SG has gvcf analysis
        sgs = await sg_layer.query(
            SequencingGroupFilter(has_gvcf=True, has_cram=True)
        )
        assert len(sgs) == 1
        assert sgs[0].id == sg_ids[0]

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_archiving_sequencing_groups(
        self,
        connection_with_project: Connection,
        sequencing_group_model: SequencingGroupUpsertInternal,
        graphql_query: GraphQLQueryFunction
    ):
        """Check that sequencing groups can be archived from graphql"""
        sg_layer = SequencingGroupLayer(connection_with_project)
        sgs_to_insert = [sequencing_group_model]

        sgs_to_insert.append(SequencingGroupUpsertInternal(
            sample_id=sequencing_group_model.sample_id,
            type='genome',
            technology='long-read',
            platform='illumina',
            meta={'meta-1': 'test-1'},
            assays=[],
        ))
        sgs_to_insert.append(SequencingGroupUpsertInternal(
            sample_id=sequencing_group_model.sample_id,
            type='exome',
            technology='short-read',
            platform='illumina',
            meta={'meta-2': 'test-2'},
            assays=[],
        ))
        sgs_to_insert.append(SequencingGroupUpsertInternal(
            sample_id=sequencing_group_model.sample_id,
            type='exome',
            technology='long-read',
            platform='illumina',
            meta={'meta-3': 'test-3'},
            assays=[],
        ))

        sgs = await sg_layer.upsert_sequencing_groups(sgs_to_insert)
        assert sgs
        sg1 = sgs[0].to_external().id
        sg2 = sgs[1].to_external().id

        assert sg1, sg2

        # Check that the sequencing groups aren't initially archived
        sgs_from_db = await sg_layer.get_sequencing_groups_by_ids([sgs[0].id, sgs[1].id])
        assert sgs_from_db[0].archived == False
        assert sgs_from_db[1].archived == False

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

        archived_sgs = archive_result['data']['sequencingGroup']['archiveSequencingGroups']

        assert len(archived_sgs) == 2
        assert archived_sgs[0]['id'] == sg1
        assert archived_sgs[0]['archived'] == True
        assert archived_sgs[1]['id'] == sg2
        assert archived_sgs[1]['archived'] == True

    @pytest.mark.asyncio
    async def test_history_no_sum(self, connection: Connection):
        """Test the trivial case where there are no sequencing groups."""
        sg_table = SequencingGroupTable(connection)
        result = await sg_table.get_sequencing_group_counts_by_month([])

        assert result == {}

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason='Currently requires more complex testing fixtures that may be easier to achieve one more functionality is migrated'
    ) # TODO Revisit when ready
    async def test_history_sum_multiple_projects(
        self,
        connection: Connection,
        test_sample: int,
        mock_date,
    ):
        """Test the case where type:technology combinations are summed and held for the same project."""
        # Mock today's date.
        today = date(year=2025, month=12, day=31)
        mock_date('db.python.tables.sequencing_group.date', today)

        # Set up mocking for rows returned from the table query.
        rows_mock = [
            {
                'project': 0,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1),
                'num_sg': 2,
            },
            {
                'project': 0,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 11, 1),
                'num_sg': 4,
            },
            {
                'project': 1,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1),
                'num_sg': 3,
            },
            {
                'project': 1,
                'type': 'genome',
                'technology': 'short-read',
                'sg_date': date(2025, 11, 1),
                'num_sg': 5,
            },
        ]
        with mock.patch(
            'db.python.connect.databases.Database.fetch_all', return_value=rows_mock
        ):
            sg_table = self.sglayer.seqgt
            result = await sg_table.get_sequencing_group_counts_by_month([0])

        self.assertDictEqual(
            result,
            {
                0: {
                    date(2025, 10, 1): {
                        'genome|||short-read': 2,
                    },
                    date(2025, 11, 1): {
                        'genome|||short-read': 6,
                    },
                    date(2025, 12, 1): {
                        'genome|||short-read': 6,
                    },
                },
                1: {
                    date(2025, 10, 1): {
                        'genome|||short-read': 3,
                    },
                    date(2025, 11, 1): {
                        'genome|||short-read': 8,
                    },
                    date(2025, 12, 1): {
                        'genome|||short-read': 8,
                    },
                },
            },
        )

    @pytest.mark.asyncio
    async def test_history_partial_sum(
        self,
        connection_with_project: Connection,
        test_sample: int,
        mock_date
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
                'id': 1,
                'sample_id': test_sample,
                'type': 'genome',
                'technology': 'long-read',
                'sg_date': date(2025, 11, 1).isoformat(),
            },
            {
                'id': 1,
                'sample_id': test_sample,
                'type': 'chip',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1).isoformat(),
            },
            {
                'id': 1,
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
        async with conn.transaction():
            async with conn.cursor() as cur:
                # Disable the trigger so that the sys_period isn't overwritten
                await cur.execute('ALTER TABLE main.sequencing_group DISABLE TRIGGER versioning_trigger')
                await cur.executemany(test_data_query, test_data)
                await cur.execute('ALTER TABLE main.sequencing_group ENABLE TRIGGER versioning_trigger')

        sg_table = SequencingGroupTable(connection_with_project)
        result = await sg_table.get_sequencing_group_counts_by_month([test_sample])

        expected_output = {
            test_sample: {
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
