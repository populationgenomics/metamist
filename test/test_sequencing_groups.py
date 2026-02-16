import pytest

from datetime import date, datetime
from unittest import mock

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import AnalysisLayer, SampleLayer, SequencingGroupLayer
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupFilter, SequencingGroupTable
from models.enums.analysis import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)

@pytest.fixture
async def test_sample(connection_with_project: Connection) -> int:
    project_id = connection_with_project.project_id

    conn = connection_with_project.pg_connection
    # Create audit_log entry first
    create_audit_log = t"""\
        INSERT INTO audit_log (author, auth_project)
        VALUES ('test', {project_id})
        RETURNING id"""
    cur = await conn.execute(create_audit_log)
    row = await cur.fetchone()
    assert row is not None
    audit_log_id = row['id']

    insert_sample = t"""\
        INSERT INTO sample 
            (project, meta, type, active, author, audit_log_id)
        VALUES ({project_id}, '{{"meta_key": "meta_value"}}', 'blood', true, 'test_aurthor', {audit_log_id})
        RETURNING id;"""
    
    cur = await conn.execute(insert_sample)
    row = await cur.fetchone()
    assert row is not None
    sample_id = row['id']

    insert_external_id = t"""\
        INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
        VALUES ({project_id}, {sample_id}, 'default', 'TESTING001', {audit_log_id})
        """
    await conn.execute(insert_external_id)

    return sample_id

@pytest.fixture
def sequencing_group_model(test_sample: int) -> SequencingGroupUpsertInternal:
    """
    Get sample model with sequencing-groups, return in a function
    to protect against any mutation to this model
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
        # assays=[
        #     AssayUpsertInternal(
        #         type='sequencing',
        #         external_ids={},
        #         meta={
        #             'sequencing_type': 'genome',
        #             'sequencing_platform': 'short-read',
        #             'sequencing_technology': 'illumina',
        #         },
        #     )
        # ],
    )


class TestSequencingGroup:
    """Test sequencing groups business logic"""

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_debug(
        self,
        connection_with_project: Connection,
    ):
        s_table = SampleTable(connection_with_project)
        sg_table = SequencingGroupTable(connection_with_project)

        sample_id = await s_table.insert_sample(
            {PRIMARY_EXTERNAL_ORG: 'Test01'},
            'blood',
            True,
            None, None, None, None
        )

        id1 = await sg_table.create_sequencing_group(
            sample_id,
            'genome',
            'short-read',
            'illumina',
            []
        )

        id2 = await sg_table.create_sequencing_group(
            sample_id,
            'exome',
            'short-read',
            'illumina',
            []
        )

        await sg_table.update_sequencing_group(id1, {'test_key': 'test_val'}, 'illumina')

        p, sg = await sg_table.get_sequencing_groups_by_ids([id2, id1])
        print(sg)

        assert False

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
        sg_id = sg[0].id
        sg = await sg_layer.get_sequencing_group_by_id(sg_id)

        inserted_sg = sample_upsert_model.sequencing_groups[0]
        assert inserted_sg.id == sg_id
        assert inserted_sg.type == sg.type
        assert inserted_sg.technology == sg.technology
        assert inserted_sg.platform.lower() == sg.platform.lower()
        assert inserted_sg.meta == sg.meta

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_update_sequencing_group(self):
        """Test updating metadata on a sequencing group"""
        sample = await self.slayer.upsert_sample(get_sample_model())

        upsert_sg = SequencingGroupUpsertInternal(
            id=sample.sequencing_groups[0].id,
            meta={'another-meta': 'field'},
        )
        # Check that id is being returned when seqg created
        sg_return = await self.sglayer.upsert_sequencing_groups([upsert_sg])
        self.assertEqual(sg_return[0].id, upsert_sg.id)

        sg = await self.sglayer.get_sequencing_group_by_id(
            sample.sequencing_groups[0].id
        )

        self.assertDictEqual(
            {'another-meta': 'field', 'meta-key': 'meta-value'}, sg.meta
        )

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_auto_deprecation_of_old_sequencing_group(self):
        """Test creating a sequencing-group, and test the old one is archived"""
        sample = await self.slayer.upsert_sample(get_sample_model())

        # self.sglayer.get_sequencing_groups_by_ids()

        new_upsert = SampleUpsertInternal(
            id=sample.id,
            sequencing_groups=[
                SequencingGroupUpsertInternal(
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
                            id=sample.sequencing_groups[0].assays[0].id,
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
            ],
        )

        updated_sample = await self.slayer.upsert_sample(new_upsert)

        old_sg = await self.sglayer.get_sequencing_group_by_id(
            sample.sequencing_groups[0].id
        )
        # now check the existing sequencing group was archived
        self.assertTrue(old_sg.archived)

        # check that the "active" sequencing group is the new one
        active_sgs = await self.sglayer.query(
            SequencingGroupFilter(
                sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                    id=GenericFilter(eq=sample.id)
                )
            )
        )

        self.assertTrue(all(not sg.archived for sg in active_sgs))
        self.assertEqual(len(active_sgs), 1)
        self.assertEqual(updated_sample.sequencing_groups[0].id, active_sgs[0].id)

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_query_with_assay_metadata(self):
        """Test searching with an assay metadata filter"""
        sample_to_insert = get_sample_model()

        # Add extra sequencing group
        sample_to_insert.sequencing_groups.append(
            SequencingGroupUpsertInternal(
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
        sample = await self.slayer.upsert_sample(sample_to_insert)

        # Query for genome assay metadata
        sgs = await self.sglayer.query(
            SequencingGroupFilter(
                assay=SequencingGroupFilter.SequencingGroupAssayFilter(
                    meta={'sequencing_type': GenericFilter(eq='genome')}
                )
            )
        )
        self.assertEqual(len(sgs), 1)
        self.assertEqual(sgs[0].id, sample.sequencing_groups[0].id)

        # Query for exome assay metadata
        sgs = await self.sglayer.query(
            SequencingGroupFilter(
                assay=SequencingGroupFilter.SequencingGroupAssayFilter(
                    meta={'sequencing_type': GenericFilter(eq='exome')}
                )
            )
        )
        self.assertEqual(len(sgs), 1)
        self.assertEqual(sgs[0].id, sample.sequencing_groups[1].id)

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_query_with_creation_date(self):
        """Test fetching using a creation date filter"""
        sample_to_insert = get_sample_model()
        await self.slayer.upsert_sample(sample_to_insert)

        # There's a race condition here -- don't run this near UTC midnight!
        today = datetime.utcnow().date()

        # Query for sequencing group with creation date before today
        sgs = await self.sglayer.query(
            SequencingGroupFilter(created_on=GenericFilter(lt=today))
        )
        self.assertEqual(len(sgs), 0)

        # Query for sequencing group with creation date today
        sgs = await self.sglayer.query(
            SequencingGroupFilter(created_on=GenericFilter(eq=today))
        )
        self.assertEqual(len(sgs), 1)

        sgs = await self.sglayer.query(
            SequencingGroupFilter(created_on=GenericFilter(lte=today))
        )
        self.assertEqual(len(sgs), 1)

        sgs = await self.sglayer.query(
            SequencingGroupFilter(created_on=GenericFilter(gte=today))
        )
        self.assertEqual(len(sgs), 1)

        # Query for sequencing group with creation date today
        sgs = await self.sglayer.query(
            SequencingGroupFilter(created_on=GenericFilter(gt=today))
        )
        self.assertEqual(len(sgs), 0)

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_query_finds_sgs_which_have_cram_analysis(self):
        """Test querying for sequencing groups which have a cram or gvcf analysis"""
        sample_to_insert = get_sample_model()

        # Add extra sequencing group
        sample_to_insert.sequencing_groups.append(
            SequencingGroupUpsertInternal(
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
        sample = await self.slayer.upsert_sample(sample_to_insert)

        # Create analysis for cram and gvcf
        await self.alayer.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sample.sequencing_groups[0].id],
                meta={},
            )
        )
        await self.alayer.create_analysis(
            AnalysisInternal(
                type='gvcf',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sample.sequencing_groups[1].id],
                meta={},
            )
        )

        # Query for cram analysis
        sgs = await self.sglayer.query(SequencingGroupFilter(has_cram=True))
        self.assertEqual(len(sgs), 1)
        self.assertEqual(sgs[0].id, sample.sequencing_groups[0].id)

        # Query for gvcf analysis
        sgs = await self.sglayer.query(SequencingGroupFilter(has_gvcf=True))
        self.assertEqual(len(sgs), 1)
        self.assertEqual(sgs[0].id, sample.sequencing_groups[1].id)

        # Query for both cram AND gvcf analysis
        sgs = await self.sglayer.query(
            SequencingGroupFilter(has_gvcf=True, has_cram=True)
        )
        self.assertEqual(len(sgs), 0)

        # Add first SG to gvcf analysis
        await self.alayer.create_analysis(
            AnalysisInternal(
                type='gvcf',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[sample.sequencing_groups[0].id],
                meta={},
            )
        )

        # Query for both cram AND gvcf analysis now that first SG has gvcf analysis
        sgs = await self.sglayer.query(
            SequencingGroupFilter(has_gvcf=True, has_cram=True)
        )
        self.assertEqual(len(sgs), 1)
        self.assertEqual(sgs[0].id, sample.sequencing_groups[0].id)

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_archiving_sequencing_groups(self):
        """Check that sequencing groups can be archived from graphql"""
        sample_model = SampleUpsertInternal(
            meta={},
            external_ids={PRIMARY_EXTERNAL_ORG: 'EXID1'},
            type='blood',
            sequencing_groups=[
                SequencingGroupUpsertInternal(
                    type='genome',
                    technology='short-read',
                    platform='illumina',
                    meta={},
                    assays=[],
                ),
                SequencingGroupUpsertInternal(
                    type='genome',
                    technology='short-read',
                    platform='illumina',
                    meta={},
                    assays=[],
                ),
                SequencingGroupUpsertInternal(
                    type='exome',
                    technology='short-read',
                    platform='illumina',
                    meta={},
                    assays=[],
                ),
            ],
        )

        sample = await self.slayer.upsert_sample(sample_model)
        assert sample.sequencing_groups
        sg1 = sample.sequencing_groups[0].to_external().id
        sg2 = sample.sequencing_groups[1].to_external().id

        assert sg1, sg2

        archive_result = await self.run_graphql_query_async(
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

        archived_sgs = archive_result['sequencingGroup']['archiveSequencingGroups']

        self.assertEqual(len(archived_sgs), 2)
        self.assertEqual(archived_sgs[0]['id'], sg1)
        self.assertEqual(archived_sgs[0]['archived'], True)
        self.assertEqual(archived_sgs[1]['id'], sg2)
        self.assertEqual(archived_sgs[1]['archived'], True)

    @pytest.mark.asyncio
    @pytest.mark.skip
    async def test_history_no_sum(self):
        """Test the trivial case where there are no sequencing groups."""

        # Set up mocking for rows returned from the table query.
        with mock.patch(
            'db.python.connect.databases.Database.fetch_all', return_value=[]
        ):
            sg_table = self.sglayer.seqgt
            result = await sg_table.get_sequencing_group_counts_by_month([])

        self.assertDictEqual(result, {})

    @pytest.mark.asyncio
    @pytest.mark.skip
    @mock.patch('db.python.tables.sequencing_group.date', wraps=date)
    async def test_history_sum_multiple_projects(self, mock_date):
        """Test the case where type:technology combinations are summed and held for the same project."""
        # Mock today's date.
        mock_date.today.return_value = date(year=2025, month=12, day=31)

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
    @pytest.mark.skip
    @mock.patch('db.python.tables.sequencing_group.date', wraps=date)
    async def test_history_partial_sum(self, mock_date):
        """Test the case where less types are present initially and more are added over time."""
        # Mock today's date.
        mock_date.today.return_value = date(year=2025, month=12, day=31)

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
                'technology': 'long-read',
                'sg_date': date(2025, 11, 1),
                'num_sg': 3,
            },
            {
                'project': 0,
                'type': 'chip',
                'technology': 'short-read',
                'sg_date': date(2025, 10, 1),
                'num_sg': 4,
            },
            {
                'project': 0,
                'type': 'chip',
                'technology': 'long-read',
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
                        'chip|||short-read': 4,
                    },
                    date(2025, 11, 1): {
                        'genome|||short-read': 2,
                        'genome|||long-read': 3,
                        'chip|||short-read': 4,
                        'chip|||long-read': 5,
                    },
                    date(2025, 12, 1): {
                        'genome|||short-read': 2,
                        'genome|||long-read': 3,
                        'chip|||short-read': 4,
                        'chip|||long-read': 5,
                    },
                }
            },
        )
