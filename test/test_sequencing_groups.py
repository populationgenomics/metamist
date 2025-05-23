from datetime import datetime

from db.python.filters import GenericFilter
from db.python.layers import AnalysisLayer, SampleLayer, SequencingGroupLayer
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.enums.analysis import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.testbase import DbIsolatedTest, run_as_sync


def get_sample_model():
    """
    Get sample model with sequencing-groups, return in a function
    to protect against any mutation to this model
    """
    return SampleUpsertInternal(
        meta={},
        external_ids={PRIMARY_EXTERNAL_ORG: 'EX_ID'},
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type='genome',
                technology='short-read',
                platform='ILLUMINA',
                meta={
                    'meta-key': 'meta-value',
                },
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
        ],
    )


class TestSequencingGroup(DbIsolatedTest):
    """Test sequencing groups business logic"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.sglayer = SequencingGroupLayer(self.connection)
        self.slayer = SampleLayer(self.connection)
        self.alayer = AnalysisLayer(self.connection)

    @run_as_sync
    async def test_empty_query(self):
        """
        Test empty IDs to see the query construction
        """
        sgs = await self.sglayer.query(SequencingGroupFilter(id=GenericFilter(in_=[])))
        self.assertEqual(len(sgs), 0)

    @run_as_sync
    async def test_insert_sequencing_group(self):
        """Test inserting and fetching a sequencing group"""
        sample_to_insert = get_sample_model()
        sample = await self.slayer.upsert_sample(sample_to_insert)
        sg_id = sample.sequencing_groups[0].id
        sg = await self.sglayer.get_sequencing_group_by_id(sg_id)

        inserted_sg = sample_to_insert.sequencing_groups[0]
        self.assertEqual(inserted_sg.id, sg_id)
        self.assertEqual(inserted_sg.type, sg.type)
        self.assertEqual(inserted_sg.technology, sg.technology)
        self.assertEqual(inserted_sg.platform.lower(), sg.platform.lower())
        self.assertDictEqual(inserted_sg.meta, sg.meta)

    @run_as_sync
    async def test_update_sequencing_group(self):
        """Test updating metadata on a sequencing group"""
        sample = await self.slayer.upsert_sample(get_sample_model())

        upsert_sg = SequencingGroupUpsertInternal(
            id=sample.sequencing_groups[0].id,
            meta={'another-meta': 'field'},
        )
        await self.sglayer.upsert_sequencing_groups([upsert_sg])

        sg = await self.sglayer.get_sequencing_group_by_id(
            sample.sequencing_groups[0].id
        )

        self.assertDictEqual(
            {'another-meta': 'field', 'meta-key': 'meta-value'}, sg.meta
        )

    @run_as_sync
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

    @run_as_sync
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

    @run_as_sync
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

    @run_as_sync
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

    @run_as_sync
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
