from db.python.filters import GenericFilter
from db.python.layers import CohortLayer, SampleLayer, SequencingGroupLayer
from db.python.tables.cohort import CohortFilter
from models.enums.cohort import CohortStatus
from models.models.cohort import CohortCriteriaInternal
from models.utils.sequencing_group_id_format import sequencing_group_id_format
from test.testbase import DbIsolatedTest
from test.testbase import run_as_sync

from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)

def get_sample_model(
    eid, s_type='blood', sg_type='genome', plat='illumina'
):
    """Create a minimal sample"""
    return SampleUpsertInternal(
        meta={},
        external_ids={PRIMARY_EXTERNAL_ORG: f'EXID{eid}'},
        type=s_type,
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type=sg_type,
                technology='short-read',
                platform=plat,
                meta={},
                assays=[],
            ),
            SequencingGroupUpsertInternal(
                type=sg_type,
                technology='long-read',
                platform=plat,
                meta={},
                assays=[],
            ),
        ],
    )

class TestStatusInCohortDBLayer(DbIsolatedTest):
    """Test cohort status related functions"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohort_layer = CohortLayer(self.connection)
        self.sample_layer = SampleLayer(self.connection)
        self.sg_layer = SequencingGroupLayer(self.connection)

        self.sA = await self.sample_layer.upsert_sample(get_sample_model('A'))
        self.sB = await self.sample_layer.upsert_sample(get_sample_model('B', 'saliva', 'exome', 'ONT'))

        self.sgA = [sequencing_group_id_format(sg.id) for sg in self.sA.sequencing_groups]
        self.sgA_raw = [sg.id for sg in self.sA.sequencing_groups]

        self.sgB = [sequencing_group_id_format(sg.id) for sg in self.sB.sequencing_groups]
        self.sgB_raw = [sg.id for sg in self.sB.sequencing_groups]

        #SG_B will be associated to this cohort (based on sample_type)
        self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Sample cohort',
            cohort_name='Sample cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sample_type=['saliva'],
            )
        )

    @run_as_sync
    async def test_create_custom_cohort_and_verify_status(self):

        created_cohort_in_list = await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id)))
        self.assertTrue(created_cohort_in_list)
        self.assertTrue(len(created_cohort_in_list) == 1)
        created_cohort = created_cohort_in_list[0]

        self.assertEqual(created_cohort.id, self.cohort.cohort_id)
        self.assertEqual(created_cohort.description, 'Sample cohort')
        self.assertEqual(created_cohort.name, 'Sample cohort 1')
        self.assertEqual(created_cohort.status, CohortStatus.ACTIVE)


    @run_as_sync
    async def test_get_cohort_with_inactive_sample(self):
        await self.sample_layer.upsert_sample(SampleUpsertInternal(id=self.sB.id, active=False))
        updated_sample = await self.sample_layer.get_by_id(sample_id=self.sB.id)
        self.assertFalse(updated_sample.active)
        cohort = (await self.cohort_layer.query(
            CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
        ))[0]
        self.assertEqual(cohort.status, CohortStatus.INACTIVE)

    @run_as_sync
    async def test_get_cohort_with_archived_sg(self):
        from db.python.filters.sequencing_group import SequencingGroupFilter

        await self.sg_layer.archive_sequencing_group(sequencing_group_id=self.sgB_raw[0])
        archived_sg = (await self.sg_layer.query(SequencingGroupFilter(id=GenericFilter(in_=[self.sgB_raw[0]]), active_only=GenericFilter(eq=False))))[0]
        self.assertTrue(archived_sg.archived)
        cohort = (await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))))[0]
        self.assertEqual(cohort.status, CohortStatus.INACTIVE)


    @run_as_sync
    async def test_get_cohort_status_with_all_active(self):
        from db.python.filters.sequencing_group import SequencingGroupFilter

        queried_sample = await self.sample_layer.get_by_id(sample_id=self.sB.id)
        self.assertTrue(queried_sample.active)

        queried_sg = (await self.sg_layer.query(SequencingGroupFilter(id=GenericFilter(in_=[self.sgB_raw[0]]))))[0]
        self.assertFalse(queried_sg.archived)

        #query directly from the cohort table as final status value is computed runtime based on sample, sg and cohort
        cohort_raw_entry = await self.connection.connection.fetch_one('SELECT status FROM cohort where id = :cohort_id',{'cohort_id': self.cohort.cohort_id})
        self.assertEqual(dict(cohort_raw_entry)['status'].lower(), CohortStatus.ACTIVE.value)

        cohort = (await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))))[0]
        self.assertEqual(cohort.status, CohortStatus.ACTIVE)


    @run_as_sync
    async def test_query_cohort_with_multiple_samples_and_at_least_one_inactive(self):
        new_cohort = self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Sample cohort Test 2',
            cohort_name='Sample cohort 2',
            dry_run=False,

            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.sgA_raw[0], self.sgB_raw[0]],
            )
        )
        await self.sample_layer.upsert_sample(SampleUpsertInternal(id=self.sB.id, active=False))

        cohort = (await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=new_cohort.cohort_id))))[0]
        self.assertEqual(cohort.status, CohortStatus.INACTIVE)


    @run_as_sync
    async def test_get_cohort_with_inactive_status(self):
        #directly update without using the cohort_db_layer

        await self.connection.connection.fetch_one('UPDATE cohort SET status = :status '
                                                   'WHERE id = :cohort_id',{'cohort_id': self.cohort.cohort_id,
                                                                            'status': CohortStatus.INACTIVE.value.upper()})

        cohort = (await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))))[0]
        self.assertEqual(cohort.status, CohortStatus.INACTIVE)

    @run_as_sync
    async def test_get_cohort_with_deleted_sg_mapping(self):
        # remove cohort -> sg mappings from cohort_sequencing_group table
        await self.connection.connection.fetch_one(
            'DELETE from cohort_sequencing_group WHERE cohort_id = :cohort_id',
            {
                'cohort_id': self.cohort.cohort_id
            },
        )
        cohort = (await self.cohort_layer.query(CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))))[0]
        self.assertEqual(cohort.status, CohortStatus.INACTIVE)


    @run_as_sync
    async def test_get_cohort_internal_to_external_mapping(self):
        #TODO api
        pass

    @run_as_sync
    async def test_get_cohort_internal_to_external_mapping_grapQL(self):
        #TODO graphQL
        pass

    @run_as_sync
    async def test_query_cohort_with_filter_by_id(self):
        #TODO graphQL
        pass

    @run_as_sync
    async def test_query_cohort_with_filter_by_non_id_field(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO graphQL
        pass


    @run_as_sync
    async def test_get_cohort_with_status_field(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO test API
        pass


    @run_as_sync
    async def test_get_info_of_non_existent_cohort(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO test API
        pass


    @run_as_sync
    async def test_update_cohort_status(self):
        #update cohort status from active -> inactive and then inactive to active
        #TODO test API
        pass

    @run_as_sync
    async def test_update_cohort_fields(self):
        #update cohort status from active -> inactive and then inactive to active
        #TODO test API
        pass

    @run_as_sync
    async def test_update_cohort_fields(self):
        #update cohort status from active -> inactive and then inactive to active
        #TODO test graphQL
        pass

    @run_as_sync
    async def test_get_info_of_non_existent_cohort(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO test graphQL
        pass

    @run_as_sync
    async def test_update_non_existent_cohort(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO test graphQL
        pass


    @run_as_sync
    async def test_update_non_existent_cohort(self):
        #test if the query returns the correct resultset when multiple rows satisfies the criteria
        #TODO test API
        pass

    @run_as_sync
    async def test_create_custom_cohort_response(self):
        pass


class TestCohortGraphQL(DbIsolatedTest):
    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)



class TestCohortAPI(DbIsolatedTest):
    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)
