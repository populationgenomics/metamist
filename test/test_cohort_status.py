import datetime
from random import randint

from db.python.filters import GenericFilter
from db.python.layers import CohortLayer, SampleLayer, SequencingGroupLayer
from db.python.tables.cohort import CohortFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from graphql.error import GraphQLError
from models.enums.cohort import CohortStatus, CohortUpdateStatus
from models.models.cohort import CohortCriteriaInternal, CohortUpdateBody
from models.utils.cohort_id_format import cohort_id_format
from models.utils.cohort_template_id_format import cohort_template_id_format
from test.testbase import DbIsolatedTest
from test.testbase import run_as_sync

from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


def get_sample_model(eid, s_type='blood', sg_type='genome', plat='illumina'):
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


ACTIVE = 'active'
INVALID = 'invalid'
ARCHIVED = 'archived'


class TestStatusInCohortDBLayer(DbIsolatedTest):
    """Test cohort status related functions implemented in the DB layer"""

    @run_as_sync
    async def setUp(self):
        super().setUp()

        self.cohort_layer = CohortLayer(self.connection)
        self.sample_layer = SampleLayer(self.connection)

        self.sample_a = await self.sample_layer.upsert_sample(
            get_sample_model('A', 'saliva', 'exome', 'ONT')
        )
        self.sgA_raw = [sg.id for sg in self.sample_a.sequencing_groups]

        self.cohort_name = 'Sample cohort 1'
        self.cohort_description = 'Sample cohort description'
        self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description=self.cohort_description,
            cohort_name=self.cohort_name,
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sample_type=['saliva'],
            ),
        )

    @run_as_sync
    async def test_create_custom_cohort_and_verify_status(self):
        """Test to create a custom cohort and verify its status
        (Here the one created in the setup method is tested)
        """

        created_cohort_in_list = await self.cohort_layer.query(
            CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
        )
        self.assertTrue(created_cohort_in_list)
        self.assertTrue(len(created_cohort_in_list) == 1)

        created_cohort = created_cohort_in_list[0]
        self.assertEqual(created_cohort.id, self.cohort.cohort_id)
        self.assertEqual(created_cohort.description, self.cohort_description)
        self.assertEqual(created_cohort.name, self.cohort_name)
        self.assertEqual(created_cohort.status, CohortStatus.active)

    @run_as_sync
    async def test_query_cohort_with_inactive_sample(self):
        """Test cohort status when inactive sample"""

        await self.sample_layer.upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.invalid)

    @run_as_sync
    async def test_query_cohort_with_archived_sg(self):
        """Test cohort status when archived sequencing group"""

        await (SequencingGroupLayer(self.connection)).archive_sequencing_group(
            sequencing_group_id=self.sgA_raw[0]
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.invalid)

    @run_as_sync
    async def test_query_cohort_status_with_all_active(self):
        """Test computed cohort status when sample/s active,
        sg/s not archived and cohort status is active in the DB"""

        queried_sample = await self.sample_layer.get_by_id(sample_id=self.sample_a.id)
        self.assertTrue(queried_sample.active)

        queried_sg_list = await (SequencingGroupLayer(self.connection)).query(
            SequencingGroupFilter(
                id=GenericFilter(in_=[self.sgA_raw[0], self.sgA_raw[1]])
            )
        )
        self.assertFalse(queried_sg_list[0].archived)
        self.assertFalse(queried_sg_list[1].archived)

        # query directly from the cohort table as the returned status is computed runtime based on sample, sg and cohort
        cohort_raw_entry = await self.connection.connection.fetch_one(
            'SELECT status FROM cohort where id = :cohort_id',
            {'cohort_id': self.cohort.cohort_id},
        )
        self.assertEqual(dict(cohort_raw_entry)['status'], CohortStatus.active.value)

        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.active)

    @run_as_sync
    async def test_query_cohort_with_at_least_one_inactive_sample(self):
        """Test cohort status when at least one sample is inactive"""

        sample_b = await self.sample_layer.upsert_sample(get_sample_model('B'))
        sg_b_raw = [sg.id for sg in sample_b.sequencing_groups]

        new_cohort = self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Sample cohort Test 2',
            cohort_name='Sample cohort 2',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.sgA_raw[0], sg_b_raw[0]],
            ),
        )
        await self.sample_layer.upsert_sample(
            SampleUpsertInternal(id=sample_b.id, active=False)
        )

        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=new_cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.invalid)

    @run_as_sync
    async def test_query_cohort_with_archived_db_status(self):
        """Test computed cohort status when cohort is archived in the DB"""

        # directly update without using the cohort_db_layer
        await self.connection.connection.fetch_one(
            'UPDATE cohort SET status = :status WHERE id = :cohort_id',
            {
                'cohort_id': self.cohort.cohort_id,
                'status': CohortUpdateStatus.archived.value,
            },
        )

        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.archived)

    @run_as_sync
    async def test_query_cohort_in_get_template_by_cohort_id(self):
        """Test template query for retrieved based on cohort id"""
        template = await self.cohort_layer.get_template_by_cohort_id(
            self.cohort.cohort_id
        )
        self.assertTrue(template)


CREATE_COHORT_MUTATION = """
  mutation CreateCohortFromCriteria($project: String!, $cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput!, $dryRun: Boolean) {
      cohort {
        createCohortFromCriteria(
          project: $project
          cohortSpec: $cohortSpec
          cohortCriteria: $cohortCriteria
          dryRun: $dryRun
        ) {
          status
        }
      }
    }
"""


class TestCohortStatusGraphQL(DbIsolatedTest):
    """Test cohort querying via GraphQL"""

    @run_as_sync
    async def setUp(self):
        super().setUp()

        self.cohort_layer = CohortLayer(self.connection)
        self.sample_a = await (SampleLayer(self.connection)).upsert_sample(
            get_sample_model('A', 'saliva', 'exome', 'ONT')
        )
        self.cohort_name = 'Sample cohort'
        self.cohort_description = 'Sample cohort description'

        self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description=self.cohort_description,
            cohort_name=self.cohort_name,
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sample_type=['saliva'],
            ),
        )
        self.cohort_id_formatted = cohort_id_format(self.cohort.cohort_id)

    @run_as_sync
    async def test_create_custom_cohort_response_for_status(self):
        """Test status field in create_custom_cohort mutation response"""

        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_COHORT_MUTATION,
                variables={
                    'project': self.project_name,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'TestCohortDescription',
                    },
                    'cohortCriteria': {
                        'projects': [self.project_name],
                        'sampleType': ['blood'],
                    },
                },
            )
        )['cohort']['createCohortFromCriteria']
        self.assertEqual(mutation_result['status'], ACTIVE)

    @run_as_sync
    async def test_query_cohort_with_filter_by_id(self):
        """Test status field in GraphQL query cohort (by id)"""

        query_cohort_incl_status = await self.run_graphql_query_async(
            """
            query CohortQuery($cohort_id: String!) {
                cohorts(id: {eq: $cohort_id}) {
                    name
                    status
                    description
                    sequencingGroups {
                        id
                        sample {
                            project {
                                name
                            }
                        }
                    }
                }
            }
        """,
            {'cohort_id': self.cohort_id_formatted},
        )

        self.assertEqual(len(query_cohort_incl_status['cohorts']), 1)
        queried_cohort = query_cohort_incl_status['cohorts'][0]

        self.assertEqual(queried_cohort['name'], self.cohort_name)
        self.assertEqual(queried_cohort['description'], self.cohort_description)

        self.assertEqual(
            queried_cohort['sequencingGroups'][0]['sample']['project']['name'],
            self.project_name,
        )
        self.assertEqual(
            queried_cohort['sequencingGroups'][1]['sample']['project']['name'],
            self.project_name,
        )
        self.assertEqual(queried_cohort['status'], ACTIVE)

    @run_as_sync
    async def test_query_cohort_with_filter_status_eq(self):
        """Test GraphQL query cohort with filter by status (eq)"""

        query_cohort_filter_status_eq = """
                    query CohortQuery($cohort_status: CohortStatus!) {
                cohorts(status: {eq: $cohort_status}) {
                    name
                    status
                }
            }
        """

        query_cohort_status_eq = await self.run_graphql_query_async(
            query_cohort_filter_status_eq,
            {'cohort_status': ACTIVE},
        )

        self.assertTrue(query_cohort_status_eq['cohorts'])
        queried_cohort = query_cohort_status_eq['cohorts'][0]

        self.assertEqual(queried_cohort['name'], self.cohort_name)
        self.assertEqual(queried_cohort['status'], ACTIVE)

        # update cohort status and retrieve
        await self.cohort_layer.update_cohort(
            CohortUpdateBody(status=CohortUpdateStatus.archived), self.cohort.cohort_id
        )
        query_cohort_status_eq = await self.run_graphql_query_async(
            query_cohort_filter_status_eq,
            {'cohort_status': ACTIVE},
        )
        self.assertFalse(query_cohort_status_eq['cohorts'])

    @run_as_sync
    async def test_query_cohort_with_filter_status_in(self):
        """Test GraphQL query cohort with filter by status (in)"""

        _ = (
            await self.run_graphql_query_async(
                CREATE_COHORT_MUTATION,
                variables={
                    'project': self.project_name,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'TestCohortDescription',
                    },
                    'cohortCriteria': {
                        'projects': [self.project_name],
                        'sampleType': ['blood'],
                    },
                },
            )
        )['cohort']['createCohortFromCriteria']

        query_cohort_filter_status_in = """
            query CohortQuery($cohort_status_list: [CohortStatus!]!) {
                cohorts(status: {in_: $cohort_status_list}) {
                    status
                }
            }
        """

        query_cohort_status_in = await self.run_graphql_query_async(
            query_cohort_filter_status_in,
            {'cohort_status_list': [ACTIVE]},
        )
        self.assertTrue(len(query_cohort_status_in['cohorts']) == 2)
        for cohort in query_cohort_status_in['cohorts']:
            self.assertEqual(cohort['status'], ACTIVE)

        # update cohort status and retrieve
        await self.cohort_layer.update_cohort(
            CohortUpdateBody(status=CohortUpdateStatus.archived), self.cohort.cohort_id
        )

        query_cohort_status_in = await self.run_graphql_query_async(
            query_cohort_filter_status_in,
            {'cohort_status_list': [ACTIVE]},
        )
        self.assertTrue(len(query_cohort_status_in['cohorts']) == 1)

    @run_as_sync
    async def test_query_cohort_with_filter_status_nin(self):
        """Test GraphQL query cohort with filter by status (not in)"""

        query_cohort_status_nin = await self.run_graphql_query_async(
            """
            query CohortQuery($cohort_status_list: [CohortStatus!]!) {
                cohorts(status: {nin: $cohort_status_list}) {
                    status
                }
            }
        """,
            {'cohort_status_list': [ACTIVE]},
        )
        self.assertFalse(query_cohort_status_nin['cohorts'])

    @run_as_sync
    async def test_query_cohort_with_filter_status_criteria_not_defined(self):
        """Test GraphQL query cohort with filter by status (filter criteria is not one of eq, in or nin)"""

        query_cohorts = await self.run_graphql_query_async(
            """
            query CohortQuery($cohort_status: CohortStatus!) {
                cohorts(status: {gt: $cohort_status}) {
                    status
                }
            }
        """,
            {'cohort_status': ACTIVE},
        )
        self.assertTrue(query_cohorts['cohorts'])

    @run_as_sync
    async def test_query_info_of_non_existent_cohort(self):
        """Test GraphQL query cohort by non-existent cohort id"""

        query_cohorts = await self.run_graphql_query_async(
            """
            query CohortQuery($cohort_id: String!) {
                cohorts(id: {eq: $cohort_id}) {
                    status
                }
            }
        """,
            {'cohort_id': cohort_id_format(self.cohort.cohort_id + randint(1, 100))},
        )
        self.assertFalse(query_cohorts['cohorts'])

    @run_as_sync
    async def test_query_cohort_with_invalid_status_filter_value(self):
        """Test GraphQL query cohort by non-existent cohort status"""

        with self.assertRaises(GraphQLError):
            _ = await self.run_graphql_query_async(
                """
                query CohortQuery($cohort_status: CohortStatus!) {
                    cohorts(status: {eq: $cohort_status}) {
                    }
                }
            """,
                {'cohort_status': 'Dummy status'},
            )

    @run_as_sync
    async def test_update_cohort_fields(self):
        """Test GraphQL mutation for updating cohort fields"""

        new_name = 'Updated Llama'
        new_status = ARCHIVED
        new_description = 'Updated description'

        queried_cohort = (
            await self.run_graphql_query_async(
                """
            query CohortQuery($id: String!) {
                cohorts(id: {eq: $id}) {
                    status
                	name
                    id
                    description
                }
            }
        """,
                {'id': self.cohort_id_formatted},
            )
        )['cohorts'][0]

        self.assertNotEqual(queried_cohort['name'], new_name)
        self.assertNotEqual(queried_cohort['status'], ARCHIVED)
        self.assertNotEqual(queried_cohort['description'], new_description)

        updated_cohort = (
            await self.run_graphql_query_async(
                """
                mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                {
                  cohort{
                    updateCohort(id:$id, cohort:$cohort){
                      id
                      name
                      description
                      status
                    }
                  }

                }
        """,
                {
                    'id': self.cohort_id_formatted,
                    'cohort': {
                        'name': new_name,
                        'status': new_status,
                        'description': new_description,
                    },
                },
            )
        )['cohort']['updateCohort']

        self.assertEqual(updated_cohort['name'], new_name)
        self.assertEqual(updated_cohort['status'], ARCHIVED)
        self.assertEqual(updated_cohort['description'], new_description)

    @run_as_sync
    async def test_update_cohort_immutable_fields(self):
        """Test GraphQL mutation for updating cohort fields with immutable fields (not allowed to update)"""

        with self.assertRaises(GraphQLError):
            _ = await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          id
                        }
                      }
                    }
            """,
                {
                    'id': self.cohort_id_formatted,
                    'cohort': {
                        'author': 'Test author update',
                        'timestamp': datetime.datetime.now(),
                        'template_id': cohort_template_id_format(randint(1, 100)),
                        'project': self.project_id,
                    },
                },
            )

    @run_as_sync
    async def test_update_cohort_fields_with_empty_input_body(self):
        """Test GraphQL mutation for updating with empty body"""

        with self.assertRaises(GraphQLError):
            _ = await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          id
                        }
                      }
                    }
            """,
                {
                    'id': self.cohort_id_formatted,
                    'cohort': {},
                },
            )

    @run_as_sync
    async def test_update_non_existent_cohort(self):
        """Test GraphQL mutation for updating cohort fields of non-existent cohort"""

        with self.assertRaises(GraphQLError):
            _ = await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          id
                        }
                      }
                    }
            """,
                {
                    'id': cohort_id_format(self.cohort.cohort_id + randint(1, 100)),
                    'cohort': {'name': 'Test name change'},
                },
            )

    @run_as_sync
    async def test_update_status_of_invalid_cohort(self):
        """Test GraphQL mutation for updating status of an INVALID cohort"""

        await SampleLayer(self.connection).upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.invalid)

        updated_cohort = (
            await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          status
                        }
                      }
                    }
            """,
                {
                    'id': cohort_id_format(self.cohort.cohort_id),
                    'cohort': {'status': ACTIVE},
                },
            )
        )['cohort']['updateCohort']

        self.assertEqual(updated_cohort['status'], INVALID)

    @run_as_sync
    async def test_update_status_of_archived_cohort_with_archived_samples(self):
        """Test GraphQL mutation for updating status of an archived cohort with archived samples"""

        # directly update cohort DB status
        await self.connection.connection.fetch_one(
            'UPDATE cohort SET status = :status WHERE id = :cohort_id',
            {
                'cohort_id': self.cohort.cohort_id,
                'status': CohortUpdateStatus.archived.value,
            },
        )
        await SampleLayer(self.connection).upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort.cohort_id))
            )
        )[0]
        self.assertEqual(cohort.status, CohortStatus.archived)

        with self.assertRaises(GraphQLError):
            _ = await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          id
                        }
                      }
                    }
            """,
                {
                    'id': cohort_id_format(self.cohort.cohort_id),
                    'cohort': {'status': ACTIVE},
                },
            )

    @run_as_sync
    async def test_update_status_of_archived_cohort_with_active_samples(self):
        """Test GraphQL mutation for updating status of an archived cohort with archived samples"""

        # directly update cohort DB status
        await self.connection.connection.fetch_one(
            'UPDATE cohort SET status = :status WHERE id = :cohort_id',
            {
                'cohort_id': self.cohort.cohort_id,
                'status': CohortUpdateStatus.archived,
            },
        )

        updated_cohort = (
            await self.run_graphql_query_async(
                """
                    mutation updateCohort($id : String!, $cohort: CohortUpdateBodyInput!)
                    {
                      cohort{
                        updateCohort(id:$id, cohort:$cohort){
                          status
                        }
                      }
                    }
            """,
                {
                    'id': cohort_id_format(self.cohort.cohort_id),
                    'cohort': {'status': ACTIVE},
                },
            )
        )['cohort']['updateCohort']

        self.assertEqual(updated_cohort['status'], ACTIVE)
