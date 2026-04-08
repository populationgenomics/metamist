import datetime
from random import randint

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import CohortLayer, SampleLayer, SequencingGroupLayer
from db.python.tables.cohort import CohortFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.enums.cohort import CohortStatus, CohortUpdateStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortCriteriaInternal, CohortUpdateBody
from models.utils.cohort_id_format import cohort_id_format
from models.utils.cohort_template_id_format import cohort_template_id_format
from test.conftest import GraphQLQueryFunction


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


class TestStatusInCohortDBLayer:
    """Test cohort status related functions implemented in the DB layer"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        self.cohort_layer = CohortLayer(connection_with_project)
        self.sample_layer = SampleLayer(connection_with_project)
        assert connection_with_project.project_id is not None
        self.project_id = connection_with_project.project_id
        self.sg_layer = SequencingGroupLayer(connection_with_project)
        self.connection = connection_with_project

        self.sample_a = await self.sample_layer.upsert_sample(
            get_sample_model('A', 'saliva', 'exome', 'ONT')
        )
        assert self.sample_a.sequencing_groups is not None
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
        assert self.cohort.cohort_id is not None
        self.cohort_id = self.cohort.cohort_id

    @pytest.mark.asyncio
    async def test_create_custom_cohort_and_verify_status(self):
        """
        Test to create a custom cohort and verify its status
        (Here the one created in the setup method is tested)
        """

        created_cohort_in_list = await self.cohort_layer.query(
            CohortFilter(id=GenericFilter(eq=self.cohort_id))
        )
        assert created_cohort_in_list
        assert len(created_cohort_in_list) == 1

        created_cohort = created_cohort_in_list[0]
        assert created_cohort.id == self.cohort_id
        assert created_cohort.description == self.cohort_description
        assert created_cohort.name == self.cohort_name
        assert created_cohort.status == CohortStatus.active

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_query_cohort_with_inactive_sample(self):
        """Test cohort status when inactive sample"""

        await self.sample_layer.upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.invalid

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_query_cohort_with_archived_sg(self):
        """Test cohort status when archived sequencing group"""

        await self.sg_layer.archive_sequencing_group(
            sequencing_group_id=self.sgA_raw[0]
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.invalid

    @pytest.mark.asyncio
    async def test_query_cohort_status_with_all_active(self, connection_with_project):
        """
        Test computed cohort status when sample/s active,
        sg/s not archived and cohort status is active in the DB"""

        assert self.sample_a.id is not None
        queried_sample = await self.sample_layer.get_by_id(sample_id=self.sample_a.id)
        assert queried_sample.active

        queried_sg_list = await self.sg_layer.query(
            SequencingGroupFilter(
                id=GenericFilter(in_=[self.sgA_raw[0], self.sgA_raw[1]])
            )
        )
        assert not queried_sg_list[0].archived
        assert not queried_sg_list[1].archived

        # query directly from the cohort table as the returned status is computed runtime based on sample, sg and cohort
        cohort_raw_entry = await (
            await connection_with_project.pg_connection.execute(
                t'SELECT status FROM cohort where id = {self.cohort_id}'
            )
        ).fetchone()
        assert cohort_raw_entry['status'] == CohortStatus.active.value

        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.active

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_query_cohort_with_at_least_one_inactive_sample(self):
        """Test cohort status when at least one sample is inactive"""

        sample_b = await self.sample_layer.upsert_sample(get_sample_model('B'))
        assert sample_b.sequencing_groups is not None
        sg_b_raw = [sg.id for sg in sample_b.sequencing_groups]

        new_cohort = self.cohort = await self.cohort_layer.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Sample cohort Test 2',
            cohort_name='Sample cohort 2',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
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
        assert cohort.status == CohortStatus.invalid

    @pytest.mark.asyncio
    async def test_query_cohort_with_archived_db_status(self):
        """Test computed cohort status when cohort is archived in the DB"""

        # directly update without using the cohort_db_layer
        await self.connection.pg_connection.execute(
            t'UPDATE cohort SET status = {CohortUpdateStatus.archived.value} WHERE id = {self.cohort_id}',
        )

        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.archived

    @pytest.mark.asyncio
    async def test_query_cohort_in_get_template_by_cohort_id(self):
        """Test template query for retrieved based on cohort id"""
        template = await self.cohort_layer.get_template_by_cohort_id(self.cohort_id)
        assert template


CREATE_COHORT_MUTATION = """
  mutation CreateCohortFromCriteria($project: String!, $cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput!, $dryRun: Boolean) {
      cohort {
        createCohortFromCriteria(
          project: $project
          cohortSpec: $cohortSpec
          cohortCriteria: $cohortCriteria
          dryRun: $dryRun
        ) {
         createdCohort
         {
            status
         }
        }
      }
    }
"""


class TestCohortStatusGraphQL:
    """Test cohort querying via GraphQL"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):

        self.cohort_layer = CohortLayer(connection_with_project)
        self.sample_layer = SampleLayer(connection_with_project)
        self.connection = connection_with_project
        self.sample_a = await self.sample_layer.upsert_sample(
            get_sample_model('A', 'saliva', 'exome', 'ONT')
        )
        self.cohort_name = 'Sample cohort'
        self.cohort_description = 'Sample cohort description'
        assert connection_with_project.project is not None
        assert connection_with_project.project_id is not None
        self.project_id = connection_with_project.project_id
        self.project_name = connection_with_project.project.name

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
        assert self.cohort.cohort_id is not None
        self.cohort_id = self.cohort.cohort_id
        self.cohort_id_formatted = cohort_id_format(self.cohort_id)

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_create_custom_cohort_response_for_status(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test status field in create_custom_cohort mutation response"""

        mutation_result = await graphql_query(
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

        cohort = mutation_result['data']['cohort']['createCohortFromCriteria'][
            'createdCohort'
        ]
        assert cohort['status'] == ACTIVE

    @pytest.mark.asyncio
    async def test_query_cohort_with_filter_by_id(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test status field in GraphQL query cohort (by id)"""

        query_cohort_incl_status = (
            await graphql_query(
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
        )['data']

        assert len(query_cohort_incl_status['cohorts']) == 1
        queried_cohort = query_cohort_incl_status['cohorts'][0]

        assert queried_cohort['name'] == self.cohort_name
        assert queried_cohort['description'] == self.cohort_description
        assert (
            queried_cohort['sequencingGroups'][0]['sample']['project']['name']
            == self.project_name
        )
        assert (
            queried_cohort['sequencingGroups'][1]['sample']['project']['name']
            == self.project_name
        )
        assert queried_cohort['status'] == ACTIVE

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_query_cohort_with_filter_status_eq(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort with filter by status (eq)"""

        query_cohort_filter_status_eq = """
                    query CohortQuery($cohort_status: CohortStatus!) {
                cohorts(status: {eq: $cohort_status}) {
                    name
                    status
                }
            }
        """

        query_cohort_status_eq = (
            await graphql_query(
                query_cohort_filter_status_eq,
                {'cohort_status': ACTIVE},
            )
        )['data']

        assert query_cohort_status_eq['cohorts']
        queried_cohort = query_cohort_status_eq['cohorts'][0]

        assert queried_cohort['name'] == self.cohort_name
        assert queried_cohort['status'] == ACTIVE

        # update cohort status and retrieve
        await self.cohort_layer.update_cohort(
            CohortUpdateBody(status=CohortUpdateStatus.archived), self.cohort_id
        )
        query_cohort_status_eq = (
            await graphql_query(
                query_cohort_filter_status_eq,
                {'cohort_status': ACTIVE},
            )
        )['data']
        assert not query_cohort_status_eq['cohorts']

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_query_cohort_with_filter_status_in(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort with filter by status (in)"""

        _ = (
            await graphql_query(
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
        )['data']['cohort']['createCohortFromCriteria']['createdCohort']

        query_cohort_filter_status_in = """
            query CohortQuery($cohort_status_list: [CohortStatus!]!) {
                cohorts(status: {in_: $cohort_status_list}) {
                    status
                }
            }
        """

        query_cohort_status_in = (
            await graphql_query(
                query_cohort_filter_status_in,
                {'cohort_status_list': [ACTIVE]},
            )
        )['data']

        assert len(query_cohort_status_in['cohorts']) == 2
        for cohort in query_cohort_status_in['cohorts']:
            assert cohort['status'] == ACTIVE

        # update cohort status and retrieve
        await self.cohort_layer.update_cohort(
            CohortUpdateBody(status=CohortUpdateStatus.archived), self.cohort_id
        )

        query_cohort_status_in = (
            await graphql_query(
                query_cohort_filter_status_in,
                {'cohort_status_list': [ACTIVE]},
            )
        )['data']
        assert len(query_cohort_status_in['cohorts']) == 1

    @pytest.mark.asyncio
    async def test_query_cohort_with_filter_status_nin(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort with filter by status (not in)"""

        query_cohort_status_nin = (
            await graphql_query(
                """
            query CohortQuery($cohort_status_list: [CohortStatus!]!) {
                cohorts(status: {nin: $cohort_status_list}) {
                    status
                }
            }
        """,
                {'cohort_status_list': [ACTIVE]},
            )
        )['data']
        assert not query_cohort_status_nin['cohorts']

    @pytest.mark.asyncio
    async def test_query_cohort_with_filter_status_criteria_not_defined(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort with filter by status (filter criteria is not one of eq, in or nin)"""

        query_cohorts = (
            await graphql_query(
                """
            query CohortQuery($cohort_status: CohortStatus!) {
                cohorts(status: {gt: $cohort_status}) {
                    status
                }
            }
        """,
                {'cohort_status': ACTIVE},
            )
        )['data']
        assert query_cohorts['cohorts']

    @pytest.mark.asyncio
    async def test_query_info_of_non_existent_cohort(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort by non-existent cohort id"""

        query_cohorts = (
            await graphql_query(
                """
            query CohortQuery($cohort_id: String!) {
                cohorts(id: {eq: $cohort_id}) {
                    status
                }
            }
        """,
                {'cohort_id': cohort_id_format(self.cohort_id + randint(1, 100))},
            )
        )['data']
        assert not query_cohorts['cohorts']

    @pytest.mark.asyncio
    async def test_query_cohort_with_invalid_status_filter_value(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL query cohort by non-existent cohort status"""

        response = await graphql_query(
            """
                query CohortQuery($cohort_status: CohortStatus!) {
                    cohorts(status: {eq: $cohort_status}) {
                    id
                    }
                }
            """,
            {'cohort_status': 'Dummy status'},
        )
        assert response['errors']
        assert response['data'] is None

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_update_cohort_fields(self, graphql_query: GraphQLQueryFunction):
        """Test GraphQL mutation for updating cohort fields"""

        new_name = 'Updated Llama'
        new_status = ARCHIVED
        new_description = 'Updated description'

        queried_cohort = (
            await graphql_query(
                """
            query CohortQuery($id: String!) {
                cohorts(id: {eq: $id}) {
                    status
                	name
                    id
                    description
                }
            }
        """,  # noqa: E101
                {'id': self.cohort_id_formatted},
            )
        )['data']['cohorts'][0]

        assert queried_cohort['name'] != new_name
        assert queried_cohort['status'] != ARCHIVED
        assert queried_cohort['description'] != new_description

        updated_cohort = (
            await graphql_query(
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
        )['data']['cohort']['updateCohort']

        assert updated_cohort['name'] == new_name
        assert updated_cohort['status'] == ARCHIVED
        assert updated_cohort['description'] == new_description

    @pytest.mark.asyncio
    async def test_update_cohort_immutable_fields(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating cohort fields with immutable fields (not allowed to update)"""

        with pytest.raises(TypeError):
            _ = await graphql_query(
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

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_update_cohort_fields_with_empty_input_body(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating with empty body"""

        response = await graphql_query(
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

        assert response['errors'] is not None
        assert response['data'] is None

    @pytest.mark.asyncio
    async def test_update_non_existent_cohort(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating cohort fields of non-existent cohort"""

        response = await graphql_query(
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
                'id': cohort_id_format(self.cohort_id + randint(1, 100)),
                'cohort': {'name': 'Test name change'},
            },
        )
        assert response['errors'] is not None
        assert response['data'] is None

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_update_status_of_invalid_cohort(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating status of an INVALID cohort"""

        await self.sample_layer.upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.invalid

        updated_cohort = (
            await graphql_query(
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
                    'id': self.cohort_id_formatted,
                    'cohort': {'status': ACTIVE},
                },
            )
        )['data']['cohort']['updateCohort']

        assert updated_cohort['status'] == INVALID

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_update_status_of_archived_cohort_with_archived_samples(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating status of an archived cohort with archived samples"""

        # directly update cohort DB status
        await self.connection.pg_connection.execute(
            t'UPDATE cohort SET status = {CohortUpdateStatus.archived.value} WHERE id = {self.cohort_id}'
        )
        await self.sample_layer.upsert_sample(
            SampleUpsertInternal(id=self.sample_a.id, active=False)
        )
        cohort = (
            await self.cohort_layer.query(
                CohortFilter(id=GenericFilter(eq=self.cohort_id))
            )
        )[0]
        assert cohort.status == CohortStatus.archived

        response = await graphql_query(
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
                'cohort': {'status': ACTIVE},
            },
        )

        assert response['errors'] is not None
        assert response['data'] is None

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_update_status_of_archived_cohort_with_active_samples(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test GraphQL mutation for updating status of an archived cohort with archived samples"""

        # directly update cohort DB status
        await self.connection.pg_connection.execute(
            t'UPDATE cohort SET status = {CohortUpdateStatus.archived} WHERE id = {self.cohort_id}'
        )

        updated_cohort = (
            await graphql_query(
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
                    'id': self.cohort_id_formatted,
                    'cohort': {'status': ACTIVE},
                },
            )
        )['data']['cohort']['updateCohort']

        assert updated_cohort['status'] == ACTIVE
