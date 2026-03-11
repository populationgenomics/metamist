import datetime
from random import randint

import pytest
from psycopg import IntegrityError

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import CohortLayer, SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.cohort import CohortFilter
from db.python.utils import to_db_json
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import (
    CohortCriteria,
    CohortCriteriaInternal,
    CohortTemplate,
    CohortTemplateInternal,
    NewCohort,
    NewCohortInternal,
)
from models.utils.cohort_id_format import cohort_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_format
from test.conftest import GraphQLQueryFunction


class TestCohortBasic:
    """Test custom cohort endpoints"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        self.cohortl = CohortLayer(connection_with_project)
        self.project_id = connection_with_project.project_id

    @pytest.mark.asyncio
    async def test_create_cohort_missing_args(self):
        """Can't create cohort with neither criteria nor template"""
        with pytest.raises(ValueError):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='No criteria or template',
                cohort_name='Broken cohort',
                dry_run=False,
            )

    # These tests are disabled because the move to an Internal Model means that verification happens in the route not the layer
    # @run_as_sync
    # async def test_create_cohort_bad_project(self):
    #     """Can't create cohort in invalid project"""
    #     with self.assertRaises((Forbidden, NotFoundError)):
    #         _ = await self.cohortl.create_cohort_from_criteria(
    #             project_to_write=self.project_id,
    #             description='Cohort based on a missing project',
    #             cohort_name='Bad-project cohort',
    #             dry_run=False,
    #             cohort_criteria=CohortCriteriaInternal(projects=[5]),
    #         )

    # @run_as_sync
    # async def test_create_template_bad_project(self):
    #     """Can't create template in invalid project"""
    #     with self.assertRaises((Forbidden, NotFoundError)):
    #         _ = await self.cohortl.create_cohort_template(
    #             project=self.project_id,
    #             cohort_template=CohortTemplate(
    #                 id=None,
    #                 name='Bad-project template',
    #                 description='Template based on a missing project',
    #                 criteria=CohortCriteria(projects=['nonexistent']),
    #             ),
    #         )

    @pytest.mark.asyncio
    async def test_create_empty_cohort(self):
        """Can't create cohorts from empty criteria"""
        with pytest.raises(ValueError) as context:
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Cohort with no entries',
                cohort_name='Empty cohort',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
            )

        assert 'criteria resulted in no sequencing groups' in str(context)


class TestCohortQueries:
    """Test query-related custom cohort layer functions"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection: Connection):
        self.cohortl = CohortLayer(connection)

    @pytest.mark.asyncio
    async def test_id_query(self):
        """Exercise querying id against an empty database"""
        result = await self.cohortl.query(CohortFilter(id=GenericFilter(eq=42)))
        assert result == []

    @pytest.mark.asyncio
    async def test_name_query(self):
        """Exercise querying name against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(name=GenericFilter(eq='Unknown cohort'))
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_author_query(self):
        """Exercise querying author against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(author=GenericFilter(eq='Alan Smithee'))
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_template_id_query(self):
        """Exercise querying template_id against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(template_id=GenericFilter(eq=28))
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_timestamp_query(self):
        """Exercise querying timestamp against an empty database"""
        new_years_day = datetime.datetime(2024, 1, 1)
        result = await self.cohortl.query(
            CohortFilter(timestamp=GenericFilter(eq=new_years_day))
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_project_query(self):
        """Exercise querying project against an empty database"""
        result = await self.cohortl.query(CohortFilter(project=GenericFilter(eq=37)))
        assert result == []


def get_sample_model(
    eid, s_type='blood', sg_type='genome', tech='short-read', plat='illumina'
):
    """Create a minimal sample"""
    return SampleUpsertInternal(
        meta={},
        external_ids={PRIMARY_EXTERNAL_ORG: f'EXID{eid}'},
        type=s_type,
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type=sg_type,
                technology=tech,
                platform=plat,
                meta={},
                assays=[],
            ),
        ],
    )


class TestCohortData:
    """Test custom cohort endpoints that need some sequencing groups already set up"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        self.cohortl = CohortLayer(connection_with_project)
        self.samplel = SampleLayer(connection_with_project)
        self.project_id = connection_with_project.project_id
        self.project_name = connection_with_project.project.name

        self.sA = await self.samplel.upsert_sample(get_sample_model('A'))
        self.sB = await self.samplel.upsert_sample(get_sample_model('B'))
        self.sC = await self.samplel.upsert_sample(
            get_sample_model('C', 'saliva', 'exome', 'long-read', 'ONT')
        )

        self.sgA = sequencing_group_id_format(self.sA.sequencing_groups[0].id)
        self.sgA_raw = self.sA.sequencing_groups[0].id
        self.sgB = sequencing_group_id_format(self.sB.sequencing_groups[0].id)
        self.sgB_raw = self.sB.sequencing_groups[0].id
        self.sgC = sequencing_group_id_format(self.sC.sequencing_groups[0].id)
        self.sgC_raw = self.sC.sequencing_groups[0].id

    @pytest.mark.asyncio
    async def test_internal_external(self):
        """Test to_internal() and to_external() methods"""
        cc_external_dict = {
            'projects': [self.project_name],
            'sg_ids_internal': [self.sgB, self.sgC],
            'excluded_sgs_internal': [self.sgA],
            'sg_technology': ['short-read'],
            'sg_platform': ['illumina'],
            'sg_type': ['genome'],
            'sample_type': ['blood'],
        }

        cc_internal_dict = {
            'projects': [self.project_id],
            'sg_ids_internal_raw': [self.sgB_raw, self.sgC_raw],
            'excluded_sgs_internal_raw': [self.sgA_raw],
            'sg_technology': ['short-read'],
            'sg_platform': ['illumina'],
            'sg_type': ['genome'],
            'sample_type': ['blood'],
        }

        cc_external = CohortCriteria(**cc_external_dict)
        cc_internal = cc_external.to_internal(projects_internal=[self.project_id])
        assert isinstance(cc_internal, CohortCriteriaInternal)
        assert cc_internal.model_dump() == cc_internal_dict

        cc_ext_trip = cc_internal.to_external(project_names=[self.project_name])
        assert isinstance(cc_ext_trip, CohortCriteria)
        assert cc_ext_trip.model_dump() == cc_external_dict

        ctpl_internal_dict = {
            'id': 496,
            'name': 'My template',
            'description': 'Testing template',
            'criteria': cc_internal_dict,
            'project': self.project_id,
        }

        ctpl_internal = CohortTemplate(
            id=496,
            name='My template',
            description='Testing template',
            criteria=cc_external,
        ).to_internal(
            criteria_projects=[self.project_id], template_project=self.project_id
        )
        assert isinstance(ctpl_internal, CohortTemplateInternal)
        assert ctpl_internal.model_dump() == ctpl_internal_dict

    @pytest.mark.asyncio
    async def test_create_cohort_by_sgs(self):
        """Create cohort by selecting sequencing groups"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with 1 SG',
            cohort_name='SG cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                sg_ids_internal_raw=[self.sgB_raw],
            ),
        )
        assert isinstance(result, NewCohortInternal)
        assert isinstance(result.cohort_id, int)
        assert [self.sgB_raw] == result.sequencing_group_ids

        external = result.to_external()
        assert isinstance(external, NewCohort)
        assert isinstance(external.cohort_id, str)
        assert external.cohort_id == cohort_id_format(result.cohort_id)
        assert [self.sgB] == external.sequencing_group_ids
        assert not external.dry_run

    @pytest.mark.asyncio
    async def test_create_cohort_by_sgs_fails_when_invalid_sg(self):
        """Create cohort with an invalid sg in the list"""
        random_sg_id = max(self.sgA_raw, self.sgB_raw, self.sgC_raw) + randint(1, 100)

        with pytest.raises(ValueError):
            await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Cohort with invalid SG',
                cohort_name='Test Cohort',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(
                    sg_ids_internal_raw=[random_sg_id, self.sgB_raw],
                ),
            )

    @pytest.mark.asyncio
    async def test_create_cohort_from_sgs_fails_when_no_projects(self):
        """Create cohort from a list of invalid sequencing group ids (without projects)"""
        random_sg_id_1 = max(self.sgA_raw, self.sgB_raw, self.sgC_raw) + randint(1, 100)
        with pytest.raises(ValueError):
            await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Cohort with 1 SG',
                cohort_name='SG cohort 1',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(
                    sg_ids_internal_raw=[random_sg_id_1, random_sg_id_1 + 1],
                ),
            )

    @pytest.mark.asyncio
    async def test_create_cohort_by_excluded_sgs(self):
        """Create cohort by excluding sequencing groups"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort without 1 SG',
            cohort_name='SG cohort 2',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                excluded_sgs_internal_raw=[self.sgA_raw],
            ),
        )
        assert isinstance(result.cohort_id, int)
        assert len(result.sequencing_group_ids) == 2
        assert self.sgB_raw in result.sequencing_group_ids
        assert self.sgC_raw in result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_cohort_by_technology(self):
        """Create cohort by selecting a technology"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Short-read cohort',
            cohort_name='Tech cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_technology=['short-read'],
            ),
        )
        assert isinstance(result.cohort_id, int)
        assert len(result.sequencing_group_ids) == 2
        assert self.sgA_raw in result.sequencing_group_ids
        assert self.sgB_raw in result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_cohort_by_platform(self):
        """Create cohort by selecting a platform"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='ONT cohort',
            cohort_name='Platform cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_platform=['ONT'],
            ),
        )
        assert isinstance(result.cohort_id, int)
        assert [self.sgC_raw] == result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_cohort_by_type(self):
        """Create cohort by selecting types"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Genome cohort',
            cohort_name='Type cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_type=['genome'],
            ),
        )
        assert isinstance(result.cohort_id, int)
        assert len(result.sequencing_group_ids) == 2
        assert self.sgA_raw in result.sequencing_group_ids
        assert self.sgB_raw in result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_cohort_by_sample_type(self):
        """Create cohort by selecting sample types"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Sample cohort',
            cohort_name='Sample cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sample_type=['saliva'],
            ),
        )
        assert isinstance(result.cohort_id, int)
        assert [self.sgC_raw] == result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_cohort_by_everything(self):
        """Create cohort by selecting a variety of fields"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Everything cohort',
            cohort_name='Everything cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                excluded_sgs_internal_raw=[self.sgA_raw],
                sg_technology=['short-read'],
                sg_platform=['illumina'],
                sg_type=['genome'],
                sample_type=['blood'],
            ),
        )
        assert len(result.sequencing_group_ids) == 1
        assert self.sgB_raw in result.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_duplicate_cohort(self):
        """Can't create cohorts with duplicate names"""
        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='A cohort to be duplicated',
            cohort_name='Trial duplicate cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
            ),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='A duplicate cohort',
            cohort_name='Trial duplicate cohort',
            dry_run=True,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
            ),
        )

        with pytest.raises(IntegrityError):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='A duplicate cohort',
                cohort_name='Trial duplicate cohort',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(
                    projects=[self.project_id],
                ),
            )

    @pytest.mark.asyncio
    async def test_create_template_then_cohorts(self):
        """Test with template and cohort IDs out of sync, and creating from template"""
        tid = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='Test template',
                description='A template from which cohorts are created',
                criteria=CohortCriteriaInternal(projects=[self.project_id]),
                project=self.project_id,
            ),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort from criteria',
            cohort_name='Another test cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort from template',
            cohort_name='Cohort from test template',
            dry_run=False,
            template_id=tid,
        )

    @pytest.mark.asyncio
    async def test_reevaluate_cohort(self):
        """Add another sample, then reevaluate a cohort template"""
        template = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='Blood template',
                description='Template selecting blood',
                criteria=CohortCriteriaInternal(
                    projects=[self.project_id],
                    sample_type=['blood'],
                ),
                project=self.project_id,
            ),
        )

        coh1 = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Blood cohort',
            cohort_name='Blood cohort 1',
            dry_run=False,
            template_id=template,
        )
        assert len(coh1.sequencing_group_ids) == 2

        sD = await self.samplel.upsert_sample(get_sample_model('D'))  # noqa: N806
        sgD_raw = sD.sequencing_groups[0].id  # noqa: N806

        coh2 = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Blood cohort',
            cohort_name='Blood cohort 2',
            dry_run=False,
            template_id=template,
        )
        assert len(coh2.sequencing_group_ids) == 3
        assert sgD_raw not in coh1.sequencing_group_ids
        assert sgD_raw in coh2.sequencing_group_ids

        assert sgD_raw not in coh1.sequencing_group_ids
        assert sgD_raw in coh2.sequencing_group_ids

    @pytest.mark.asyncio
    async def test_create_template_fail_when_other_criteria_with_sg_list(self):
        """Test template creation fails when other criteria with sg list"""
        with pytest.raises(ValueError):
            await self.cohortl.create_cohort_template(
                project=self.project_id,
                cohort_template=CohortTemplateInternal(
                    id=None,
                    name='Test template',
                    description='Template with sg criteria and other criteria',
                    criteria=CohortCriteriaInternal(
                        projects=[self.project_id], sg_ids_internal_raw=[self.sgB_raw]
                    ),
                    project=self.project_id,
                ),
            )

    @pytest.mark.asyncio
    async def test_create_template_with_sg_list(self):
        """Test template creation when sg list provided as only criterion"""
        new_template = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='Test template',
                description='Template with sg criterion only',
                criteria=CohortCriteriaInternal(sg_ids_internal_raw=[self.sgB_raw]),
                project=self.project_id,
            ),
        )
        assert new_template

    @pytest.mark.asyncio
    async def test_create_cohort_from_template_with_sg_list_and_other_criteria(
        self, connection_with_project: Connection
    ):
        """Test cohort creation from an invalid template having sg list with other criteria"""
        # create template directly in the db as this is not supported from API
        _query = """
            INSERT INTO cohort_template (name, description, criteria, project, audit_log_id)
            VALUES (%(name)s, %(description)s, %(criteria)s, %(project)s, %(audit_log_id)s) RETURNING id;
        """
        acur = await connection_with_project.pg_connection.execute(
            _query,
            {
                'name': 'Test template',
                'description': 'Test description',
                'criteria': to_db_json(
                    {
                        'sg_ids_internal_raw': [self.sgA_raw],
                        'projects': [self.project_id],
                    }
                ),
                'project': self.project_id,
                'audit_log_id': await connection_with_project.audit_log_id(),
            },
        )

        row = await acur.fetchone()
        assert row
        cohort_template_id = row['id']

        with pytest.raises(ValueError):
            await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Test description',
                cohort_name='Test cohort',
                dry_run=False,
                template_id=cohort_template_id,
            )

    @pytest.mark.asyncio
    async def test_query_cohort(self):
        """Create a cohort and test that it is populated when queried"""
        created = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with two samples',
            cohort_name='Duo cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                sg_ids_internal_raw=[self.sgA_raw, self.sgB_raw],
            ),
        )
        assert len(created.sequencing_group_ids) == 2

        queried = await self.cohortl.query(
            CohortFilter(name=GenericFilter(eq='Duo cohort'))
        )
        assert len(queried) == 1

        result = await self.cohortl.get_cohort_sequencing_group_ids(int(queried[0].id))
        assert len(result) == 2
        assert self.sA.sequencing_groups[0].id in result
        assert self.sB.sequencing_groups[0].id in result


class TestCohortGraphql:
    """Test custom cohort endpoints that need some sequencing groups already set up"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        self.cohortl = CohortLayer(connection_with_project)
        self.samplel = SampleLayer(connection_with_project)
        self.sgl = SequencingGroupLayer(connection_with_project)
        self.project_id = connection_with_project.project_id

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_cohort_with_archived_sgs(self, graphql_query: GraphQLQueryFunction):
        """Check that archived sequencing groups are shown by default in cohorts"""
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
                    type='exome',
                    technology='short-read',
                    platform='illumina',
                    meta={},
                    assays=[],
                ),
            ],
        )

        sample = await self.samplel.upsert_sample(sample_model)
        assert sample.sequencing_groups
        sg1 = sample.sequencing_groups[0].id
        sg2 = sample.sequencing_groups[1].id

        assert sg1, sg2

        cohort_name = 'Archive test cohort 1'
        await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Genome & Exome cohort',
            cohort_name=cohort_name,
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_type=['genome', 'exome'],
            ),
        )

        # Archive the first sequencing group
        await self.sgl.archive_sequencing_group(sg1)

        query_result_incl_archived = await graphql_query(
            """
            query Cohort($name: StrGraphQLFilter) {
                cohorts(name:$name) {
                    name
                    sequencingGroups {
                        id
                        archived
                    }
                }
            }
        """,
            {'name': {'eq': cohort_name}},
        )

        incl_archived_cohort = query_result_incl_archived['data']['cohorts'][0]
        assert incl_archived_cohort['name'] == cohort_name
        assert len(incl_archived_cohort['sequencingGroups']) == 2
        assert incl_archived_cohort['sequencingGroups'][0]['archived']
        assert not incl_archived_cohort['sequencingGroups'][1]['archived']

        query_result_excl_archived = await graphql_query(
            """
            query Cohort($name: StrGraphQLFilter, $active_only: BoolGraphQLFilter) {
                cohorts(name:$name) {
                    name
                    sequencingGroups(activeOnly: $active_only) {
                        id
                        archived
                    }
                }
            }
        """,
            {'name': {'eq': cohort_name}, 'active_only': {'eq': True}},
        )

        excl_archived_cohort = query_result_excl_archived['data']['cohorts'][0]
        assert len(excl_archived_cohort['sequencingGroups']) == 1
        assert not excl_archived_cohort['sequencingGroups'][0]['archived']
