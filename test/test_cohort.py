import datetime

from pymysql.err import IntegrityError

from db.python.filters import GenericFilter
from db.python.layers import CohortLayer, SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.cohort import CohortFilter
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
from test.testbase import DbIsolatedTest, run_as_sync


class TestCohortBasic(DbIsolatedTest):
    """Test custom cohort endpoints"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)

    @run_as_sync
    async def test_create_cohort_missing_args(self):
        """Can't create cohort with neither criteria nor template"""
        with self.assertRaises(ValueError):
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

    @run_as_sync
    async def test_create_empty_cohort(self):
        """Create cohort from empty criteria"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with no entries',
            cohort_name='Empty cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
        )
        self.assertIsInstance(result, NewCohortInternal)
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual([], result.sequencing_group_ids)

    @run_as_sync
    async def test_create_duplicate_cohort(self):
        """Can't create cohorts with duplicate names"""
        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with no entries',
            cohort_name='Trial duplicate cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with no entries',
            cohort_name='Trial duplicate cohort',
            dry_run=True,
            cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
        )

        with self.assertRaises(IntegrityError):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Cohort with no entries',
                cohort_name='Trial duplicate cohort',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
            )

    @run_as_sync
    async def test_create_template_then_cohorts(self):
        """Test with template and cohort IDs out of sync, and creating from template"""
        tid = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='Empty template',
                description='Template with no entries',
                criteria=CohortCriteriaInternal(projects=[self.project_id]),
                project=self.project_id,
            ),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with no entries',
            cohort_name='Another empty cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(projects=[self.project_id]),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort from template',
            cohort_name='Cohort from empty template',
            dry_run=False,
            template_id=tid,
        )


class TestCohortQueries(DbIsolatedTest):
    """Test query-related custom cohort layer functions"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)

    @run_as_sync
    async def test_id_query(self):
        """Exercise querying id against an empty database"""
        result = await self.cohortl.query(CohortFilter(id=GenericFilter(eq=42)))
        self.assertEqual([], result)

    @run_as_sync
    async def test_name_query(self):
        """Exercise querying name against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(name=GenericFilter(eq='Unknown cohort'))
        )
        self.assertEqual([], result)

    @run_as_sync
    async def test_author_query(self):
        """Exercise querying author against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(author=GenericFilter(eq='Alan Smithee'))
        )
        self.assertEqual([], result)

    @run_as_sync
    async def test_template_id_query(self):
        """Exercise querying template_id against an empty database"""
        result = await self.cohortl.query(
            CohortFilter(template_id=GenericFilter(eq=28))
        )
        self.assertEqual([], result)

    @run_as_sync
    async def test_timestamp_query(self):
        """Exercise querying timestamp against an empty database"""
        new_years_day = datetime.datetime(2024, 1, 1)
        result = await self.cohortl.query(
            CohortFilter(timestamp=GenericFilter(eq=new_years_day))
        )
        self.assertEqual([], result)

    @run_as_sync
    async def test_project_query(self):
        """Exercise querying project against an empty database"""
        result = await self.cohortl.query(CohortFilter(project=GenericFilter(eq=37)))
        self.assertEqual([], result)


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


class TestCohortData(DbIsolatedTest):
    """Test custom cohort endpoints that need some sequencing groups already set up"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)
        self.samplel = SampleLayer(self.connection)

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

    @run_as_sync
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
        self.assertIsInstance(cc_internal, CohortCriteriaInternal)
        self.assertDictEqual(cc_internal.model_dump(), cc_internal_dict)

        cc_ext_trip = cc_internal.to_external(project_names=[self.project_name])
        self.assertIsInstance(cc_ext_trip, CohortCriteria)
        self.assertDictEqual(cc_ext_trip.model_dump(), cc_external_dict)

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
        self.assertIsInstance(ctpl_internal, CohortTemplateInternal)
        self.assertDictEqual(ctpl_internal.model_dump(), ctpl_internal_dict)

    @run_as_sync
    async def test_create_cohort_by_sgs(self):
        """Create cohort by selecting sequencing groups"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with 1 SG',
            cohort_name='SG cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.sgB_raw],
            ),
        )
        self.assertIsInstance(result, NewCohortInternal)
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual([self.sgB_raw], result.sequencing_group_ids)

        external = result.to_external()
        self.assertIsInstance(external, NewCohort)
        self.assertIsInstance(external.cohort_id, str)
        self.assertEqual(external.cohort_id, cohort_id_format(result.cohort_id))
        self.assertEqual([self.sgB], external.sequencing_group_ids)
        self.assertEqual(False, external.dry_run)

    @run_as_sync
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
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual(2, len(result.sequencing_group_ids))
        self.assertIn(self.sgB_raw, result.sequencing_group_ids)
        self.assertIn(self.sgC_raw, result.sequencing_group_ids)

    @run_as_sync
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
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual(2, len(result.sequencing_group_ids))
        self.assertIn(self.sgA_raw, result.sequencing_group_ids)
        self.assertIn(self.sgB_raw, result.sequencing_group_ids)

    @run_as_sync
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
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual([self.sgC_raw], result.sequencing_group_ids)

    @run_as_sync
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
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual(2, len(result.sequencing_group_ids))
        self.assertIn(self.sgA_raw, result.sequencing_group_ids)
        self.assertIn(self.sgB_raw, result.sequencing_group_ids)

    @run_as_sync
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
        self.assertIsInstance(result.cohort_id, int)
        self.assertEqual([self.sgC_raw], result.sequencing_group_ids)

    @run_as_sync
    async def test_create_cohort_by_everything(self):
        """Create cohort by selecting a variety of fields"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Everything cohort',
            cohort_name='Everything cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.sgB_raw, self.sgC_raw],
                excluded_sgs_internal_raw=[self.sgA_raw],
                sg_technology=['short-read'],
                sg_platform=['illumina'],
                sg_type=['genome'],
                sample_type=['blood'],
            ),
        )
        self.assertEqual(1, len(result.sequencing_group_ids))
        self.assertIn(self.sgB_raw, result.sequencing_group_ids)

    @run_as_sync
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
        self.assertEqual(2, len(coh1.sequencing_group_ids))

        sD = await self.samplel.upsert_sample(get_sample_model('D'))
        sgD_raw = sD.sequencing_groups[0].id

        coh2 = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Blood cohort',
            cohort_name='Blood cohort 2',
            dry_run=False,
            template_id=template,
        )
        self.assertEqual(3, len(coh2.sequencing_group_ids))

        self.assertNotIn(sgD_raw, coh1.sequencing_group_ids)
        self.assertIn(sgD_raw, coh2.sequencing_group_ids)

    @run_as_sync
    async def test_query_cohort(self):
        """Create a cohort and test that it is populated when queried"""
        created = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='Cohort with two samples',
            cohort_name='Duo cohort',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.sgA_raw, self.sgB_raw],
            ),
        )
        self.assertEqual(2, len(created.sequencing_group_ids))

        queried = await self.cohortl.query(
            CohortFilter(name=GenericFilter(eq='Duo cohort'))
        )
        self.assertEqual(1, len(queried))

        result = await self.cohortl.get_cohort_sequencing_group_ids(int(queried[0].id))
        self.assertEqual(2, len(result))
        self.assertIn(self.sA.sequencing_groups[0].id, result)
        self.assertIn(self.sB.sequencing_groups[0].id, result)


class TestCohortGraphql(DbIsolatedTest):
    """Test custom cohort endpoints that need some sequencing groups already set up"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)
        self.samplel = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)

    @run_as_sync
    async def test_cohort_with_archived_sgs(self):
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

        query_result_incl_archived = await self.run_graphql_query_async(
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

        incl_archived_cohort = query_result_incl_archived['cohorts'][0]
        self.assertEqual(incl_archived_cohort['name'], cohort_name)
        self.assertEqual(len(incl_archived_cohort['sequencingGroups']), 2)
        self.assertEqual(incl_archived_cohort['sequencingGroups'][0]['archived'], True)
        self.assertEqual(incl_archived_cohort['sequencingGroups'][1]['archived'], False)

        query_result_excl_archived = await self.run_graphql_query_async(
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

        excl_archived_cohort = query_result_excl_archived['cohorts'][0]
        self.assertEqual(len(excl_archived_cohort['sequencingGroups']), 1)
        self.assertEqual(excl_archived_cohort['sequencingGroups'][0]['archived'], False)
