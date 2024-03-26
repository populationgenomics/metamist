from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.layers import CohortLayer, SampleLayer
from db.python.utils import Forbidden, NotFoundError
from models.models import SampleUpsertInternal, SequencingGroupUpsertInternal
from models.models.cohort import CohortCriteria, CohortTemplate
from models.utils.sequencing_group_id_format import sequencing_group_id_format


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
                author='bob@example.org',
                description='No criteria or template',
                cohort_name='Broken cohort',
                dry_run=False,
            )

    @run_as_sync
    async def test_create_cohort_bad_project(self):
        """Can't create cohort in invalid project"""
        with self.assertRaises((Forbidden, NotFoundError)):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                author='bob@example.org',
                description='Cohort based on a missing project',
                cohort_name='Bad-project cohort',
                dry_run=False,
                cohort_criteria=CohortCriteria(projects=['nonexistent']),
            )

    @run_as_sync
    async def test_create_template_bad_project(self):
        """Can't create template in invalid project"""
        with self.assertRaises((Forbidden, NotFoundError)):
            _ = await self.cohortl.create_cohort_template(
                project=self.project_id,
                cohort_template=CohortTemplate(
                    name='Bad-project template',
                    description='Template based on a missing project',
                    criteria=CohortCriteria(projects=['nonexistent']),
                ),
            )

    @run_as_sync
    async def test_create_empty_cohort(self):
        """Create cohort from empty criteria"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with no entries',
            cohort_name='Empty cohort',
            dry_run=False,
            cohort_criteria=CohortCriteria(projects=['test']),
        )
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual([], result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_duplicate_cohort(self):
        """Can't create cohorts with duplicate names"""
        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with no entries',
            cohort_name='Trial duplicate cohort',
            dry_run=False,
            cohort_criteria=CohortCriteria(projects=['test']),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with no entries',
            cohort_name='Trial duplicate cohort',
            dry_run=True,
            cohort_criteria=CohortCriteria(projects=['test']),
        )

        with self.assertRaises(IntegrityError):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                author='bob@example.org',
                description='Cohort with no entries',
                cohort_name='Trial duplicate cohort',
                dry_run=False,
                cohort_criteria=CohortCriteria(projects=['test']),
            )

    @run_as_sync
    async def test_create_template_then_cohorts(self):
        """Test with template and cohort IDs out of sync, and creating from template"""
        tid = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplate(
                name='Empty template',
                description='Template with no entries',
                criteria=CohortCriteria(projects=['test']),
            ),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with no entries',
            cohort_name='Another empty cohort',
            dry_run=False,
            cohort_criteria=CohortCriteria(projects=['test']),
        )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort from template',
            cohort_name='Cohort from empty template',
            dry_run=False,
            template_id=tid,
        )


def get_sample_model(eid, s_type='blood', sg_type='genome', tech='short-read', plat='illumina'):
    """Create a minimal sample"""
    return SampleUpsertInternal(
        meta={},
        external_id=f'EXID{eid}',
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
        self.sC = await self.samplel.upsert_sample(get_sample_model('C', 'saliva', 'exome', 'long-read', 'ONT'))

        self.sgA = sequencing_group_id_format(self.sA.sequencing_groups[0].id)
        self.sgB = sequencing_group_id_format(self.sB.sequencing_groups[0].id)
        self.sgC = sequencing_group_id_format(self.sC.sequencing_groups[0].id)

    @run_as_sync
    async def test_create_cohort_by_sgs(self):
        """Create cohort by selecting sequencing groups"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with 1 SG',
            cohort_name='SG cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_ids_internal=[self.sgB],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual([self.sgB], result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_excluded_sgs(self):
        """Create cohort by excluding sequencing groups"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort without 1 SG',
            cohort_name='SG cohort 2',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                excluded_sgs_internal=[self.sgA],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual(2, len(result['sequencing_group_ids']))
        self.assertIn(self.sgB, result['sequencing_group_ids'])
        self.assertIn(self.sgC, result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_technology(self):
        """Create cohort by selecting a technology"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Short-read cohort',
            cohort_name='Tech cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_technology=['short-read'],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual(2, len(result['sequencing_group_ids']))
        self.assertIn(self.sgA, result['sequencing_group_ids'])
        self.assertIn(self.sgB, result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_platform(self):
        """Create cohort by selecting a platform"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='ONT cohort',
            cohort_name='Platform cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_platform=['ONT'],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual([self.sgC], result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_type(self):
        """Create cohort by selecting types"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Genome cohort',
            cohort_name='Type cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_type=['genome'],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual(2, len(result['sequencing_group_ids']))
        self.assertIn(self.sgA, result['sequencing_group_ids'])
        self.assertIn(self.sgB, result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_sample_type(self):
        """Create cohort by selecting sample types"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Sample cohort',
            cohort_name='Sample cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sample_type=['saliva'],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual([self.sgC], result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_everything(self):
        """Create cohort by selecting a variety of fields"""
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Everything cohort',
            cohort_name='Everything cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_ids_internal=[self.sgB, self.sgC],
                excluded_sgs_internal=[self.sgA],
                sg_technology=['short-read'],
                sg_platform=['illumina'],
                sg_type=['genome'],
                sample_type=['blood'],
            ),
        )
        self.assertEqual(1, len(result['sequencing_group_ids']))
        self.assertIn(self.sgB, result['sequencing_group_ids'])

    @run_as_sync
    async def test_reevaluate_cohort(self):
        """Add another sample, then reevaluate a cohort template"""
        template = await self.cohortl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplate(
                name='Boold template',
                description='Template selecting blood',
                criteria=CohortCriteria(
                    projects=['test'],
                    sample_type=['blood'],
                ),
            ),
        )

        coh1 = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Blood cohort',
            cohort_name='Blood cohort 1',
            dry_run=False,
            template_id=template,
        )
        self.assertEqual(2, len(coh1['sequencing_group_ids']))

        sD = await self.samplel.upsert_sample(get_sample_model('D'))
        sgD = sequencing_group_id_format(sD.sequencing_groups[0].id)

        coh2 = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Blood cohort',
            cohort_name='Blood cohort 2',
            dry_run=False,
            template_id=template,
        )
        self.assertEqual(3, len(coh2['sequencing_group_ids']))

        self.assertNotIn(sgD, coh1['sequencing_group_ids'])
        self.assertIn(sgD, coh2['sequencing_group_ids'])
