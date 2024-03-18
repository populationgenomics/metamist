from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.layers import CohortLayer, SampleLayer
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


def get_sample_model(eid):
    """Create a minimal sample"""
    return SampleUpsertInternal(
        meta={},
        external_id=f'EXID{eid}',
        sequencing_groups=[
            SequencingGroupUpsertInternal(
                type='genome',
                technology='short-read',
                platform='illumina',
                meta={},
                assays=[],
            ),
        ],
    )


class TestCohortData(DbIsolatedTest):
    """Test custom cohort endpoints that need some sequencing groups already set up"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.cohortl = CohortLayer(self.connection)
        self.samplel = SampleLayer(self.connection)

        self.sA = await self.samplel.upsert_sample(get_sample_model('A'))
        self.sB = await self.samplel.upsert_sample(get_sample_model('B'))
        self.sC = await self.samplel.upsert_sample(get_sample_model('C'))

    @run_as_sync
    async def test_create_cohort_by_sgs(self):
        """Create cohort by selecting sequencing groups"""
        sgB = sequencing_group_id_format(self.sB.sequencing_groups[0].id)
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with 1 SG',
            cohort_name='SG cohort 1',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                sg_ids_internal=[sgB],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual([sgB], result['sequencing_group_ids'])

    @run_as_sync
    async def test_create_cohort_by_excluded_sgs(self):
        """Create cohort by excluding sequencing groups"""
        sgA = sequencing_group_id_format(self.sA.sequencing_groups[0].id)
        sgB = sequencing_group_id_format(self.sB.sequencing_groups[0].id)
        sgC = sequencing_group_id_format(self.sC.sequencing_groups[0].id)
        result = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort without 1 SG',
            cohort_name='SG cohort 2',
            dry_run=False,
            cohort_criteria=CohortCriteria(
                projects=['test'],
                excluded_sgs_internal=[sgA],
            ),
        )
        self.assertIsInstance(result['cohort_id'], str)
        self.assertEqual(2, len(result['sequencing_group_ids']))
        self.assertIn(sgB, result['sequencing_group_ids'])
        self.assertIn(sgC, result['sequencing_group_ids'])
