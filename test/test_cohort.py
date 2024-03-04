from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.layers import CohortLayer
from models.models.cohort import CohortCriteria


class TestCohort(DbIsolatedTest):
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
                cohort_name='Borken cohort',
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
            cohort_criteria=CohortCriteria(
                projects=['test']
                )
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
            cohort_criteria=CohortCriteria(projects=['test'])
            )

        _ = await self.cohortl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            author='bob@example.org',
            description='Cohort with no entries',
            cohort_name='Trial duplicate cohort',
            dry_run=True,
            cohort_criteria=CohortCriteria(projects=['test'])
            )

        with self.assertRaises(IntegrityError):
            _ = await self.cohortl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                author='bob@example.org',
                description='Cohort with no entries',
                cohort_name='Trial duplicate cohort',
                dry_run=False,
                cohort_criteria=CohortCriteria(projects=['test'])
                )
