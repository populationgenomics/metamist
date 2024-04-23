from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

import metamist.model.cohort_body
import metamist.model.cohort_criteria
from metamist.models import CohortBody
from models.utils.cohort_template_id_format import cohort_template_id_format
from scripts.create_custom_cohort import get_cohort_spec, main


class TestCohortBuilder(DbIsolatedTest):
    """Test custom cohort builder script"""

    @run_as_sync
    async def setUp(self):
        super().setUp()

    @run_as_sync
    async def test_get_cohort_spec(self):
        """Test get_cohort_spec(), invoked by the creator script"""
        ctemplate_id = cohort_template_id_format(28)
        result = get_cohort_spec('My cohort', 'Describing the cohort', ctemplate_id)
        self.assertIsInstance(result, metamist.model.cohort_body.CohortBody)
        self.assertEqual(result.name, 'My cohort')
        self.assertEqual(result.description, 'Describing the cohort')
        self.assertEqual(result.template_id, ctemplate_id)

    @run_as_sync
    @patch('metamist.apis.CohortApi.create_cohort_from_criteria')
    async def test_empty_main(self, mock):
        """Test main with no criteria"""
        mock.return_value = {'cohort_id': 'COH1', 'sequencing_group_ids': ['SG1', 'SG2']}
        main(
            project='greek-myth',
            cohort_body_spec=CohortBody(name='Empty cohort', description='No criteria'),
            projects=None,
            sg_ids_internal=[],
            excluded_sg_ids=[],
            sg_technologies=[],
            sg_platforms=[],
            sg_types=[],
            sample_types=[],
            dry_run=False,
        )
        mock.assert_called_once()

    @run_as_sync
    @patch('metamist.apis.CohortApi.create_cohort_from_criteria')
    async def test_epic_main(self, mock):
        """Test"""
        mock.return_value = {'cohort_id': 'COH2', 'sequencing_group_ids': ['SG3']}
        main(
            project='greek-myth',
            cohort_body_spec=CohortBody(name='Epic cohort', description='Every criterion'),
            projects=['alpha', 'beta'],
            sg_ids_internal=['SG3'],
            excluded_sg_ids=['SG1', 'SG2'],
            sg_technologies=['short-read'],
            sg_platforms=['illumina'],
            sg_types=['genome'],
            sample_types=['blood'],
            dry_run=False,
        )
        mock.assert_called_once()
        self.assertEqual(mock.call_args.kwargs['project'], 'greek-myth')

        body = mock.call_args.kwargs['body_create_cohort_from_criteria']
        spec = body['cohort_spec']
        self.assertEqual(spec.name, 'Epic cohort')
        self.assertEqual(spec.description, 'Every criterion')

        criteria = body['cohort_criteria']
        self.assertIsInstance(criteria, metamist.model.cohort_criteria.CohortCriteria)
        self.assertListEqual(criteria.projects, ['alpha', 'beta'])
        self.assertListEqual(criteria.sg_ids_internal, ['SG3'])
        self.assertListEqual(criteria.excluded_sgs_internal, ['SG1', 'SG2'])
        self.assertListEqual(criteria.sg_technology, ['short-read'])
        self.assertListEqual(criteria.sg_platform, ['illumina'])
        self.assertListEqual(criteria.sg_type, ['genome'])
        self.assertListEqual(criteria.sample_types, ['blood'])

        self.assertFalse(body['dry_run'])
