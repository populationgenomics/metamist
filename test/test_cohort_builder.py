from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

import api.routes.cohort
import metamist.models
from db.python.layers import SampleLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortBody, CohortCriteria, NewCohort
from models.utils.cohort_template_id_format import cohort_template_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_format
from scripts.create_custom_cohort import get_cohort_spec, main


class TestCohortBuilderBasic(DbIsolatedTest):
    """Test basic functionality for the custom cohort builder script"""

    @run_as_sync
    async def setUp(self):
        super().setUp()

    @run_as_sync
    async def mock_ccfc(self, project, body_create_cohort_from_criteria):
        """Mock by directly calling the API route"""
        self.assertEqual(project, self.project_name)
        return await api.routes.cohort.create_cohort_from_criteria(
            CohortBody(**body_create_cohort_from_criteria['cohort_spec'].to_dict()),
            CohortCriteria(
                **body_create_cohort_from_criteria['cohort_criteria'].to_dict()
            ),
            self.connection,
            body_create_cohort_from_criteria['dry_run'],
        )

    @run_as_sync
    async def test_get_cohort_spec(self):
        """Test get_cohort_spec(), invoked by the creator script"""
        ctemplate_id = cohort_template_id_format(28)
        result = get_cohort_spec('My cohort', 'Describing the cohort', ctemplate_id)
        self.assertIsInstance(result, metamist.models.CohortBody)
        self.assertEqual(result.name, 'My cohort')
        self.assertEqual(result.description, 'Describing the cohort')
        self.assertEqual(result.template_id, ctemplate_id)

    @run_as_sync
    @patch('metamist.apis.CohortApi.create_cohort_from_criteria')
    async def test_build_empty_cohort(self, mock):
        """Test main with an empty cohort"""
        mock.side_effect = self.mock_ccfc
        with self.assertRaises(ValueError) as context:
            _ = main(
                project=self.project_name,
                cohort_body_spec=metamist.models.CohortBody(
                    name='Empty cohort', description='No criteria'
                ),
                projects=[self.project_name],
                sg_ids_internal=[],
                excluded_sg_ids=[],
                sg_technologies=[],
                sg_platforms=[],
                sg_types=[],
                sample_types=[],
                dry_run=False,
            )
        mock.assert_called_once()
        self.assertIn(
            'criteria resulted in no sequencing groups',
            str(context.exception),
        )


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


class TestCohortBuilderData(DbIsolatedTest):
    """Test functionality for the custom cohort builder script that requires data"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.samplel = SampleLayer(self.connection)

        self.sA = await self.samplel.upsert_sample(
            get_sample_model('A', 'blood', 'genome', 'short-read', 'illumina')
        )
        self.sB = await self.samplel.upsert_sample(
            get_sample_model('B', 'blood', 'genome', 'short-read', 'illumina')
        )
        self.sC = await self.samplel.upsert_sample(
            get_sample_model('C', 'blood', 'genome', 'short-read', 'illumina')
        )

        self.sgA = sequencing_group_id_format(self.sA.sequencing_groups[0].id)
        self.sgB = sequencing_group_id_format(self.sB.sequencing_groups[0].id)
        self.sgC = sequencing_group_id_format(self.sC.sequencing_groups[0].id)

    @run_as_sync
    async def mock_ccfc(self, project, body_create_cohort_from_criteria):
        """Mock by directly calling the API route"""
        self.assertEqual(project, self.project_name)
        return await api.routes.cohort.create_cohort_from_criteria(
            CohortBody(**body_create_cohort_from_criteria['cohort_spec'].to_dict()),
            CohortCriteria(
                **body_create_cohort_from_criteria['cohort_criteria'].to_dict()
            ),
            self.connection,
            body_create_cohort_from_criteria['dry_run'],
        )

    @run_as_sync
    @patch('metamist.apis.CohortApi.create_cohort_from_criteria')
    async def test_empty_main(self, mock):
        """Test main with no criteria other than project"""
        mock.side_effect = self.mock_ccfc

        result = main(
            project=self.project_name,
            cohort_body_spec=metamist.models.CohortBody(
                name='Test cohort', description='Project criteria'
            ),
            projects=[self.project_name],
            sg_ids_internal=[],
            excluded_sg_ids=[],
            sg_technologies=[],
            sg_platforms=[],
            sg_types=[],
            sample_types=[],
            dry_run=False,
        )
        mock.assert_called_once()
        self.assertIsInstance(result, NewCohort)
        self.assertIsInstance(result.cohort_id, str)
        self.assertListEqual(
            result.sequencing_group_ids, [self.sgA, self.sgB, self.sgC]
        )
        self.assertEqual(result.dry_run, False)

    @run_as_sync
    @patch('metamist.apis.CohortApi.create_cohort_from_criteria')
    async def test_epic_main(self, mock):
        """Test main with every criteria"""
        mock.side_effect = self.mock_ccfc
        result = main(
            project=self.project_name,
            cohort_body_spec=metamist.models.CohortBody(
                name='Epic cohort', description='Every criterion'
            ),
            projects=[self.project_name],
            sg_ids_internal=[self.sgA],
            excluded_sg_ids=[self.sgB],
            sg_technologies=['short-read'],
            sg_platforms=['illumina'],
            sg_types=['genome'],
            sample_types=['blood'],
            dry_run=False,
        )
        mock.assert_called_once()
        self.assertEqual(mock.call_args.kwargs['project'], self.project_name)

        body = mock.call_args.kwargs['body_create_cohort_from_criteria']
        spec = body['cohort_spec']
        self.assertEqual(spec.name, 'Epic cohort')
        self.assertEqual(spec.description, 'Every criterion')

        criteria = body['cohort_criteria']
        self.assertIsInstance(criteria, metamist.models.CohortCriteria)
        self.assertListEqual(criteria.projects, [self.project_name])
        self.assertListEqual(criteria.sg_ids_internal, [self.sgA])
        self.assertListEqual(criteria.excluded_sgs_internal, [self.sgB])
        self.assertListEqual(criteria.sg_technology, ['short-read'])
        self.assertListEqual(criteria.sg_platform, ['illumina'])
        self.assertListEqual(criteria.sg_type, ['genome'])
        self.assertListEqual(criteria.sample_type, ['blood'])

        self.assertFalse(body['dry_run'])

        self.assertIsInstance(result, NewCohort)
        self.assertListEqual(result.sequencing_group_ids, [self.sgA])
        self.assertEqual(result.dry_run, False)
