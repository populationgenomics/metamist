import pytest

import metamist.models

import api.routes.cohort
from db.python.connect import Connection
from db.python.layers import SampleLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortBody, CohortCriteria, NewCohort
from models.utils.cohort_template_id_format import cohort_template_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_format
from scripts.create_custom_cohort import get_cohort_spec


class TestCohortBuilderBasic:
    """Test basic functionality for the custom cohort builder script"""

    @pytest.mark.asyncio
    async def test_get_cohort_spec(self):
        """Test get_cohort_spec(), invoked by the creator script"""
        ctemplate_id = cohort_template_id_format(28)
        result = get_cohort_spec('My cohort', 'Describing the cohort', ctemplate_id)
        assert isinstance(result, metamist.models.CohortBody)
        assert result.name == 'My cohort'
        assert result.description == 'Describing the cohort'
        assert result.template_id == ctemplate_id

    @pytest.mark.asyncio
    async def test_build_empty_cohort(self, connection_with_project: Connection):
        """Test creating a cohort with no matching sequencing groups"""
        with pytest.raises(
            ValueError, match='criteria resulted in no sequencing groups'
        ):
            await api.routes.cohort.create_cohort_from_criteria(
                CohortBody(name='Empty cohort', description='No criteria'),
                CohortCriteria(projects=[connection_with_project.project.name]),
                connection_with_project,
                dry_run=False,
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


class TestCohortBuilderData:
    """Test cohort creation with sample data"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        self.connection = connection_with_project
        self.project_name = connection_with_project.project.name
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

    @pytest.mark.asyncio
    async def test_cohort_with_project_criteria(self):
        """Test creating a cohort with only project criteria"""
        result = await api.routes.cohort.create_cohort_from_criteria(
            CohortBody(name='Test cohort', description='Project criteria'),
            CohortCriteria(projects=[self.project_name]),
            self.connection,
            dry_run=False,
        )

        assert isinstance(result, NewCohort)
        assert isinstance(result.cohort_id, str)
        assert result.sequencing_group_ids == [self.sgA, self.sgB, self.sgC]
        assert result.dry_run is False

    @pytest.mark.asyncio
    async def test_cohort_with_all_criteria(self):
        """Test creating a cohort with all criteria specified"""
        result = await api.routes.cohort.create_cohort_from_criteria(
            CohortBody(name='Epic cohort', description='Every criterion'),
            CohortCriteria(
                projects=[self.project_name],
                excluded_sgs_internal=[self.sgB, self.sgC],
                sg_technology=['short-read'],
                sg_platform=['illumina'],
                sg_type=['genome'],
                sample_type=['blood'],
            ),
            self.connection,
            dry_run=False,
        )

        assert isinstance(result, NewCohort)
        assert result.sequencing_group_ids == [self.sgA]
        assert result.dry_run is False
