import pytest

from db.python.connect import Connection
from db.python.layers import (
    AssayLayer,
    ParticipantLayer,
    ProjectInsightsLayer,
    SampleLayer,
)
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    ProjectInsightsSummaryInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}


def get_test_participant():
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    )
                ],
            )
        ],
    )


class TestProjectInsights:
    """Test project insights class containing project insights endpoints"""

    maxDiff = None

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection) -> None:
        assert connection_with_project.project is not None
        self.partl = ParticipantLayer(connection_with_project)
        self.pil = ProjectInsightsLayer(connection_with_project)
        self.sampl = SampleLayer(connection_with_project)
        self.seql = AssayLayer(connection_with_project)
        self.project_name = connection_with_project.project.name
        assert connection_with_project.project_id is not None
        self.project_id = connection_with_project.project_id

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_project_insights_summary(self):
        """Test getting the summaries for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        result = await self.pil.get_project_insights_summary(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )

        expected = [
            ProjectInsightsSummaryInternal(
                project=self.project_id,
                dataset=self.project_name,  # for ProjectInsights, dataset is project.name
                sequencing_type='genome',
                sequencing_technology='short-read',
                total_families=0,
                total_participants=1,
                total_samples=1,
                total_sequencing_groups=1,
                total_crams=0,
                latest_annotate_dataset=None,
                latest_snv_es_index=None,
                latest_sv_es_index=None,
            ),
        ]

        assert result == expected

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_project_insights_details(self):
        """Test getting the details for all available projects"""

        await self.partl.upsert_participant(get_test_participant())

        # There's not enough data set up to usefully verify the result
        _ = await self.pil.get_project_insights_details(
            project_names=[self.project_name], sequencing_types=['genome', 'exome']
        )
