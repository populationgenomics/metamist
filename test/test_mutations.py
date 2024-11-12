# pylint: disable=ungrouped-imports

from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.enums import AnalysisStatus
from api.graphql.mutations.analysis import AnalysisStatusType

CREATE_ANALYSIS_MUTATION = """
    mutation createAnalysis($project: String!, $sequencingGroupIds: [String!], $status: AnalysisStatus!, $type: String!) {
        analysis(projectName: $project) {
            createAnalysis(analysis: {
                type: $type,
                status: $status,
                meta:{},
                sequencingGroupIds: $sequencingGroupIds,
            }){
                id
                status
                meta
                sequencingGroups {
                    id
                }
                type
            }
        }
    }
"""

UPDATE_ANALYSIS_MUTATION = """
    mutation updateAnalysis($project: String!, $analysisId: Int!, $status: AnalysisStatus!, $meta: JSON) {
        analysis(projectName: $project) {
            updateAnalysis(analysisId: $analysisId, analysis: {
                status: $status,
                meta: $meta,
            }) {
                id
                status
                meta
                sequencingGroups {
                    id
                }
                type
            }
        }
    }
"""


class TestMutations(DbIsolatedTest):
    """Test sample class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()  # type: ignore [call-arg]
        self.sl = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)
        self.pl = ParticipantLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'genome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'exome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                ],
            )
        )
        self.sample_id = sample.id
        self.genome_sequencing_group_id = sample.sequencing_groups[0].id  # type: ignore [arg-type]
        self.genome_sequencing_group_id_external = (
            sample.sequencing_groups[0].to_external().id  # type: ignore [arg-type]
        )
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id  # type: ignore [arg-type]
        self.exome_sequencing_group_id_external = (
            sample.sequencing_groups[self.project_id].to_external().id  # type: ignore [arg-type]
        )

    @run_as_sync
    async def test_create_analysis(self):
        """Test creating an analysis using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_ANALYSIS_MUTATION,
                variables={
                    'project': self.project_name,
                    'sequencingGroupIds': [self.genome_sequencing_group_id_external],
                    'status': AnalysisStatusType.UNKNOWN.name,
                    'type': 'analysis-runner',
                },
            )
        )['analysis']['createAnalysis']

        aid = await self.al.create_analysis(
            project=self.project_id,
            analysis=AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                meta={},
                sequencing_group_ids=[self.genome_sequencing_group_id],  # type: ignore [arg-type]
            ),
        )

        api_result = (await self.al.get_analysis_by_id(aid)).to_external()

        self.assertEqual(
            api_result.type,
            mutation_result['type'],
        )
        self.assertEqual(
            api_result.status.name,
            mutation_result['status'],
        )
        self.assertEqual(
            api_result.sequencing_group_ids,
            [s['id'] for s in mutation_result['sequencingGroups']],
        )
        self.assertEqual(
            api_result.meta,
            mutation_result['meta'],
        )

    @run_as_sync
    async def test_update_analysis(self):
        """Test updating an analysis using the mutation and the API"""

        analysis = await self.al.create_analysis(
            project=self.project_id,
            analysis=AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                meta={},
                sequencing_group_ids=[self.genome_sequencing_group_id],  # type: ignore [arg-type]
            ),
        )
        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_ANALYSIS_MUTATION,
                variables={
                    'project': self.project_name,
                    'analysisId': analysis,
                    'status': AnalysisStatusType.COMPLETED.name,
                    'meta': {'test': 'test'},
                },
            )
        )['analysis']['updateAnalysis']

        await self.al.update_analysis(
            analysis_id=analysis,
            status=AnalysisStatus.COMPLETED,
            meta={'test': 'test'},
        )

        api_result = (await self.al.get_analysis_by_id(analysis)).to_external()

        self.assertEqual(api_result.status.name, mutation_result['status'])
        self.assertEqual(api_result.meta, mutation_result['meta'])
