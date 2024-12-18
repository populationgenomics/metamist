# pylint: disable=ungrouped-imports

from db.python.filters.generic import GenericFilter
from db.python.layers.cohort import CohortLayer
from db.python.tables.cohort import CohortFilter
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortCriteriaInternal
from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.enums import AnalysisStatus
from api.graphql.mutations.analysis import AnalysisStatusType

# region ANALYSIS MUTATIONS

CREATE_ANALYSIS_MUTATION = """
    mutation createAnalysis($project: String!, $sequencingGroupIds: [String!], $status: AnalysisStatus!, $type: String!) {
        analysis {
            createAnalysis(project: $project, analysis: {
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
    mutation updateAnalysis($analysisId: Int!, $status: AnalysisStatus!, $meta: JSON) {
        analysis {
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

# endregion ANALYSIS MUTATIONS

# region ASSAY MUTATIONS

CREATE_ASSAY_MUTATION = """
    mutation createAssay($type: String!, $meta: JSON, $externalIds: JSON, $sampleId: String) {
        assay {
            createAssay(assay: {
                type: $type,
                meta: $meta,
                externalIds: $externalIds,
                sampleId: $sampleId,
            }){
                id
                type
                meta
                externalIds
                sample {
                    id
                }
            }
        }
    }
"""

UPDATE_ASSAY_MUTATION = """
    mutation updateAssay($assayId: Int!, $type: String!, $meta: JSON, $externalIds: JSON, $sampleId: String) {
        assay {
            updateAssay(assay: {
                id: $assayId,
                type: $type,
                meta: $meta,
                externalIds: $externalIds,
                sampleId: $sampleId,
            }) {
                id
                type
                meta
                externalIds
                sample {
                    id
                }
            }
        }
    }
"""

# endregion ASSAY MUTATIONS

# region COHORT MUTATIONS

CREATE_COHORT_FROM_CRITERIA_MUTATION = """
    mutation CreateCohortFromCriteria($project: String!, $cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput!, $dryRun: Boolean) {
        cohort{
            createCohortFromCriteria(
                project: $project
                cohortSpec: $cohortSpec
                cohortCriteria: $cohortCriteria
                dryRun: $dryRun
            ) {
                id
                name
                description
                author
            }
        }
    }
"""

# endregion COHORT MUTATIONS


class TestMutations(DbIsolatedTest):
    """Test sample class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()  # type: ignore [call-arg]
        self.cl = CohortLayer(self.connection)
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
        self.external_sample_id = sample.to_external().id
        self.genome_sequencing_group_id = sample.sequencing_groups[0].id  # type: ignore [arg-type]
        self.genome_sequencing_group_id_external = (
            sample.sequencing_groups[0].to_external().id  # type: ignore [arg-type]
        )
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id  # type: ignore [arg-type]
        self.exome_sequencing_group_id_external = (
            sample.sequencing_groups[self.project_id].to_external().id  # type: ignore [arg-type]
        )

    # region ANALYSIS TESTS

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

    # endregion ANALYSIS TESTS

    # region ASSAY TESTS
    @run_as_sync
    async def test_create_assay(self):
        """Test creating an assay using the mutation and the API"""
        default_sequencing_meta = {
            'sequencing_type': 'genome',
            'sequencing_platform': 'short-read',
            'sequencing_technology': 'illumina',
        }
        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_ASSAY_MUTATION,
                variables={
                    'project': self.project_name,
                    'externalIds': {'test1': 'test1'},
                    'sampleId': self.external_sample_id,
                    'type': 'sequencing',
                    'meta': {'test': 'test', **default_sequencing_meta},
                },
            )
        )['assay']['createAssay']

        api_result = (
            await self.asl.upsert_assay(
                assay=AssayUpsertInternal(
                    type='sequencing',
                    meta={'test': 'test', **default_sequencing_meta},
                    external_ids={'test': 'test'},
                    sample_id=self.sample_id,
                ),
            )
        ).to_external()

        self.assertEqual(
            api_result.type,
            mutation_result['type'],
        )
        self.assertEqual(
            api_result.meta,
            mutation_result['meta'],
        )
        self.assertEqual(
            api_result.sample_id,
            mutation_result['sample']['id'],
        )

    @run_as_sync
    async def test_update_assay(self):
        """Test updating an assay using the mutation and the API"""

        default_sequencing_meta = {
            'sequencing_type': 'genome',
            'sequencing_platform': 'short-read',
            'sequencing_technology': 'illumina',
        }

        assay_id = (
            (
                await self.asl.upsert_assay(
                    assay=AssayUpsertInternal(
                        type='sequencing',
                        meta={'test': 'test', **default_sequencing_meta},
                        external_ids={'test': 'test'},
                        sample_id=self.sample_id,
                    ),
                )
            )
            .to_external()
            .id
        )

        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_ASSAY_MUTATION,
                variables={
                    'project': self.project_name,
                    'assayId': assay_id,
                    'type': 'sequencing',
                    'meta': {'test': 'test2', **default_sequencing_meta},
                },
            )
        )['assay']['updateAssay']

        await self.asl.upsert_assay(
            AssayUpsertInternal(
                id=assay_id,  # type: ignore [arg-type]
                type='sequencing',
                meta={'test': 'test2', **default_sequencing_meta},
            )
        )

        api_result = (await self.asl.get_assay_by_id(assay_id)).to_external()  # type: ignore [arg-type]

        self.assertEqual(api_result.type, mutation_result['type'])
        self.assertEqual(api_result.meta, mutation_result['meta'])
        self.assertEqual(api_result.sample_id, mutation_result['sample']['id'])
        self.assertEqual(api_result.external_ids, mutation_result['externalIds'])

    # endregion ASSAY TESTS

    # region COHORT TESTS
    @run_as_sync
    async def test_create_cohort_from_criteria(self):
        """Test creating a cohort from criteria using the mutation and the API"""

        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_COHORT_FROM_CRITERIA_MUTATION,
                variables={
                    'project': self.project_name,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'TestCohortDescription',
                        # 'templateId': cohort_template_id_format(tid),
                    },
                    'cohortCriteria': {
                        'projects': [self.project_name],
                        'sgIdsInternal': [self.genome_sequencing_group_id_external],
                        'excludedSgsInternal': [
                            self.exome_sequencing_group_id_external
                        ],
                        'sgTechnology': ['short-read'],
                        'sgPlatform': ['illumina'],
                        'sgType': ['genome'],
                        'sampleType': ['blood'],
                    },
                },
            )
        )['cohort']['createCohortFromCriteria']
        cohort = await self.cl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='TestCohortDescription',
            cohort_name='TestCohort2',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
                sg_ids_internal_raw=[self.genome_sequencing_group_id],  # type: ignore [arg-type]
                excluded_sgs_internal_raw=[self.exome_sequencing_group_id],  # type: ignore [arg-type]
                sg_technology=['short-read'],
                sg_platform=['illumina'],
                sg_type=['genome'],
                sample_type=['blood'],
            ),
        )
        api_result = (
            await self.cl.query(CohortFilter(id=GenericFilter(eq=cohort.cohort_id)))
        )[0]
        self.assertEqual(api_result.description, mutation_result['description'])
        self.assertEqual(api_result.author, mutation_result['author'])

    # endregion COHORT TESTS
