import pytest

from api.graphql.mutations.analysis import AnalysisStatusType
from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.cohort import CohortLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.cohort import CohortFilter, CohortTemplateFilter
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortCriteriaInternal, CohortTemplateInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.project import ProjectMemberRole
from models.utils.cohort_template_id_format import cohort_template_id_transform_to_raw
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw
from test.conftest import GraphQLQueryFunction


GROUP_NAME_PROJECT_CREATORS = 'project-creators'

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
    mutation CreateCohortFromCriteria($project: String!, $cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput!, $dryRun: Boolean, $excludeIneligibleSgIdsInternal: Boolean) {
        cohort{
            createCohortFromCriteria(
                project: $project
                cohortSpec: $cohortSpec
                cohortCriteria: $cohortCriteria
                dryRun: $dryRun
                excludeIneligibleSgIdsInternal: $excludeIneligibleSgIdsInternal
            ) {
             createdCohort {
                id
                name
                description
                author
                sequencingGroups {
                    id
                }
             }
            }
        }
    }
"""

CREATE_COHORT_FROM_CRITERIA_MUTATION_WITH_EXCLUDE = """
    mutation CreateCohortFromCriteria($project: String!, $cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput!, $dryRun: Boolean, $excludeIneligibleSgIdsInternal: Boolean) {
        cohort{
            createCohortFromCriteria(
                project: $project
                cohortSpec: $cohortSpec
                cohortCriteria: $cohortCriteria
                dryRun: $dryRun
                excludeIneligibleSgIdsInternal: $excludeIneligibleSgIdsInternal
            ) {
             createdCohort {
                id
                sequencingGroups {
                    id
                }
             }
             excludedIneligibleSgIdsInternal
            }
        }
    }
"""

CREATE_COHORT_TEMPLATE_MUTATION = """
    mutation createCohortTemplate($project: String!, $template: CohortTemplateInput!) {
        cohort {
            createCohortTemplate(
                project: $project
                template: $template
            ) {
                id
                name
                description
                criteria
            }
        }
    }
"""


CREATE_COHORT_FROM_TEMPLATE_MUTATION = """
      mutation CreateCohortFromTemplate($project: String!, $cohortSpec: CohortBodyInput!, $dryRun: Boolean, $excludeIneligibleSgIdsInternal: Boolean) {
      cohort {
        createCohortFromCriteria(
          project: $project
          cohortSpec: $cohortSpec
          dryRun: $dryRun
          excludeIneligibleSgIdsInternal: $excludeIneligibleSgIdsInternal
        ) {
        createdCohort{
                  id
          template {
            id
          }
          sequencingGroups {
            id
          }
        }
        }
      }
    }
"""
# endregion COHORT MUTATIONS

# region FAMILY MUTATIONS
UPDATE_FAMILY_MUTATION = """
    mutation updateFamily($family: FamilyUpdateInput!) {
        family {
            updateFamily(family: $family) {
                id
                externalIds
                description
                codedPhenotype
            }
        }
    }
"""
# endregion FAMILY MUTATIONS

# region PARTICIPANT MUTATIONS
UPDATE_PARTICIPANT_MUTATION = """
    mutation updateParticipant($participantId: Int!, $participant: ParticipantUpsertInput!) {
        participant {
            updateParticipant(participantId: $participantId, participant: $participant) {
                id
                externalIds
                reportedSex
                reportedGender
                karyotype
                samples {
                    id
                    type
                    meta
                    externalIds
                }
            }
        }
    }
"""

UPSERT_PARTICIPANTS_MUTATION = """
    mutation upsertParticipants($project: String!, $participants: [ParticipantUpsertInput!]!) {
        participant {
            upsertParticipants(project: $project, participants: $participants) {
                id
                externalIds
                reportedSex
                reportedGender
                karyotype
                samples {
                    id
                    type
                    meta
                    externalIds
                }
            }
        }
    }
"""

UPDATE_PARTICIPANT_FAMILY_MUTATION = """
    mutation updateParticipantFamily($participantId: Int!, $oldFamilyId: Int!, $newFamilyId: Int!) {
        participant {
            updateParticipantFamily(participantId: $participantId, oldFamilyId: $oldFamilyId, newFamilyId: $newFamilyId) {
                familyId
                participantId
            }
        }
    }
"""
# endregion PARTICIPANT MUTATIONS

# region PROJECT MUTATIONS
CREATE_PROJECT_MUTATION = """
    mutation createProject($name: String!, $dataset: String!, $createTestProject: Boolean!) {
        project {
            createProject(name: $name, dataset: $dataset, createTestProject: $createTestProject) {
                id
                name
                dataset
                meta
            }
        }
    }
"""

UPDATE_PROJECT_MUTATION = """
    mutation updateProject($project: String!, $projectUpdateModel: JSON!) {
        project {
            updateProject(project: $project, projectUpdateModel: $projectUpdateModel) {
                id
                name
                dataset
                meta
            }
        }
    }
"""

UPDATE_PROJECT_MEMBERS_MUTATION = """
    mutation updateProjectMembers($project: String!, $members: [ProjectMemberUpdateInput!]!) {
        project {
            updateProjectMembers(project: $project, members: $members) {
                id
                name
                dataset
                meta
                roles
            }
        }
    }
"""
# endregion PROJECT MUTATIONS

# region SAMPLE MUTATIONS
CREATE_SAMPLE_MUTATION = """
    mutation createSample($project: String!, $sample: SampleUpsertInput!) {
        sample {
            createSample(project: $project, sample: $sample) {
                id
                externalIds
                type
                active
                meta
            }
        }
    }
"""

UPSERT_SAMPLES_MUTATION = """
    mutation upsertSamples($project: String!, $samples: [SampleUpsertInput!]!) {
        sample {
            upsertSamples(project: $project, samples: $samples) {
                id
                externalIds
                type
                active
                meta
            }
        }
    }
"""

UPDATE_SAMPLE_MUTATION = """
    mutation updateSample($sample: SampleUpsertInput!) {
        sample {
            updateSample(sample: $sample) {
                id
                externalIds
                type
                active
                meta
            }
        }
    }
"""
# endregion SAMPLE MUTATIONS

# region SEQUENCING GROUP MUTATIONS
UPDATE_SEQUENCING_GROUP_MUTATION = """
    mutation updateSequencingGroup($project: String!, $sequencingGroup: SequencingGroupMetaUpdateInput!) {
        sequencingGroup {
            updateSequencingGroup(project: $project, sequencingGroup: $sequencingGroup) {
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
# endregion SEQUENCING GROUP MUTATIONS


def get_test_sample() -> SampleUpsertInternal:
    """Util function to create sample data"""

    return SampleUpsertInternal(
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


class TestMutations:
    """Test sample class"""

    @pytest.fixture(autouse=True)
    async def setup(self, connection_with_project: Connection):
        self.cl = CohortLayer(connection_with_project)
        self.sl = SampleLayer(connection_with_project)
        self.sgl = SequencingGroupLayer(connection_with_project)
        self.asl = AssayLayer(connection_with_project)
        self.al = AnalysisLayer(connection_with_project)
        self.pl = ParticipantLayer(connection_with_project)
        self.fl = FamilyLayer(connection_with_project)
        self.ppt = ProjectPermissionsTable(connection_with_project)

        self.family_id = await self.fl.create_family(external_ids={'forg': 'FAM01'})
        self.family_id_2 = await self.fl.create_family(external_ids={'forg': 'FAM02'})

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
        self.exome_sequencing_group_id = sample.sequencing_groups[1].id  # type: ignore [arg-type]
        self.exome_sequencing_group_id_external = (
            sample.sequencing_groups[1].to_external().id  # type: ignore [arg-type]
        )
        self.participant = await self.pl.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
                reported_sex=2,
                samples=[sample],
            )
        )
        self.pat_id = (
            await self.pl.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_pat'}, reported_sex=1
                )
            )
        ).id
        self.mat_id = (
            await self.pl.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_mat'}, reported_sex=2
                )
            )
        ).id

        assert self.participant.id
        assert self.pat_id
        assert self.mat_id

        await self.pl.add_participant_to_family(
            family_id=self.family_id,
            participant_id=self.participant.id,
            paternal_id=self.pat_id,
            maternal_id=self.mat_id,
            affected=2,
        )
        self.project_id = connection_with_project.project_id
        self.project_name = connection_with_project.project.name
        self.connection = connection_with_project

    # region ANALYSIS TESTS

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_analysis(self, graphql_query: GraphQLQueryFunction):
        """Test creating an analysis using the mutation and the API"""
        res = await graphql_query(
            CREATE_ANALYSIS_MUTATION,
            variables={
                'project': self.project_name,
                'sequencingGroupIds': [self.genome_sequencing_group_id_external],
                'status': AnalysisStatusType.UNKNOWN.name,
                'type': 'analysis-runner',
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['analysis']['createAnalysis']

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

        assert api_result.type == mutation_result['type']
        assert api_result.status.name == mutation_result['status']
        assert api_result.sequencing_group_ids == [
            s['id'] for s in mutation_result['sequencingGroups']
        ]
        assert api_result.meta == mutation_result['meta']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_analysis(self, graphql_query: GraphQLQueryFunction):
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
        res = await graphql_query(
            UPDATE_ANALYSIS_MUTATION,
            variables={
                'analysisId': analysis,
                'status': AnalysisStatusType.COMPLETED.name,
                'meta': {'test': 'test'},
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['analysis']['updateAnalysis']

        await self.al.update_analysis(
            analysis_id=analysis,
            status=AnalysisStatus.COMPLETED,
            meta={'test': 'test'},
        )

        api_result = (await self.al.get_analysis_by_id(analysis)).to_external()

        assert api_result.status.name == mutation_result['status']
        assert api_result.meta == mutation_result['meta']

    # endregion ANALYSIS TESTS

    # region ASSAY TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_assay(self, graphql_query: GraphQLQueryFunction):
        """Test creating an assay using the mutation and the API"""
        default_sequencing_meta = {
            'sequencing_type': 'genome',
            'sequencing_platform': 'short-read',
            'sequencing_technology': 'illumina',
        }
        res = await graphql_query(
            CREATE_ASSAY_MUTATION,
            variables={
                'type': 'sequencing',
                'meta': {'test': 'test', **default_sequencing_meta},
                'externalIds': {'test1': 'test1'},
                'sampleId': self.external_sample_id,
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['assay']['createAssay']

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

        assert api_result.type == mutation_result['type']
        assert api_result.meta == mutation_result['meta']
        assert api_result.sample_id == mutation_result['sample']['id']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_assay(self, graphql_query: GraphQLQueryFunction):
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

        res = await graphql_query(
            UPDATE_ASSAY_MUTATION,
            variables={
                'assayId': assay_id,
                'type': 'sequencing',
                'meta': {'test': 'test2', **default_sequencing_meta},
                'externalIds': None,
                'sampleId': None,
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['assay']['updateAssay']

        await self.asl.upsert_assay(
            AssayUpsertInternal(
                id=assay_id,  # type: ignore [arg-type]
                type='sequencing',
                meta={'test': 'test2', **default_sequencing_meta},
            )
        )

        api_result = (await self.asl.get_assay_by_id(assay_id)).to_external()  # type: ignore [arg-type]

        assert api_result.type == mutation_result['type']
        assert api_result.meta == mutation_result['meta']
        assert api_result.sample_id == mutation_result['sample']['id']
        assert api_result.external_ids == mutation_result['externalIds']

    # endregion ASSAY TESTS

    # region COHORT TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_criteria(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test creating a cohort from criteria using the mutation and the API"""

        res = await graphql_query(
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
                    'excludedSgsInternal': [self.exome_sequencing_group_id_external],
                    'sgTechnology': ['short-read'],
                    'sgPlatform': ['illumina'],
                    'sgType': ['genome'],
                    'sampleType': ['blood'],
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['cohort']['createCohortFromCriteria'][
            'createdCohort'
        ]

        cohort = await self.cl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            description='TestCohortDescription',
            cohort_name='TestCohort2',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                projects=[self.project_id],
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
        assert api_result.description == mutation_result['description']
        assert api_result.author == mutation_result['author']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_template(self, graphql_query: GraphQLQueryFunction):
        """Test creating a cohort template"""

        res = await graphql_query(
            CREATE_COHORT_TEMPLATE_MUTATION,
            variables={
                'project': self.project_name,
                'template': {
                    'name': 'TestTemplate',
                    'description': 'TestCohortTemplateDescription',
                    'criteria': {
                        'projects': [self.project_name],
                        'excludedSgsInternal': [
                            self.exome_sequencing_group_id_external
                        ],
                        'sgTechnology': ['short-read'],
                        'sgPlatform': ['illumina'],
                        'sgType': ['genome'],
                        'sampleType': ['blood'],
                    },
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['cohort']['createCohortTemplate']

        template_id = await self.cl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='TestTemplate',
                description='TestCohortTemplateDescription',
                project=self.project_id,
                criteria=CohortCriteriaInternal(
                    projects=[self.project_id],
                    excluded_sgs_internal_raw=[self.exome_sequencing_group_id],  # type: ignore [arg-type]
                    sg_technology=['short-read'],
                    sg_platform=['illumina'],
                    sg_type=['genome'],
                    sample_type=['blood'],
                ),
            ),
        )
        api_result = (
            await self.cl.query_cohort_templates(
                CohortTemplateFilter(id=GenericFilter(eq=template_id))
            )
        )[0]
        assert api_result.description == mutation_result['description']
        assert (
            api_result.criteria.sample_type
            == mutation_result['criteria']['sample_type']
        )
        assert (
            api_result.criteria.sg_platform
            == mutation_result['criteria']['sg_platform']
        )
        assert (
            api_result.criteria.sg_technology
            == mutation_result['criteria']['sg_technology']
        )
        assert api_result.criteria.sg_type == mutation_result['criteria']['sg_type']
        assert api_result.name == mutation_result['name']

    # endregion COHORT TESTS

    # region FAMILY TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_family(self, graphql_query: GraphQLQueryFunction):
        """Test updating a family using the mutation and the API"""
        res = await graphql_query(
            UPDATE_FAMILY_MUTATION,
            variables={
                'family': {
                    'id': self.family_id,
                    'externalIds': {PRIMARY_EXTERNAL_ORG: 'test'},
                    'description': 'test_family',
                    'codedPhenotype': 'test_family_phenotype',
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['family']['updateFamily']

        await self.fl.update_family(
            id_=self.family_id,
            external_ids={'test': 'test'},
            description='test_family',
            coded_phenotype='test_family_phenotype',
        )

        api_result = (
            await self.fl.get_family_by_internal_id(self.family_id)
        ).to_external()  # type: ignore [arg-type]

        assert api_result.external_ids == mutation_result['externalIds']
        assert api_result.description == mutation_result['description']
        assert api_result.coded_phenotype == mutation_result['codedPhenotype']

    # endregion FAMILY TESTS

    # region PARTICIPANT TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_participant(self, graphql_query: GraphQLQueryFunction):
        """Test updating a participant using the mutation and the API"""
        res = await graphql_query(
            UPDATE_PARTICIPANT_MUTATION,
            variables={
                'participantId': self.participant.id,
                'participant': {
                    'id': self.participant.id,
                    'externalIds': {PRIMARY_EXTERNAL_ORG: 'test'},
                    'reportedSex': 2,
                    'reportedGender': 'female',
                    'karyotype': 'test_karyotype',
                    'samples': [
                        {
                            'id': self.external_sample_id,
                            'type': 'blood',
                            'meta': {'test': 'test'},
                            'externalIds': {'test': 'test'},
                        }
                    ],
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['participant']['updateParticipant']

        api_result = (
            await self.pl.upsert_participant(
                ParticipantUpsertInternal(
                    id=self.participant.id,
                    external_ids={PRIMARY_EXTERNAL_ORG: 'test'},
                    reported_sex=2,
                    reported_gender='female',
                    karyotype='test_karyotype',
                    samples=[
                        SampleUpsertInternal(
                            id=self.sample_id,
                            type='blood',
                            meta={'test': 'test'},
                            external_ids={'test': 'test'},
                        )
                    ],
                )
            )
        ).to_external()

        assert api_result.external_ids == mutation_result['externalIds']
        assert api_result.reported_sex == mutation_result['reportedSex']
        assert api_result.reported_gender == mutation_result['reportedGender']
        assert api_result.karyotype == mutation_result['karyotype']
        assert api_result.samples[0].id == mutation_result['samples'][0]['id']  # type: ignore [arg-type]

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_upsert_participants(self, graphql_query: GraphQLQueryFunction):
        """Test upserting a list of participants using the mutation and the API. This inserts a new participant and updates an existing one."""
        res = await graphql_query(
            UPSERT_PARTICIPANTS_MUTATION,
            variables={
                'project': self.project_name,
                'participants': [
                    {
                        'id': self.participant.id,
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'EX01'},
                        'reportedSex': 2,
                        'reportedGender': 'female',
                        'karyotype': 'test_karyotype',
                        'samples': [
                            {
                                'id': self.external_sample_id,
                                'type': 'blood',
                                'meta': {'test': 'test'},
                                'externalIds': {'test': 'test'},
                            }
                        ],
                    },
                    {
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'EX02_pat'},
                        'reportedSex': 1,
                        'reportedGender': 'female',
                        'karyotype': 'test_karyotype',
                        'samples': [
                            {
                                'id': self.external_sample_id,
                                'type': 'blood',
                                'meta': {'test': 'test'},
                                'externalIds': {'test': 'test'},
                            }
                        ],
                    },
                ],
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['participant']['upsertParticipants']

        api_result = await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
                    reported_sex=2,
                    samples=[
                        SampleUpsertInternal(
                            id=self.sample_id,
                            type='blood',
                            meta={'test': 'test'},
                            external_ids={'test': 'test'},
                        )
                    ],
                    id=self.participant.id,
                    reported_gender='female',
                    karyotype='test_karyotype',
                ),
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX03_pat'},
                    reported_sex=1,
                    samples=[
                        SampleUpsertInternal(
                            id=self.sample_id,
                            type='blood',
                            meta={'test': 'test'},
                            external_ids={'test': 'test'},
                        )
                    ],
                    reported_gender='female',
                    karyotype='test_karyotype',
                    id=None,
                ),
            ]
        )

        api_result = [p.to_external() for p in api_result]
        assert api_result[0].id == mutation_result[0]['id']
        assert api_result[0].external_ids == mutation_result[0]['externalIds']
        assert api_result[0].reported_sex == mutation_result[0]['reportedSex']
        assert api_result[0].reported_gender == mutation_result[0]['reportedGender']
        assert api_result[0].karyotype == mutation_result[0]['karyotype']

        assert api_result[1].reported_sex == mutation_result[1]['reportedSex']
        assert api_result[1].reported_gender == mutation_result[1]['reportedGender']
        assert api_result[1].karyotype == mutation_result[1]['karyotype']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_participant_family(self, graphql_query: GraphQLQueryFunction):
        """Test updating a participants family data"""
        res = await graphql_query(
            UPDATE_PARTICIPANT_FAMILY_MUTATION,
            variables={
                'participantId': self.participant.id,
                'oldFamilyId': self.family_id,
                'newFamilyId': self.family_id_2,
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['participant']['updateParticipantFamily']

        api_result = (
            await self.fl.get_family_participants_by_family_ids([self.family_id_2])
        )[self.family_id_2][0]

        assert api_result.individual_id == mutation_result['participantId']
        assert api_result.family_id == mutation_result['familyId']

    # endregion PARTICIPANT TESTS

    # region PROJECT TESTS
    @pytest.mark.asyncio
    @pytest.mark.admin_groups(['project-creators', 'members-admin'])
    @pytest.mark.project_roles(['writer'])
    async def test_create_project(self, graphql_query: GraphQLQueryFunction):
        """Test creating a project using the mutation and the API"""
        res = await graphql_query(
            CREATE_PROJECT_MUTATION,
            variables={
                'name': 'test_project',
                'dataset': 'test_dataset',
                'createTestProject': True,
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['project']['createProject']

        # projects need to refresh on connection as it is a different connection
        # to the one used by graphql
        await self.connection.refresh_projects()

        api_result = list(
            self.connection.get_and_check_access_to_projects_for_names(
                [mutation_result['name'], mutation_result['name'] + '-test'],
                allowed_roles={
                    ProjectMemberRole.project_admin,
                    ProjectMemberRole.writer,
                },
            )
        )

        assert api_result[0].name == mutation_result['name']
        assert api_result[0].dataset == mutation_result['dataset']
        assert api_result[0].meta == mutation_result['meta']

        assert api_result[1].name == mutation_result['name'] + '-test'
        assert api_result[1].dataset == mutation_result['dataset']
        assert api_result[1].meta == mutation_result['meta']

    @pytest.mark.asyncio
    @pytest.mark.admin_groups(['project-creators', 'members-admin'])
    @pytest.mark.project_roles(['writer'])
    async def test_update_project(self, graphql_query: GraphQLQueryFunction):
        """Test updating a project using the mutation and the API"""
        res = await graphql_query(
            CREATE_PROJECT_MUTATION,
            variables={
                'name': 'new_test_project',
                'dataset': 'test_dataset',
                'createTestProject': False,
            },
        )
        assert not res.get('errors')
        create_project_result = res['data']['project']['createProject']

        res2 = await graphql_query(
            UPDATE_PROJECT_MUTATION,
            variables={
                'project': create_project_result['name'],
                'projectUpdateModel': {
                    'meta': {'test': 'test'},
                },
            },
        )
        assert not res2.get('errors')
        mutation_result = res2['data']['project']['updateProject']

        await self.connection.refresh_projects()

        api_result = list(
            self.connection.get_and_check_access_to_projects_for_names(
                [mutation_result['name']],
                allowed_roles={
                    ProjectMemberRole.project_admin,
                    ProjectMemberRole.writer,
                },
            )
        )[0]

        assert api_result.name == mutation_result['name']
        assert api_result.dataset == mutation_result['dataset']
        assert api_result.meta == {'test': 'test'}

    @pytest.mark.asyncio
    @pytest.mark.admin_groups(['project-creators', 'members-admin'])
    @pytest.mark.project_roles(['writer'])
    async def test_update_project_members(self, graphql_query: GraphQLQueryFunction):
        """Test updating project members using the mutation and the API"""
        res = await graphql_query(
            CREATE_PROJECT_MUTATION,
            variables={
                'name': 'new_test_project2',
                'dataset': 'test_dataset',
                'createTestProject': False,
            },
        )
        assert not res.get('errors')
        create_project_result = res['data']['project']['createProject']

        res2 = await graphql_query(
            UPDATE_PROJECT_MEMBERS_MUTATION,
            variables={
                'project': create_project_result['name'],
                'members': [
                    {
                        'member': 'testuser',
                        'roles': ['reader', 'writer'],
                    }
                ],
            },
        )
        assert not res2.get('errors')
        mutation_result = res2['data']['project']['updateProjectMembers']

        # projects need to refresh on connection as it is a different connection
        # to the one used by graphql
        await self.connection.refresh_projects()

        api_result = list(
            self.connection.get_and_check_access_to_projects_for_names(
                [mutation_result['name']],
                allowed_roles={
                    ProjectMemberRole.project_member_admin,
                },
            )
        )[0]

        assert api_result.name == mutation_result['name']
        assert api_result.dataset == mutation_result['dataset']
        assert api_result.meta == mutation_result['meta']
        assert [role.value for role in api_result.roles] == mutation_result['roles']

    # endregion PROJECT TESTS

    # region SAMPLE TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_sample(self, graphql_query: GraphQLQueryFunction):
        """Test creating a sample using the mutation and the API"""
        res = await graphql_query(
            CREATE_SAMPLE_MUTATION,
            variables={
                'project': self.project_name,
                'sample': {
                    'type': 'blood',
                    'meta': {'test': 'test'},
                    'externalIds': {PRIMARY_EXTERNAL_ORG: 'Test10'},
                    'active': True,
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['sample']['createSample']

        api_result = await self.sl.get_sample_by_id(
            sample_id_transform_to_raw(mutation_result['id'])
        )

        assert api_result.type == 'blood'
        assert api_result.meta == {'test': 'test'}
        assert api_result.external_ids == {PRIMARY_EXTERNAL_ORG: 'Test10'}
        assert api_result.active is True

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_upsert_samples(self, graphql_query: GraphQLQueryFunction):
        """Test upserting a list of samples using the mutation and the API. This inserts a new sample and updates an existing one."""
        res = await graphql_query(
            UPSERT_SAMPLES_MUTATION,
            variables={
                'project': self.project_name,
                'samples': [
                    {
                        'id': self.external_sample_id,
                        'type': 'blood',
                        'meta': {'test': 'test'},
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'Test10'},
                        'active': True,
                    },
                    {
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'Test11'},
                        'type': 'saliva',
                        'meta': {'test': 'test'},
                        'active': True,
                    },
                ],
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['sample']['upsertSamples']

        api_result = await self.sl.get_samples_by(
            sample_ids=[sample_id_transform_to_raw(s['id']) for s in mutation_result]
        )

        assert len(api_result) == 2
        assert api_result[0].type == 'blood'
        assert api_result[0].external_ids == {PRIMARY_EXTERNAL_ORG: 'Test10'}
        assert api_result[0].active is True
        assert api_result[1].type == 'saliva'
        assert api_result[1].meta == {'test': 'test'}
        assert api_result[1].external_ids == {PRIMARY_EXTERNAL_ORG: 'Test11'}
        assert api_result[1].active is True

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_sample(self, graphql_query: GraphQLQueryFunction):
        """Test updating a sample using the mutation and the API"""
        res = await graphql_query(
            CREATE_SAMPLE_MUTATION,
            variables={
                'project': self.project_name,
                'sample': {
                    'type': 'blood',
                    'meta': {'test': 'test'},
                    'externalIds': {PRIMARY_EXTERNAL_ORG: 'Test10'},
                    'active': True,
                },
            },
        )
        assert not res.get('errors')
        create_sample_result = res['data']['sample']['createSample']

        res2 = await graphql_query(
            UPDATE_SAMPLE_MUTATION,
            variables={
                'sample': {
                    'id': create_sample_result['id'],
                    'type': 'saliva',
                    'meta': {'test': 'test'},
                    'externalIds': {PRIMARY_EXTERNAL_ORG: 'Test11'},
                    'active': True,
                },
            },
        )
        assert not res2.get('errors')
        mutation_result = res2['data']['sample']['updateSample']

        api_result = await self.sl.get_sample_by_id(
            sample_id_transform_to_raw(mutation_result['id'])
        )

        assert api_result.type == 'saliva'
        assert api_result.meta == {'test': 'test'}
        assert api_result.external_ids == {PRIMARY_EXTERNAL_ORG: 'Test11'}
        assert api_result.active is True

    # endregion SAMPLE TESTS

    # region SEQUENCING GROUP TESTS
    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_update_sequencing_group(self, graphql_query: GraphQLQueryFunction):
        """Test updating a sequencing group using the mutation and the API"""
        res = await graphql_query(
            UPDATE_SEQUENCING_GROUP_MUTATION,
            variables={
                'project': self.project_name,
                'sequencingGroup': {
                    'id': self.genome_sequencing_group_id_external,
                    'meta': {'test': 'test'},
                },
            },
        )
        assert not res.get('errors')
        mutation_result = res['data']['sequencingGroup']['updateSequencingGroup']

        api_result = await self.sgl.get_sequencing_group_by_id(
            sequencing_group_id_transform_to_raw(mutation_result['id'])
        )
        assert api_result.meta == {'test': 'test'}

    # endregion SEQUENCING GROUP TESTS


class TestCohortMutations:
    """Test class for new cohort mutations"""

    # pylint: disable=too-many-instance-attributes

    @pytest.fixture(autouse=True)
    async def setUp(self, connection_with_project: Connection) -> None:
        self.sl = SampleLayer(connection_with_project)
        self.cl = CohortLayer(connection_with_project)
        self.sgl = SequencingGroupLayer(connection_with_project)
        self.sample = await self.sl.upsert_sample(get_test_sample())
        self.project_id = connection_with_project.project_id
        self.project_name = connection_with_project.project.name
        self.genome_sequencing_group_id_1 = self.sample.sequencing_groups[0].id
        self.genome_sequencing_group_id_external_1 = (
            self.sample.sequencing_groups[0].to_external().id
        )
        self.genome_sequencing_group_id_2 = self.sample.sequencing_groups[1].id
        self.genome_sequencing_group_id_external_2 = (
            self.sample.sequencing_groups[1].to_external().id
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_sequencing_group_list(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test mutation and API to create a cohort from sequencing group criteria"""
        mutation_cohort = (
            await graphql_query(
                CREATE_COHORT_FROM_CRITERIA_MUTATION,
                variables={
                    'project': self.project_name,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'Create cohort from sequencing group criteria',
                    },
                    'cohortCriteria': {
                        'sgIdsInternal': [self.genome_sequencing_group_id_external_1],
                    },
                },
            )
        )['data']['cohort']['createCohortFromCriteria']['createdCohort']

        api_cohort = await self.cl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            cohort_name='TestCohort2',
            description='Create cohort from sequencing group criteria',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                sg_ids_internal_raw=[self.genome_sequencing_group_id_1],
            ),
        )
        api_result = (
            await self.cl.query(CohortFilter(id=GenericFilter(eq=api_cohort.cohort_id)))
        )[0]
        assert api_result.description == mutation_cohort['description']
        assert api_result.author == mutation_cohort['author']
        assert api_result.name != mutation_cohort['name']
        assert api_result.id != mutation_cohort['id']

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_criteria_with_archived_sg_and_exclude_true(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test mutation and API to create a cohort from sequencing group criteria with archived sgs and exclude archived sgs set to true"""
        await self.sgl.archive_sequencing_group(self.genome_sequencing_group_id_1)

        graphql_response = (
            await graphql_query(
                CREATE_COHORT_FROM_CRITERIA_MUTATION_WITH_EXCLUDE,
                variables={
                    'project': self.project_name,
                    'excludeIneligibleSgIdsInternal': True,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'Create cohort with an archived sequencing group',
                    },
                    'cohortCriteria': {
                        'sgIdsInternal': [
                            self.sample.sequencing_groups[0].to_external().id,
                            self.sample.sequencing_groups[1].to_external().id,
                        ],
                    },
                },
            )
        )['data']['cohort']['createCohortFromCriteria']

        mutation_cohort = graphql_response['createdCohort']
        excluded_ineligible_sg_ids_internal = graphql_response[
            'excludedIneligibleSgIdsInternal'
        ]

        api_cohort = await self.cl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            exclude_ineligible_sg_ids_internal=True,
            cohort_name='TestCohort2',
            description='Create cohort with an archived sequencing group',
            dry_run=False,
            cohort_criteria=CohortCriteriaInternal(
                sg_ids_internal_raw=[
                    self.genome_sequencing_group_id_1,
                    self.genome_sequencing_group_id_2,
                ]
            ),
        )
        assert mutation_cohort['sequencingGroups']
        assert api_cohort.sequencing_group_ids
        assert excluded_ineligible_sg_ids_internal
        assert (
            excluded_ineligible_sg_ids_internal[0]
            == self.genome_sequencing_group_id_external_1
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_criteria_fail_when_sg_archived_and_exclude_not_set(
        self, graphql_query: GraphQLQueryFunction
    ):
        """
        Test mutation and API to create a cohort from sequencing group criteria when sgs are archived
        (and exclude archived sgs not set)"""
        await self.sgl.archive_sequencing_group(self.genome_sequencing_group_id_1)

        result = await graphql_query(
            CREATE_COHORT_FROM_CRITERIA_MUTATION,
            variables={
                'project': self.project_name,
                'cohortSpec': {
                    'name': 'TestCohort1',
                    'description': 'Create cohort with an archived sequencing group',
                },
                'cohortCriteria': {
                    'sgIdsInternal': [
                        self.genome_sequencing_group_id_external_1,
                        self.genome_sequencing_group_id_external_2,
                    ],
                },
            },
        )

        assert (
            'Contains sequencing groups which are not active'
            in result['errors'][0]['message']
        )

        with pytest.raises(ValueError):
            await self.cl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                cohort_name='TestCohort2',
                description='Create cohort with an archived sequencing group',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(
                    sg_ids_internal_raw=[
                        self.genome_sequencing_group_id_1,
                        self.genome_sequencing_group_id_2,
                    ]
                ),
            )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_criteria_fail_when_sg_other_criteria(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test mutation and API to create a cohort from sequencing group criteria when other criteria are provided"""
        result = await graphql_query(
            CREATE_COHORT_FROM_CRITERIA_MUTATION,
            variables={
                'project': self.project_name,
                'cohortSpec': {
                    'name': 'TestCohort1',
                    'description': 'Create cohort with an sg list and other criteria',
                },
                'cohortCriteria': {
                    'sgIdsInternal': [self.genome_sequencing_group_id_external_1],
                    'projects': [self.project_name],
                },
            },
        )

        assert (
            'Other criteria not supported if sequencing group ids provided as a criterion'
            in result['errors'][0]['message']
        )

        with pytest.raises(ValueError):
            await self.cl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                description='Create cohort with an sg list and other criteria',
                cohort_name='TestCohort2',
                dry_run=False,
                cohort_criteria=CohortCriteriaInternal(
                    sg_ids_internal_raw=[self.genome_sequencing_group_id_1],
                    projects=[self.project_id],
                ),
            )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_template_with_archived_sg_and_exclude_set(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test mutation and API to create a cohort from a template with archived sg and excludeIneligibleSgIdsInternal set to True"""
        await self.sgl.archive_sequencing_group(self.genome_sequencing_group_id_1)

        template_id = (
            await graphql_query(
                CREATE_COHORT_TEMPLATE_MUTATION,
                variables={
                    'project': self.project_name,
                    'template': {
                        'name': 'TestTemplate',
                        'description': 'Cohort template with archived sequencing group',
                        'criteria': {
                            'sgIdsInternal': [
                                self.genome_sequencing_group_id_external_1,
                                self.genome_sequencing_group_id_external_2,
                            ],
                        },
                    },
                },
            )
        )['data']['cohort']['createCohortTemplate']['id']

        mutation_cohort = (
            await graphql_query(
                CREATE_COHORT_FROM_TEMPLATE_MUTATION,
                variables={
                    'project': self.project_name,
                    'excludeIneligibleSgIdsInternal': True,
                    'cohortSpec': {
                        'name': 'TestCohort1',
                        'description': 'Create cohort with an archived sequencing group',
                        'templateId': template_id,
                    },
                },
            )
        )['data']['cohort']['createCohortFromCriteria']['createdCohort']

        api_cohort = await self.cl.create_cohort_from_criteria(
            project_to_write=self.project_id,
            cohort_name='TestCohort2',
            description='Create cohort with an archived sequencing group',
            template_id=cohort_template_id_transform_to_raw(template_id),
            exclude_ineligible_sg_ids_internal=True,
            dry_run=False,
        )

        assert mutation_cohort['template']['id'] != template_id
        assert len(mutation_cohort['sequencingGroups']) == 1

        api_result = (
            await self.cl.query(CohortFilter(id=GenericFilter(eq=api_cohort.cohort_id)))
        )[0]
        assert len(api_cohort.sequencing_group_ids) == 1
        assert api_result.template_id != template_id

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_create_cohort_from_template_with_archived_sg_and_exclude_not_set(
        self, graphql_query: GraphQLQueryFunction
    ):
        """Test mutation and API to create a cohort from a template with archived sg and excludeIneligibleSgIdsInternal not set"""
        await self.sgl.archive_sequencing_group(self.genome_sequencing_group_id_1)

        template_id = (
            await graphql_query(
                CREATE_COHORT_TEMPLATE_MUTATION,
                variables={
                    'project': self.project_name,
                    'template': {
                        'name': 'TestTemplate',
                        'description': 'Template with archived sequencing group',
                        'criteria': {
                            'sgIdsInternal': [
                                self.genome_sequencing_group_id_external_1,
                                self.genome_sequencing_group_id_external_2,
                            ],
                        },
                    },
                },
            )
        )['data']['cohort']['createCohortTemplate']['id']

        result = await graphql_query(
            CREATE_COHORT_FROM_TEMPLATE_MUTATION,
            variables={
                'project': self.project_name,
                'cohortSpec': {
                    'name': 'TestCohort1',
                    'description': 'Create cohort with an archived sequencing group',
                    'templateId': template_id,
                },
            },
        )

        assert (
            'Contains sequencing groups which are not active.'
            in result['errors'][0]['message']
        )

        with pytest.raises(ValueError):
            await self.cl.create_cohort_from_criteria(
                project_to_write=self.project_id,
                cohort_name='TestCohort2',
                description='Create cohort with an archived sequencing group',
                template_id=cohort_template_id_transform_to_raw(template_id),
                dry_run=False,
            )
