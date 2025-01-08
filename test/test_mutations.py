# pylint: disable=ungrouped-imports, too-many-lines

from db.python.filters.generic import GenericFilter
from db.python.layers.cohort import CohortLayer
from db.python.layers.family import FamilyLayer
from db.python.tables.cohort import CohortFilter, CohortTemplateFilter
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import ProjectMemberRole
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.models.cohort import CohortCriteriaInternal, CohortTemplateInternal
from models.models.participant import ParticipantUpsertInternal
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw
from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.enums import AnalysisStatus
from api.graphql.mutations.analysis import AnalysisStatusType

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
        self.fl = FamilyLayer(self.connection)
        self.ppt = ProjectPermissionsTable(self.connection)

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
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id  # type: ignore [arg-type]
        self.exome_sequencing_group_id_external = (
            sample.sequencing_groups[self.project_id].to_external().id  # type: ignore [arg-type]
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

        await self.connection.connection.execute(
            f"""
            INSERT INTO group_member(group_id, member)
            SELECT id, '{self.author}'
            FROM `group` WHERE name IN('project-creators', 'members-admin')
            """
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

    @run_as_sync
    async def test_create_cohort_template(self):
        """Test creating a cohort template"""

        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_COHORT_TEMPLATE_MUTATION,
                variables={
                    'project': self.project_name,
                    'template': {
                        'name': 'TestTemplate',
                        'description': 'TestCohortTemplateDescription',
                        'criteria': {
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
                },
            )
        )['cohort']['createCohortTemplate']
        template_id = await self.cl.create_cohort_template(
            project=self.project_id,
            cohort_template=CohortTemplateInternal(
                id=None,
                name='TestTemplate',
                description='TestCohortTemplateDescription',
                project=self.project_id,
                criteria=CohortCriteriaInternal(
                    projects=[self.project_id],
                    sg_ids_internal_raw=[self.genome_sequencing_group_id],  # type: ignore [arg-type]
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
        self.assertEqual(api_result.description, mutation_result['description'])
        self.assertEqual(
            api_result.criteria.sample_type, mutation_result['criteria']['sample_type']
        )
        self.assertEqual(
            api_result.criteria.sg_platform, mutation_result['criteria']['sg_platform']
        )
        self.assertEqual(
            api_result.criteria.sg_technology,
            mutation_result['criteria']['sg_technology'],
        )
        self.assertEqual(
            api_result.criteria.sg_type, mutation_result['criteria']['sg_type']
        )
        self.assertEqual(api_result.name, mutation_result['name'])

    # endregion COHORT TESTS

    # region FAMILY TESTS
    @run_as_sync
    async def test_update_family(self):
        """Test updating a family using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_FAMILY_MUTATION,
                variables={
                    'project': self.project_name,
                    'family': {
                        'id': self.family_id,
                        'externalIds': {PRIMARY_EXTERNAL_ORG: 'test'},
                        'description': 'test_family',
                        'codedPhenotype': 'test_family_phenotype',
                    },
                },
            )
        )['family']['updateFamily']

        await self.fl.update_family(
            id_=self.family_id,
            external_ids={'test': 'test'},
            description='test_family',
            coded_phenotype='test_family_phenotype',
        )

        api_result = (
            await self.fl.get_family_by_internal_id(self.family_id)
        ).to_external()  # type: ignore [arg-type]

        self.assertEqual(api_result.external_ids, mutation_result['externalIds'])
        self.assertEqual(api_result.description, mutation_result['description'])
        self.assertEqual(api_result.coded_phenotype, mutation_result['codedPhenotype'])

    # endregion FAMILY TESTS

    # region PARTICIPANT TESTS
    @run_as_sync
    async def test_update_participant(self):
        """Test updating a participant using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_PARTICIPANT_MUTATION,
                variables={
                    'project': self.project_name,
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
        )['participant']['updateParticipant']

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

        self.assertEqual(api_result.external_ids, mutation_result['externalIds'])
        self.assertEqual(api_result.reported_sex, mutation_result['reportedSex'])
        self.assertEqual(api_result.reported_gender, mutation_result['reportedGender'])
        self.assertEqual(api_result.karyotype, mutation_result['karyotype'])
        self.assertEqual(api_result.samples[0].id, mutation_result['samples'][0]['id'])  # type: ignore [arg-type]

    @run_as_sync
    async def test_upsert_participants(self):
        """Test upserting a list of participants using the mutation and the API. This inserts a new participant and updates an existing one."""
        mutation_result = (
            await self.run_graphql_query_async(
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
        )['participant']['upsertParticipants']

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
        self.assertEqual(api_result[0].id, mutation_result[0]['id'])
        self.assertEqual(api_result[0].external_ids, mutation_result[0]['externalIds'])
        self.assertEqual(api_result[0].reported_sex, mutation_result[0]['reportedSex'])
        self.assertEqual(
            api_result[0].reported_gender, mutation_result[0]['reportedGender']
        )
        self.assertEqual(api_result[0].karyotype, mutation_result[0]['karyotype'])

        self.assertEqual(api_result[1].reported_sex, mutation_result[1]['reportedSex'])
        self.assertEqual(
            api_result[1].reported_gender, mutation_result[1]['reportedGender']
        )
        self.assertEqual(api_result[1].karyotype, mutation_result[1]['karyotype'])

    @run_as_sync
    async def test_update_participant_family(self):
        """Test updating a participants family data"""
        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_PARTICIPANT_FAMILY_MUTATION,
                variables={
                    'project': self.project_name,
                    'participantId': self.participant.id,
                    'oldFamilyId': self.family_id,
                    'newFamilyId': self.family_id_2,
                },
            )
        )['participant']['updateParticipantFamily']

        api_result = (
            await self.fl.get_family_participants_by_family_ids([self.family_id_2])
        )[self.family_id_2][0]

        self.assertEqual(api_result.individual_id, mutation_result['participantId'])
        self.assertEqual(api_result.family_id, mutation_result['familyId'])

    # endregion PARTICIPANT TESTS

    # region PROJECT TESTS
    @run_as_sync
    async def test_create_project(self):
        """Test creating a project using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
                CREATE_PROJECT_MUTATION,
                variables={
                    'name': 'test_project',
                    'dataset': 'test_dataset',
                    'createTestProject': True,
                },
            )
        )['project']['createProject']

        api_result = list(
            self.connection.get_and_check_access_to_projects_for_names(
                [mutation_result['name'], mutation_result['name'] + '-test'],
                allowed_roles={
                    ProjectMemberRole.project_admin,
                    ProjectMemberRole.writer,
                },
            )
        )

        self.assertEqual(api_result[0].name, mutation_result['name'])
        self.assertEqual(api_result[0].dataset, mutation_result['dataset'])
        self.assertEqual(api_result[0].meta, mutation_result['meta'])

        self.assertEqual(api_result[1].name, mutation_result['name'] + '-test')
        self.assertEqual(api_result[1].dataset, mutation_result['dataset'])
        self.assertEqual(api_result[1].meta, mutation_result['meta'])

    @run_as_sync
    async def test_update_project(self):
        """Test updating a project using the mutation and the API"""
        create_project_result = (
            await self.run_graphql_query_async(
                CREATE_PROJECT_MUTATION,
                variables={
                    'name': 'new_test_project',
                    'dataset': 'test_dataset',
                    'createTestProject': False,
                },
            )
        )['project']['createProject']

        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_PROJECT_MUTATION,
                variables={
                    'project': create_project_result['name'],
                    'projectUpdateModel': {
                        'meta': {'test': 'test'},
                    },
                },
            )
        )['project']['updateProject']

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

        self.assertEqual(api_result.name, mutation_result['name'])
        self.assertEqual(api_result.dataset, mutation_result['dataset'])
        self.assertEqual(api_result.meta, {'test': 'test'})

    @run_as_sync
    async def test_update_project_members(self):
        """Test updating project members using the mutation and the API"""
        create_project_result = (
            await self.run_graphql_query_async(
                CREATE_PROJECT_MUTATION,
                variables={
                    'name': 'new_test_project2',
                    'dataset': 'test_dataset',
                    'createTestProject': False,
                },
            )
        )['project']['createProject']

        mutation_result = (
            await self.run_graphql_query_async(
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
        )['project']['updateProjectMembers']

        api_result = list(
            self.connection.get_and_check_access_to_projects_for_names(
                [mutation_result['name']],
                allowed_roles={
                    ProjectMemberRole.project_member_admin,
                },
            )
        )[0]

        self.assertEqual(api_result.name, mutation_result['name'])
        self.assertEqual(api_result.dataset, mutation_result['dataset'])
        self.assertEqual(api_result.meta, mutation_result['meta'])
        self.assertEqual(
            [role.value for role in api_result.roles], mutation_result['roles']
        )

    # endregion PROJECT TESTS

    # region SAMPLE TESTS
    @run_as_sync
    async def test_create_sample(self):
        """Test creating a sample using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
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
        )['sample']['createSample']

        api_result = await self.sl.get_sample_by_id(
            sample_id_transform_to_raw(mutation_result['id'])
        )

        self.assertEqual(
            api_result.type,
            'blood',
        )
        self.assertEqual(
            api_result.meta,
            {'test': 'test'},
        )
        self.assertEqual(
            api_result.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'Test10'},
        )
        self.assertEqual(
            api_result.active,
            True,
        )

    @run_as_sync
    async def test_upsert_samples(self):
        """Test upserting a list of samples using the mutation and the API. This inserts a new sample and updates an existing one."""
        mutation_result = (
            await self.run_graphql_query_async(
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
        )['sample']['upsertSamples']

        api_result = await self.sl.get_samples_by(
            sample_ids=[sample_id_transform_to_raw(s['id']) for s in mutation_result]
        )

        self.assertEqual(len(api_result), 2)
        self.assertEqual(
            api_result[0].type,
            'blood',
        )
        self.assertEqual(
            api_result[0].external_ids,
            {PRIMARY_EXTERNAL_ORG: 'Test10'},
        )
        self.assertEqual(
            api_result[0].active,
            True,
        )
        self.assertEqual(
            api_result[1].type,
            'saliva',
        )
        self.assertEqual(
            api_result[1].meta,
            {'test': 'test'},
        )
        self.assertEqual(
            api_result[1].external_ids,
            {PRIMARY_EXTERNAL_ORG: 'Test11'},
        )
        self.assertEqual(
            api_result[1].active,
            True,
        )

    @run_as_sync
    async def test_update_sample(self):
        """Test updating a sample using the mutation and the API"""
        create_sample_result = (
            await self.run_graphql_query_async(
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
        )['sample']['createSample']

        mutation_result = (
            await self.run_graphql_query_async(
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
        )['sample']['updateSample']

        api_result = await self.sl.get_sample_by_id(
            sample_id_transform_to_raw(mutation_result['id'])
        )

        self.assertEqual(
            api_result.type,
            'saliva',
        )
        self.assertEqual(
            api_result.meta,
            {'test': 'test'},
        )
        self.assertEqual(
            api_result.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'Test11'},
        )
        self.assertEqual(
            api_result.active,
            True,
        )

    # endregion SAMPLE TESTS

    # region SEQUENCING GROUP TESTS
    @run_as_sync
    async def test_update_sequencing_group(self):
        """Test updating a sequencing group using the mutation and the API"""
        mutation_result = (
            await self.run_graphql_query_async(
                UPDATE_SEQUENCING_GROUP_MUTATION,
                variables={
                    'project': self.project_name,
                    'sequencingGroup': {
                        'id': self.genome_sequencing_group_id_external,
                        'meta': {'test': 'test'},
                    },
                },
            )
        )['sequencingGroup']['updateSequencingGroup']

        api_result = await self.sgl.get_sequencing_group_by_id(
            sequencing_group_id_transform_to_raw(mutation_result['id'])
        )
        self.assertEqual(
            api_result.meta,
            {'test': 'test'},
        )

    # endregion SEQUENCING GROUP TESTS
