from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.project import (
    GROUP_NAME_MEMBERS_ADMIN,
    GROUP_NAME_PROJECT_CREATORS,
)
from metamist.graphql import gql
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal

default_sequencing_meta = {
    'sequencing_type': 'genome',
    'sequencing_platform': 'short-read',
    'sequencing_technology': 'illumina',
}


class GraphQLMutationsTest(DbIsolatedTest):
    """Test graphql mutations"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.slayer = SampleLayer(self.connection)
        self.flayer = FamilyLayer(self.connection)
        self.player = ParticipantLayer(self.connection)

    # AnalysisRunnerMutations
    @run_as_sync
    async def test_create_analysis_runner_log_mutation(self):
        """Test createAnalysisRunnerLog mutation"""
        query = gql(
            """
        mutation CreateAnalysisRunnerLog($arGuid: String!, $accessLevel: String!, $repository: String!, $commit: String!, $script: String!, $description: String!, $driverImage: String!, $configPath: String!, $environment: String!, $batchUrl: String!, $submittingUser: String!, $meta: JSON!, $outputPath: String!, $hailVersion: String, $cwd: String) {
        analysisRunner {
            createAnalysisRunnerLog(arGuid: $arGuid, accessLevel: $accessLevel, repository: $repository, commit: $commit, script: $script, description: $description, driverImage: $driverImage, configPath: $configPath, environment: $environment, batchUrl: $batchUrl, submittingUser: $submittingUser, meta: $meta, outputPath: $outputPath, hailVersion: $hailVersion, cwd: $cwd)
            }
        }
        """
        )

        variables = {
            'arGuid': 'guid123',
            'accessLevel': 'admin',
            'repository': 'repo123',
            'commit': 'commit123',
            'script': 'script.sh',
            'description': 'description',
            'driverImage': 'driver_image',
            'configPath': 'config/path',
            'environment': 'env',
            'batchUrl': 'batch/url',
            'submittingUser': 'user123',
            'meta': {'key': 'value'},
            'outputPath': 'output/path',
            'hailVersion': 'hail_version',
            'cwd': 'cwd/path',
        }

        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        self.assertIsInstance(
            response['analysisRunner']['createAnalysisRunnerLog'], str
        )
        self.assertEqual(
            response['analysisRunner']['createAnalysisRunnerLog'], 'guid123'
        )

    # Analysis Mutations
    @run_as_sync
    async def test_create_and_update_analysis_mutation(self):
        """Test createAnalysis and updateAnalysis mutations"""
        create_query = gql(
            """
        mutation CreateAnalysis($analysis: AnalysisInput!) {
          analysis {
            createAnalysis(analysis: $analysis)
          }
        }
        """
        )

        create_variables = {
            'analysis': {
                'type': 'cram',
                'status': 'QUEUED',
                'id': 1,
                'output': 'ExampleOutput',
                'sequencingGroupIds': ['group1'],
                'cohortIds': ['cohort1'],
                'author': 'AuthorName',
                'timestampCompleted': '2023-06-16T00:00:00',
                'project': 1,
                'active': True,
                'meta': {'key': 'value'},
            }
        }

        create_response = await self.run_graphql_query_async(
            create_query, variables=create_variables
        )
        assert create_response
        create_id = create_response['analysis']['createAnalysis']

        self.assertIsInstance(create_id, int)
        self.assertEqual(create_id, 1)

        update_query = gql(
            """
        mutation UpdateAnalysis($analysisId: Int!, $analysis: AnalysisUpdateInput!) {
          analysis {
            updateAnalysis(analysisId: $analysisId, analysis: $analysis)
          }
        }
        """
        )

        update_variables = {
            'analysisId': create_id,
            'analysis': {
                'status': 'COMPLETED',
                'output': 'UpdatedOutput',
                'meta': {'key': 'updated_value'},
                'active': False,
            },
        }

        update_response = await self.run_graphql_query_async(
            update_query, variables=update_variables
        )
        assert update_response
        self.assertIsInstance(update_response['analysis']['updateAnalysis'], bool)
        self.assertTrue(update_response['analysis']['updateAnalysis'])

    # AssayMutations
    @run_as_sync
    async def test_create_and_update_assay(self):
        """Test createAssay and updateAssay mutations"""
        sample = (
            await self.slayer.upsert_sample(
                SampleUpsertInternal(
                    external_ids={
                        PRIMARY_EXTERNAL_ORG: 'P1',
                        'CONTROL': '86',
                        'KAOS': 'shoe',
                    },
                    project=1,
                    type='blood',
                    active=True,
                    meta={'meta': 'meta ;)'},
                )
            )
        ).to_external()

        create_query = gql(
            """
        mutation CreateAssay($assay: AssayUpsertInput!) {
            assay {
                createAssay(assay: $assay) {
                    id
                    type
                    externalIds
                    sampleId
                    meta
                }
            }
        }
        """
        )

        create_variables = {
            'assay': {
                'id': None,
                'type': 'sequencing',
                'externalIds': {'key': 'value'},
                'sampleId': sample.id,
                'meta': {**default_sequencing_meta},
            }
        }

        create_response = await self.run_graphql_query_async(
            create_query, variables=create_variables
        )
        assert create_response
        created_assay = create_response['assay']['createAssay']
        self.assertEqual(created_assay['id'], 1)
        self.assertEqual(created_assay['type'], 'sequencing')
        self.assertEqual(created_assay['externalIds'], {'key': 'value'})
        self.assertEqual(created_assay['sampleId'], sample.id)
        self.assertDictEqual(created_assay['meta'], default_sequencing_meta)

        # Then, update the created assay
        update_query = gql(
            """
        mutation UpdateAssay($assay: AssayUpsertInput!) {
            assay {
                updateAssay(assay: $assay)
            }
        }
        """
        )

        update_variables = {
            'assay': {
                'id': created_assay['id'],
                'type': 'sequencing',
                'externalIds': {'key': 'updated_value'},
                'sampleId': None,
                'meta': {**default_sequencing_meta},
            }
        }

        update_response = await self.run_graphql_query_async(
            update_query, variables=update_variables
        )
        assert update_response
        updated_id = update_response['assay']['updateAssay']
        self.assertEqual(updated_id, 1)

    # CohortMutations
    @run_as_sync
    async def test_create_cohort_from_criteria(self):
        """Test createCohortFromCriteria mutation"""
        create_query = gql(
            """
        mutation CreateCohortFromCriteria($cohortSpec: CohortBodyInput!, $cohortCriteria: CohortCriteriaInput, $dryRun: Boolean!) {
          cohort {
            createCohortFromCriteria(cohortSpec: $cohortSpec, cohortCriteria: $cohortCriteria, dryRun: $dryRun) {
              dryRun
              cohortId
              sequencingGroupIds
            }
          }
        }
        """
        )

        create_variables = {
            'cohortSpec': {
                'name': 'Example Cohort',
                'description': 'An example cohort',
                'templateId': None,
            },
            'cohortCriteria': {
                'projects': ['test'],
                'sgIdsInternal': ['CPGLCL33'],
                'excludedSgsInternal': ['CPGLCL17', 'CPGLCL25'],
                'sgTechnology': ['illumina'],
                'sgPlatform': ['short-read'],
                'sgType': ['genome'],
                'sampleType': ['blood'],
            },
            'dryRun': False,
        }

        create_response = await self.run_graphql_query_async(
            create_query, variables=create_variables
        )
        assert create_response
        created_cohort = create_response['cohort']['createCohortFromCriteria']
        self.assertEqual(created_cohort['dryRun'], False)
        self.assertIn('COH', created_cohort['cohortId'])
        self.assertEqual(created_cohort['sequencingGroupIds'], [])

    @run_as_sync
    async def test_create_cohort_template(self):
        """Test createCohortTemplate mutation"""
        create_template_query = gql(
            """
        mutation CreateCohortTemplate($template: CohortTemplateInput!) {
          cohort {
            createCohortTemplate(template: $template)
          }
        }
        """
        )

        create_template_variables = {
            'template': {
                'id': None,
                'name': 'Template Name',
                'description': 'Template Description',
                'criteria': {
                    'projects': ['test'],
                    'sgIdsInternal': ['CPGLCL33'],
                    'excludedSgsInternal': ['CPGLCL17', 'CPGLCL25'],
                    'sgTechnology': ['illumina'],
                    'sgPlatform': ['short-read'],
                    'sgType': ['genome'],
                    'sampleType': ['blood'],
                },
            }
        }

        create_template_response = await self.run_graphql_query_async(
            create_template_query, variables=create_template_variables
        )
        assert create_template_response
        self.assertIsInstance(
            create_template_response['cohort']['createCohortTemplate'],
            str,
        )
        self.assertIn(
            'CTPL', create_template_response['cohort']['createCohortTemplate']
        )

    # FamilyMutations
    @run_as_sync
    async def test_update_family(self):
        """Test updateFamily mutation"""
        fid_1 = await self.flayer.create_family(external_id='FAM01')

        update_query = gql(
            """
        mutation UpdateFamily($family: FamilyUpdateInput!) {
          family {
            updateFamily(family: $family)
          }
        }
        """
        )

        update_variables = {
            'family': {
                'id': fid_1,
                'externalId': 'ext123',
                'description': 'Updated description',
                'codedPhenotype': 'Updated phenotype',
            }
        }

        update_response = await self.run_graphql_query_async(
            update_query, variables=update_variables
        )
        assert update_response
        self.assertIsInstance(update_response['family']['updateFamily'], bool)
        self.assertTrue(update_response['family']['updateFamily'])

    # ParticipantMutations
    @run_as_sync
    async def test_fill_in_missing_participants(self):
        """Test fillInMissingParticipants mutation"""
        query = gql(
            """
        mutation {
          participant {
            fillInMissingParticipants
          }
        }
        """
        )
        response = await self.run_graphql_query_async(query)
        assert response
        self.assertIsInstance(
            response['participant']['fillInMissingParticipants']['success'],
            str,
        )

    @run_as_sync
    async def test_update_many_participant_external_ids(self):
        """Test updateManyParticipantExternalIds mutation"""
        pid = (
            await self.player.upsert_participant(
                project=self.project_id,
                participant=ParticipantUpsertInternal(
                    external_ids={
                        PRIMARY_EXTERNAL_ORG: 'P1',
                        'CONTROL': '86',
                        'KAOS': 'shoe',
                    },
                    reported_sex=2,
                ),
            )
        ).id
        pat_pid = (
            await self.player.upsert_participant(
                project=self.project_id,
                participant=ParticipantUpsertInternal(
                    external_ids={
                        PRIMARY_EXTERNAL_ORG: 'P2',
                        'CONTROL': '90',
                        'KAOS': 'fish',
                    },
                    reported_sex=1,
                ),
            )
        ).id

        query = gql(
            """
        mutation UpdateManyParticipantExternalIds($internalToExternalId: JSON!) {
          participant {
            updateManyParticipantExternalIds(internalToExternalId: $internalToExternalId)
          }
        }
        """
        )
        variables = {'internalToExternalId': {pid: 'P1B', pat_pid: 'P2B'}}
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        self.assertTrue(response['participant']['updateManyParticipantExternalIds'])

        assert pid and pat_pid
        participants = await self.player.get_participants_by_ids([pid, pat_pid])
        p_map = {p.id: p for p in participants}
        outp1 = p_map[pid]
        outp2 = p_map[pat_pid]
        self.assertDictEqual(
            outp1.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'P1B', 'control': '86', 'kaos': 'shoe'},
        )
        self.assertDictEqual(
            outp2.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'P2B', 'control': '90', 'kaos': 'fish'},
        )

    @run_as_sync
    async def test_update_participant(self):
        """Test updateParticipant mutation"""
        pid = await self.player.upsert_participant(
            project=self.project_id,
            participant=ParticipantUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'P1',
                    'CONTROL': '86',
                    'KAOS': 'shoe',
                },
                reported_sex=2,
            ),
        )
        query = gql(
            """
        mutation UpdateParticipant($participantId: Int!, $participant: ParticipantUpsertInput!) {
          participant {
            updateParticipant(participantId: $participantId, participant: $participant) {
              id
              externalIds
              reportedSex
              reportedGender
              karyotype
              meta
              samples {
                id
                externalIds
                meta
              }
            }
          }
        }
        """
        )
        variables = {
            'participantId': pid.id,
            'participant': {
                'externalIds': {
                    PRIMARY_EXTERNAL_ORG: 'P1B',
                    'CONTROL': '90',
                    'KAOS': 'kiwi',
                },
                'reportedSex': 1,
                'reportedGender': 'Male',
                'karyotype': '46,XY',
                'meta': {'key': 'value'},
                'samples': [
                    {
                        'type': 'blood',
                        'externalIds': {
                            PRIMARY_EXTERNAL_ORG: 'P1B',
                            'CONTROL': '90',
                            'KAOS': 'kiwi',
                        },
                        'meta': {'sampleKey': 'sampleValue'},
                    }
                ],
            },
        }
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        updated_participant = response['participant']['updateParticipant']
        self.assertEqual(updated_participant['id'], pid.id)
        self.assertDictEqual(
            updated_participant['externalIds'],
            {
                PRIMARY_EXTERNAL_ORG: 'P1B',
                'CONTROL': '90',
                'KAOS': 'kiwi',
            },
        )
        self.assertEqual(updated_participant['reportedSex'], 1)
        self.assertEqual(updated_participant['reportedGender'], 'Male')
        self.assertEqual(updated_participant['karyotype'], '46,XY')
        self.assertEqual(updated_participant['meta'], {'key': 'value'})
        self.assertIsInstance(updated_participant['samples'][0]['id'], str)
        self.assertEqual(
            updated_participant['samples'][0]['externalIds'],
            {
                PRIMARY_EXTERNAL_ORG: 'P1B',
                'CONTROL': '90',
                'KAOS': 'kiwi',
            },
        )
        self.assertEqual(
            updated_participant['samples'][0]['meta'], {'sampleKey': 'sampleValue'}
        )

    @run_as_sync
    async def test_upsert_participants(self):
        """Test upsertParticipants mutation"""
        p1 = await self.player.upsert_participant(
            project=self.project_id,
            participant=ParticipantUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'P1',
                    'CONTROL': '86',
                    'KAOS': 'shoe',
                },
                reported_sex=2,
                samples=[
                    SampleUpsertInternal(
                        external_ids={
                            PRIMARY_EXTERNAL_ORG: 'P1',
                            'CONTROL': '86',
                            'KAOS': 'shoe',
                        },
                        type='blood',
                        meta={'sampleKey': 'sampleValue'},
                    )
                ],
            ),
        )
        query = gql(
            """
        mutation UpsertParticipants($participants: [ParticipantUpsertInput!]!) {
          participant {
            upsertParticipants(participants: $participants) {
              id
              externalIds
              reportedSex
              reportedGender
              karyotype
              meta
              samples {
                id
                externalIds
                meta
              }
            }
          }
        }
        """
        )
        variables = {
            'participants': [
                {
                    'id': p1.id,
                    'externalIds': {
                        PRIMARY_EXTERNAL_ORG: 'P1B',
                        'CONTROL': '90',
                        'KAOS': 'kiwi',
                    },
                    'reportedSex': 1,
                    'reportedGender': 'Male',
                    'karyotype': '46,XY',
                    'meta': {'key': 'value'},
                    'samples': [
                        {
                            'externalIds': {
                                PRIMARY_EXTERNAL_ORG: 'P1B',
                                'CONTROL': '90',
                                'KAOS': 'kiwi',
                            },
                            'meta': {'sampleKey': 'sampleValue'},
                        }
                    ],
                },
                {
                    'externalIds': {
                        PRIMARY_EXTERNAL_ORG: 'P2B',
                        'CONTROL': '91',
                        'KAOS': 'apple',
                    },
                    'reportedSex': 2,
                    'reportedGender': 'Female',
                    'karyotype': '46,XX',
                    'meta': {'key': 'value2'},
                    'samples': [
                        {
                            'externalIds': {
                                PRIMARY_EXTERNAL_ORG: 'P2B',
                                'CONTROL': '91',
                                'KAOS': 'apple',
                            },
                            'meta': {'sampleKey': 'sampleValue2'},
                        }
                    ],
                },
            ]
        }
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        upserted_participants = response['participant']['upsertParticipants']
        self.assertEqual(len(upserted_participants), 2)

        # Validate first participant
        participant_1 = upserted_participants[0]
        self.assertEqual(participant_1['id'], p1.id)
        self.assertEqual(
            participant_1['externalIds'],
            {PRIMARY_EXTERNAL_ORG: 'P1B', 'CONTROL': '90', 'KAOS': 'kiwi'},
        )
        self.assertEqual(participant_1['reportedSex'], 1)
        self.assertEqual(participant_1['reportedGender'], 'Male')
        self.assertEqual(participant_1['karyotype'], '46,XY')
        self.assertEqual(participant_1['meta'], {'key': 'value'})
        self.assertIsInstance(participant_1['samples'][0]['id'], str)
        self.assertDictEqual(
            participant_1['samples'][0]['externalIds'],
            {PRIMARY_EXTERNAL_ORG: 'P1B', 'CONTROL': '90', 'KAOS': 'kiwi'},
        )
        self.assertEqual(
            participant_1['samples'][0]['meta'], {'sampleKey': 'sampleValue'}
        )

        # Validate second participant
        participant_2 = upserted_participants[1]
        self.assertEqual(
            participant_2['externalIds'],
            {PRIMARY_EXTERNAL_ORG: 'P2B', 'CONTROL': '91', 'KAOS': 'apple'},
        )
        self.assertEqual(participant_2['reportedSex'], 2)
        self.assertEqual(participant_2['reportedGender'], 'Female')
        self.assertEqual(participant_2['karyotype'], '46,XX')
        self.assertEqual(participant_2['meta'], {'key': 'value2'})
        self.assertIsInstance(participant_2['samples'][0]['id'], str)
        self.assertDictEqual(
            participant_2['samples'][0]['externalIds'],
            {PRIMARY_EXTERNAL_ORG: 'P2B', 'CONTROL': '91', 'KAOS': 'apple'},
        )
        self.assertEqual(
            participant_2['samples'][0]['meta'], {'sampleKey': 'sampleValue2'}
        )

    @run_as_sync
    async def test_update_participant_family(self):
        """Test updateParticipantFamily mutation"""
        pid = (
            await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'}, reported_sex=2
                )
            )
        ).id
        pat_pid = (
            await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_pat'}, reported_sex=1
                )
            )
        ).id
        mat_pid = (
            await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_mat'}, reported_sex=2
                )
            )
        ).id
        fid_1 = await self.flayer.create_family(external_id='FAM01')
        fid_2 = await self.flayer.create_family(external_id='FAM02')

        await self.player.add_participant_to_family(
            family_id=fid_1,
            participant_id=pid,
            paternal_id=pat_pid,
            maternal_id=mat_pid,
            affected=2,
        )

        query = gql(
            """
        mutation UpdateParticipantFamily($participantId: Int!, $oldFamilyId: Int!, $newFamilyId: Int!) {
          participant {
            updateParticipantFamily(participantId: $participantId, oldFamilyId: $oldFamilyId, newFamilyId: $newFamilyId) {
              familyId
              participantId
            }
          }
        }
        """
        )
        variables = {'participantId': pid, 'oldFamilyId': fid_1, 'newFamilyId': fid_2}
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        updated_family = response['participant']['updateParticipantFamily']
        self.assertEqual(updated_family['participantId'], pid)
        self.assertEqual(updated_family['familyId'], fid_2)

    # ProjectMutations
    async def _add_group_member_direct(self, group_name: str):
        """
        Helper method to directly add members to group with name
        """
        members_admin_group = await self.connection.connection.fetch_val(
            'SELECT id FROM `group` WHERE name = :name',
            {'name': group_name},
        )
        await self.connection.connection.execute(
            """
            INSERT INTO group_member (group_id, member, audit_log_id)
            VALUES (:group_id, :member, :audit_log_id);
            """,
            {
                'group_id': members_admin_group,
                'member': self.author,
                'audit_log_id': await self.audit_log_id(),
            },
        )

    @run_as_sync
    async def test_create_project(self):
        """Test createProject mutation"""
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)

        query = gql(
            """
        mutation CreateProject($name: String!, $dataset: String!, $createTestProject: Boolean!) {
          project {
            createProject(name: $name, dataset: $dataset, createTestProject: $createTestProject)
          }
        }
        """
        )
        variables = {
            'name': 'Test Project',
            'dataset': 'Test Dataset',
            'createTestProject': True,
        }
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        created_project_id = response['project']['createProject']
        self.assertIsInstance(created_project_id, int)

    @run_as_sync
    async def test_update_project(self):
        """Test updateProject mutation"""
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)

        query = gql(
            """
        mutation UpdateProject($project: String!, $projectUpdateModel: JSON!) {
          project {
            updateProject(project: $project, projectUpdateModel: $projectUpdateModel)
          }
        }
        """
        )
        variables = {
            'project': 'Test Project',
            'projectUpdateModel': {'name': 'Updated Project Name'},
        }
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        updated_project = response['project']['updateProject']
        self.assertEqual(updated_project['success'], True)

    @run_as_sync
    async def test_delete_project_data(self):
        """Test deleteProjectData mutation"""
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)

        query = gql(
            """
        mutation DeleteProjectData($deleteProject: Boolean!) {
          project {
            deleteProjectData(deleteProject: $deleteProject)
          }
        }
        """
        )
        variables = {'deleteProject': False}
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        deleted_project = response['project']['deleteProjectData']
        self.assertEqual(deleted_project['success'], True)

    @run_as_sync
    async def test_update_project_members(self):
        """Test updateProjectMembers mutation"""
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        query = gql(
            """
        mutation UpdateProjectMembers($project: String!, $members: [ProjectMemberUpdateInput!]!, $readonly: Boolean!) {
          project {
            updateProjectMembers(project: $project, members: $members, readonly: $readonly)
          }
        }
        """
        )
        variables = {
            'project': 'Test Project',
            'members': [
                {'member': 'member1', 'roles': ['reader', 'writer']},
                {'member': 'member2', 'roles': ['contributor']},
            ],
            'readonly': True,
        }
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        updated_members = response['project']['updateProjectMembers']
        self.assertEqual(updated_members['success'], True)
