from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from metamist.graphql import gql
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
                'project': 123,
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
                    external_id='Test01',
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
                    external_id='EX01', reported_sex=2
                ),
            )
        ).id
        pat_pid = (
            await self.player.upsert_participant(
                project=self.project_id,
                participant=ParticipantUpsertInternal(
                    external_id='EX01_pat', reported_sex=1
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
        variables = {'internalToExternalId': {pid: 'EX02', pat_pid: 'EX02_pat'}}
        response = await self.run_graphql_query_async(query, variables=variables)
        assert response
        self.assertTrue(response['participant']['updateManyParticipantExternalIds'])
