import datetime

from graphql.error import GraphQLError

from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.utils import NotFoundError
from models.models.analysis_runner import (
    AnalysisRunnerInternal,
    AnalysisRunnerUpdateBody,
)
from test.testbase import DbIsolatedTest, run_as_sync


QUERY_BY_CONFIG_PATH = """
query AnalysisRunners($projects: [Int!]!, $configPath: StrGraphQLFilter!) {
    analysisRunners(project: {in_: $projects}, configPath: $configPath) {
        arGuid
        configPath
        project { name }
    }
}
"""

QUERY_BY_AR_GUID = """
query AnalysisRunners($arGuids: [String!]!) {
    analysisRunners(arGuid: {in_: $arGuids}) {
        arGuid
        project { name }
    }
}
"""

QUERY_WITHOUT_PROJECT_OR_AR_GUID = """
query AnalysisRunners {
    analysisRunners(configPath: {isnull: true}) {
        arGuid
    }
}
"""

UPDATE_MUTATION = """
mutation UpdateAnalysisRunner($arGuid: String!, $update: AnalysisRunnerUpdateInput!) {
    analysisRunner {
        updateAnalysisRunnerLog(arGuid: $arGuid, arUpdateInput: $update) {
            arGuid
            configPath
            batchUrl
            project { name }
        }
    }
}
"""

QUERY_PENDING_BY_META = """
query AnalysisRunners($projects: [Int!]!, $meta: JSON!) {
    analysisRunners(
        project: {in_: $projects}, configPath: {isnull: true}, meta: $meta
    ) {
        arGuid
        configPath
        meta
    }
}
"""

CREATE_MUTATION = """
mutation CreateAnalysisRunner($project: String!, $analysisRunner: AnalysisRunnerInput!) {
    analysisRunner {
        createAnalysisRunnerLog(project: $project, analysisRunner: $analysisRunner)
    }
}
"""


class TestAnalysisRunner(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()

        self.al = AnalysisRunnerLayer(self.connection)

    def get_test_analysis(self, ar_guid_param: str) -> AnalysisRunnerInternal:
        return AnalysisRunnerInternal(
            ar_guid=ar_guid_param,
            project=self.project_id,
            output_path='output_path',
            timestamp=datetime.datetime(2024, 1, 1),
            access_level='test',
            repository='repository',
            config_path='config_path',
            environment='gcp',
            submitting_user='submitting_user',
            commit='commit',
            script='script',
            description='description',
            hail_version='1.0',
            cwd='cwd',
            driver_image='driver_image',
            batch_url='batch_url',
            meta={'meta': 'meta'},
        )

    @run_as_sync
    async def test_insert(self):
        """Test insert"""
        analysis = self.get_test_analysis('<ar-guid>')
        await self.al.insert_analysis_runner_entry(analysis)

        db_ars = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq=analysis.ar_guid))
        )
        self.assertEqual(len(db_ars), 1)
        field_to_compare = [
            'project',
            'output_path',
            'access_level',
            'repository',
            'config_path',
            'environment',
            'submitting_user',
            'commit',
            'script',
            'description',
            'hail_version',
            'cwd',
            'driver_image',
            'batch_url',
            'meta',
        ]
        for field in field_to_compare:
            # check each field is the same
            self.assertEqual(
                getattr(db_ars[0], field),
                getattr(analysis, field),
                msg='Field: ' + field,
            )

    @run_as_sync
    async def test_query(self):
        """
        Query all the Filter fields to check they work correctly
        """
        analyses = [self.get_test_analysis(f'<ar-guid-{i + 1}>') for i in range(3)]
        for analysis in analyses:
            await self.al.insert_analysis_runner_entry(analysis)

        db_ars = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                submitting_user=GenericFilter(eq='submitting_user'),
                repository=GenericFilter(eq='repository'),
                access_level=GenericFilter(eq='test'),
                environment=GenericFilter(eq='gcp'),
            )
        )

        # return all 3
        self.assertEqual(len(db_ars), 3)

        # get one for 2 of the ar-guids
        guids_to_query = {a.ar_guid for a in analyses[:2]}
        db_ars = await self.al.query(
            AnalysisRunnerFilter(
                ar_guid=GenericFilter(in_=list(guids_to_query)),
            )
        )
        self.assertEqual(len(db_ars), 2)
        self.assertSetEqual({a.ar_guid for a in db_ars}, guids_to_query)

    @run_as_sync
    async def test_insert_skip_duplicates(self):
        """Test inserting an existing ar_guid does not throw an error"""
        analysis = self.get_test_analysis('<ar-guid>')

        # Insert entry
        self.assertEqual(
            await self.al.insert_analysis_runner_entry(analysis), analysis.ar_guid
        )

        # Insert entry again
        self.assertEqual(
            await self.al.insert_analysis_runner_entry(analysis), analysis.ar_guid
        )

        db_ars = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq=analysis.ar_guid))
        )
        self.assertEqual(len(db_ars), 1)

    @run_as_sync
    async def test_query_config_path(self):
        """Test filter config paths"""

        resolved_value = 'gs://bucket/<ar-guid-set>.toml'
        config_paths = {
            '<ar-guid-set>': resolved_value,
            '<ar-guid-null>': None,
        }
        for ar_guid, config_path in config_paths.items():
            ar_entry = self.get_test_analysis(ar_guid)
            ar_entry.config_path = config_path
            await self.al.insert_analysis_runner_entry(ar_entry)

        ar_fetched = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                config_path=GenericFilter(eq=resolved_value),
            )
        )

        self.assertEqual(len(ar_fetched), 1)
        self.assertEqual(ar_fetched[0].ar_guid, '<ar-guid-set>')

        ar_fetched = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                config_path=GenericFilter(isnull=True),
            )
        )

        self.assertEqual(len(ar_fetched), 1)
        self.assertEqual(ar_fetched[0].ar_guid, '<ar-guid-null>')

    @run_as_sync
    async def test_update_nonexistent_entry(self):
        """Test updating a nonexistent AnalysisRunner entry raises NotFoundError"""
        with self.assertRaises(NotFoundError):
            await self.al.update_analysis_runner_entry(
                AnalysisRunnerUpdateBody(
                    ar_guid='<missing-ar-guid>',
                    config_path='gs://bucket/missing.toml',
                )
            )

    @run_as_sync
    async def test_update_requires_a_field(self):
        """Test updating an AnalysisRunner entry without input fields raises ValueError"""
        await self.al.insert_analysis_runner_entry(self.get_test_analysis('<ar-guid>'))

        with self.assertRaises(ValueError):
            await self.al.update_analysis_runner_entry(
                AnalysisRunnerUpdateBody(ar_guid='<ar-guid>')
            )

    @run_as_sync
    async def test_graphql_ar_guid_filter(self):
        """Test query AnalysisRunner entries on arGuids"""
        await self.al.insert_analysis_runner_entry(
            self.get_test_analysis('<ar-guid-1>')
        )
        await self.al.insert_analysis_runner_entry(
            self.get_test_analysis('<ar-guid-2>')
        )

        result = await self.run_graphql_query_async(
            QUERY_BY_AR_GUID,
            variables={'arGuids': ['<ar-guid-1>', '<ar-guid-2>', '<ar-guid-3>']},
        )
        ar_guids = [ar['arGuid'] for ar in result['analysisRunners']]
        self.assertIn('<ar-guid-1>', ar_guids)
        self.assertIn('<ar-guid-2>', ar_guids)
        self.assertNotIn('<ar-guid-3>', ar_guids)

    @run_as_sync
    async def test_graphql_requires_project_or_ar_guid(self):
        """Test throws error when neither project nor arGuid is given"""
        with self.assertRaises(GraphQLError):
            await self.run_graphql_query_async(QUERY_WITHOUT_PROJECT_OR_AR_GUID)

    @run_as_sync
    async def test_graphql_config_path_filter(self):
        """Test query AnalysisRunner entries given config path"""
        resolved_value = 'gs://bucket/<ar-guid-set>.toml'
        for ar_guid, config_path in (
            ('<ar-guid-set>', resolved_value),
            ('<ar-guid-null>', None),
        ):
            ar_entry = self.get_test_analysis(ar_guid)
            ar_entry.config_path = config_path
            await self.al.insert_analysis_runner_entry(ar_entry)

        async def get_ids_given_config_path(config_path: dict) -> list[str]:
            result = await self.run_graphql_query_async(
                QUERY_BY_CONFIG_PATH,
                variables={'projects': [self.project_id], 'configPath': config_path},
            )
            return [ar['arGuid'] for ar in result['analysisRunners']]

        self.assertListEqual(
            await get_ids_given_config_path({'isnull': True}), ['<ar-guid-null>']
        )
        self.assertListEqual(
            await get_ids_given_config_path({'eq': resolved_value}), ['<ar-guid-set>']
        )

    @run_as_sync
    async def test_graphql_create_without_config_path(self):
        """Test createAnalysisRunnerLog accepts a missing config path"""
        analysis_runner = {
            'arGuid': '<ar-guid>',
            'accessLevel': 'test',
            'repository': 'repository',
            'commit': 'commit',
            'script': 'script',
            'description': 'description',
            'driverImage': 'driver_image',
            'environment': 'gcp',
            'batchUrl': 'batch_url',
            'submittingUser': 'submitting_user',
            'meta': {'seqera_status': 'PENDING'},
            'outputPath': 'output_path',
        }
        await self.run_graphql_query_async(
            CREATE_MUTATION,
            variables={'project': self.project_name, 'analysisRunner': analysis_runner},
        )

        result = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq='<ar-guid>'))
        )
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0].config_path)

    @run_as_sync
    async def test_graphql_meta_filter(self):
        """Test query AnalysisRunner entries on meta value"""
        ar_entry = self.get_test_analysis('<ar-guid-pending>')
        ar_entry.config_path = None
        ar_entry.meta = {'key': 'value'}
        await self.al.insert_analysis_runner_entry(ar_entry)

        result = await self.run_graphql_query_async(
            QUERY_PENDING_BY_META,
            variables={
                'projects': [self.project_id],
                'meta': {'key': {'eq': 'value'}},
            },
        )
        self.assertListEqual(
            result['analysisRunners'],
            [
                {
                    'arGuid': '<ar-guid-pending>',
                    'configPath': None,
                    'meta': {'key': 'value'},
                }
            ],
        )

    @run_as_sync
    async def test_graphql_update(self):
        """Test updateAnalysisRunnerLog sets the config path"""
        ar_entry = self.get_test_analysis('<ar-guid>')
        ar_entry.config_path = None
        ar_entry.batch_url = 'URL UNAVAILABLE'
        await self.al.insert_analysis_runner_entry(ar_entry)

        new_config_path = 'gs://bucket/<ar-guid>.toml'
        new_batch_url = 'new_batch_url'
        result = await self.run_graphql_query_async(
            UPDATE_MUTATION,
            variables={
                'arGuid': '<ar-guid>',
                'update': {'configPath': new_config_path, 'batchUrl': new_batch_url},
            },
        )

        self.assertDictEqual(
            result['analysisRunner']['updateAnalysisRunnerLog'],
            {
                'arGuid': '<ar-guid>',
                'configPath': new_config_path,
                'batchUrl': new_batch_url,
                'project': {'name': self.project_name},
            },
        )

    @run_as_sync
    async def test_graphql_update_meta(self):
        """Test updateAnalysisRunnerLog merges the meta into the existing meta"""
        ar_entry = self.get_test_analysis('<ar-guid>')
        ar_entry.meta = {'key1': 'value1', 'key2': 'value2'}
        await self.al.insert_analysis_runner_entry(ar_entry)

        await self.run_graphql_query_async(
            UPDATE_MUTATION,
            variables={
                'arGuid': '<ar-guid>',
                'update': {'meta': {'key2': 'new_value2'}},
            },
        )

        result = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq='<ar-guid>'))
        )
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0].meta, {'key1': 'value1', 'key2': 'new_value2'})
