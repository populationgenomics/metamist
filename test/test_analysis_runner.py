import datetime

from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.utils import NotFoundError
from models.models.analysis_runner import AnalysisRunnerInternal
from test.testbase import DbIsolatedTest, run_as_sync


QUERY_BY_CONFIG_PATH = """
query AnalysisRunners($project: String!, $configPath: String!) {
    project(name: $project) {
        analysisRunner(configPath: {eq: $configPath}) { arGuid configPath }
    }
}
"""

CREATE_MUTATION = """
mutation CreateAnalysisRunner($project: String!, $ar: AnalysisRunnerInput!) {
    analysisRunner {
        createAnalysisRunnerLog(project: $project, analysisRunner: $ar)
    }
}
"""

UPDATE_MUTATION = """
mutation UpdateAnalysisRunner(
    $project: String!, $arGuid: String!, $update: AnalysisRunnerUpdateInput!
) {
    analysisRunner {
        updateAnalysisRunnerLog(
            project: $project, arGuid: $arGuid, arUpdateInput: $update
        )
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

        pending_marker = 'SEQERA-PENDING'
        resolved_value = 'gs://cpg-config/<ar-guid-set>.toml'
        config_paths = {
            '<ar-guid-set>': resolved_value,
            '<ar-guid-pending>': pending_marker,
            '<ar-guid-null>': None,
        }
        for ar_guid, config_path in config_paths.items():
            ar_entry = self.get_test_analysis(ar_guid)
            ar_entry.config_path = config_path
            await self.al.insert_analysis_runner_entry(ar_entry)

        ar_fetched = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                config_path=GenericFilter(eq=pending_marker),
            )
        )
        self.assertEqual(len(ar_fetched), 1)
        self.assertEqual(ar_fetched[0].ar_guid, '<ar-guid-pending>')

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
    async def test_update_not_found(self):
        """Test updating a non-existing ar entry raises NotFoundError"""
        with self.assertRaises(NotFoundError):
            await self.al.update_analysis_runner_entry(
                self.project_id,
                '<missing-ar-guid>',
                config_path='gs://cpg-config/missing.json',
            )

    async def insert_with_config_paths(self, ar_guid: str, config_path: str):
        """Insert AR entry for given ar_guid and config_path"""

        await self.run_graphql_query_async(
            CREATE_MUTATION,
            variables={
                'project': self.project_name,
                'ar': {
                    'arGuid': ar_guid,
                    'accessLevel': 'test',
                    'repository': 'repository',
                    'commit': 'commit',
                    'script': 'script',
                    'description': 'description',
                    'driverImage': 'driver_image',
                    'configPath': config_path,
                    'environment': 'gcp',
                    'batchUrl': 'batch_url',
                    'submittingUser': 'submitting_user',
                    'meta': {'meta': 'meta'},
                    'outputPath': 'output_path',
                },
            },
        )

    async def get_arguids_of_given_config_path(self, config_path: str) -> list[str]:
        """Return arGuid/s for queried config_path"""
        result = await self.run_graphql_query_async(
            QUERY_BY_CONFIG_PATH,
            variables={'project': self.project_name, 'configPath': config_path},
        )
        return [ar['arGuid'] for ar in result['project']['analysisRunner']]

    @run_as_sync
    async def test_graphql_config_path_filter(self):
        """Test filter config paths via GraphQL"""
        pending_marker = 'SEQERA-PENDING'
        resolved_value = 'gs://cpg-config/a.toml'
        await self.insert_with_config_paths('<ar-guid-set>', resolved_value)

        await self.insert_with_config_paths('<ar-guid-pending>', pending_marker)

        self.assertListEqual(
            await self.get_arguids_of_given_config_path(pending_marker),
            ['<ar-guid-pending>'],
        )
        self.assertListEqual(
            await self.get_arguids_of_given_config_path(resolved_value),
            ['<ar-guid-set>'],
        )

    @run_as_sync
    async def test_graphql_update(self):
        """Test GraphQL update mutation"""

        pending_marker = 'SEQERA-PENDING'
        await self.insert_with_config_paths('<ar-guid-pending>', pending_marker)

        new_config_path = 'gs://cpg-config/<ar-guid-pending>.json'

        await self.run_graphql_query_async(
            UPDATE_MUTATION,
            variables={
                'project': self.project_name,
                'arGuid': '<ar-guid-pending>',
                'update': {'configPath': new_config_path},
            },
        )

        self.assertListEqual(
            await self.get_arguids_of_given_config_path(pending_marker), []
        )
        self.assertListEqual(
            await self.get_arguids_of_given_config_path(new_config_path),
            ['<ar-guid-pending>'],
        )
