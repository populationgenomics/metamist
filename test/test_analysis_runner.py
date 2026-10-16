import datetime

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.utils import NotFoundError
from models.models.analysis_runner import AnalysisRunnerInternal
from test.conftest import GraphQLQueryFunction


QUERY_BY_CONFIG_PATH = """
query AnalysisRunners($projects: [Int!]!, $configPath: String!) {
    analysisRunners(project: {in_: $projects}, configPath: {eq: $configPath}) {
        arGuid
        configPath
        project { name }
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


class TestAnalysisRunner:
    """Test sample class"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection) -> None:
        self.al = AnalysisRunnerLayer(connection_with_project)
        assert connection_with_project.project_id is not None
        self.project_id = connection_with_project.project_id
        assert connection_with_project.project is not None
        self.project_name = connection_with_project.project.name

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

    @pytest.mark.asyncio
    async def test_insert(self) -> None:
        """Test insert"""
        analysis = self.get_test_analysis('<ar-guid>')
        await self.al.insert_analysis_runner_entry(analysis)

        db_ars = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq=analysis.ar_guid))
        )
        assert len(db_ars) == 1
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
            assert getattr(db_ars[0], field) == getattr(analysis, field), (
                'Field: ' + field
            )

    @pytest.mark.asyncio
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
        assert len(db_ars) == 3

        # get one for 2 of the ar-guids
        guids_to_query = {a.ar_guid for a in analyses[:2]}
        db_ars = await self.al.query(
            AnalysisRunnerFilter(
                ar_guid=GenericFilter(in_=list(guids_to_query)),
            )
        )
        assert len(db_ars) == 2
        db_ars_guid = {a.ar_guid for a in db_ars}
        assert db_ars_guid == guids_to_query

    @pytest.mark.asyncio
    async def test_query_does_not_throw_error_for_empty_filters(self):
        """
        Test that the query throws an error if the filters are empty
        """

        analysis = self.get_test_analysis('<ar-guid>')
        await self.al.insert_analysis_runner_entry(analysis)

        result = await self.al.query(AnalysisRunnerFilter())
        assert result

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graph_query(self, graphql_query):
        """
        Test createAnalysisRunnerLog mutation
        """
        ar_guid = '0511a4bb-1010-4cbf-af6a-1e807be8cad6'
        analysis_runner_log_entry = (
            await graphql_query(
                """
                mutation CreateLog($project: String!, $input: AnalysisRunnerInput!) {
                  analysisRunner {
                    createAnalysisRunnerLog(project: $project, analysisRunner: $input)
                  }
                }
        """,
                {
                    'project': self.project_name,
                    'input': {
                        'arGuid': ar_guid,
                        'accessLevel': 'test',
                        'repository': 'metamist',
                        'commit': 'some-hash',
                        'script': 'myFakeScript.py',
                        'description': 'just analysis things',
                        'driverImage': 'fake-australia-southeast1-fake-docker.pkg',
                        'configPath': 'gs://path/to/config.toml',
                        'cwd': 'scripts',
                        'environment': 'gcp',
                        'hailVersion': '1.0',
                        'batchUrl': 'FAKE://batch.hail.populationgenomics.org.au/batches/fake_2',
                        'submittingUser': 'fake-user',
                        'outputPath': 'gs://my-bucket/output/path',
                        'meta': {
                            'runtime_minutes': '45',
                            'status': 'initialized',
                            'internal_priority': 'low',
                        },
                    },
                },
            )
        )['data']['analysisRunner']['createAnalysisRunnerLog']

        assert analysis_runner_log_entry == ar_guid

    @pytest.mark.asyncio
    async def test_insert_skip_duplicates(self) -> None:
        """Test inserting an existing ar_guid does not throw an error"""
        ar_entry = self.get_test_analysis('<ar-guid>')

        # Insert entry
        assert await self.al.insert_analysis_runner_entry(ar_entry) == ar_entry.ar_guid

        # Insert entry again
        assert await self.al.insert_analysis_runner_entry(ar_entry) == ar_entry.ar_guid

        db_ars = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq=ar_entry.ar_guid))
        )
        assert len(db_ars) == 1

    @pytest.mark.asyncio
    async def test_query_config_path(self) -> None:
        """Test filter config paths"""

        pending_marker = 'SEQERA-PENDING'
        resolved_value = 'gs://bucket/<ar-guid-set>.toml'

        config_paths = {
            '<ar-guid-pending>': pending_marker,
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
                config_path=GenericFilter(eq=pending_marker),
            )
        )
        assert len(ar_fetched) == 1
        assert ar_fetched[0].ar_guid == '<ar-guid-pending>'

        ar_fetched = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                config_path=GenericFilter(eq=resolved_value),
            )
        )
        assert len(ar_fetched) == 1
        assert ar_fetched[0].ar_guid == '<ar-guid-set>'

        ar_fetched = await self.al.query(
            AnalysisRunnerFilter(
                project=GenericFilter(eq=self.project_id),
                config_path=GenericFilter(isnull=True),
            )
        )
        assert len(ar_fetched) == 1
        assert ar_fetched[0].ar_guid == '<ar-guid-null>'

    @pytest.mark.asyncio
    async def test_update_nonexistent_entry(self) -> None:
        """Test updating a nonexistent ar entry raises NotFoundError"""

        with pytest.raises(NotFoundError):
            await self.al.update_analysis_runner_entry(
                self.project_id,
                '<missing-ar-guid>',
                config_path='gs://bucket/missing.toml',
            )

    async def insert_with_config_paths(
        self, graphql_query: GraphQLQueryFunction, ar_guid: str, config_path: str
    ) -> None:
        """Insert one AR entry per ar_guid with the given config path, through GraphQL"""

        await graphql_query(
            CREATE_MUTATION,
            {
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

    async def get_ids_given_config_path(
        self, graphql_query: GraphQLQueryFunction, config_path: str
    ) -> list[str]:
        """Get arGuid list given config path"""
        result = await graphql_query(
            QUERY_BY_CONFIG_PATH,
            {'projects': [self.project_id], 'configPath': config_path},
        )
        return [ar['arGuid'] for ar in result['data']['analysisRunners']]


    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graphql_config_path_filter(self, graphql_query) -> None:
        """Test the configPath filter through GraphQL"""

        pending_marker = 'SEQERA-PENDING'
        resolved_value = 'gs://cpg-config/a.toml'
        await self.insert_with_config_paths(
            graphql_query, '<ar-guid-set>', resolved_value
        )
        await self.insert_with_config_paths(
            graphql_query, '<ar-guid-pending>', pending_marker
        )

        assert await self.get_ids_given_config_path(graphql_query, pending_marker) == [
            '<ar-guid-pending>'
        ]
        assert await self.get_ids_given_config_path(graphql_query, resolved_value) == [
            '<ar-guid-set>'
        ]
        assert await self.get_ids_given_config_path(graphql_query, 'missing') == []


    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graphql_update(self, graphql_query) -> None:
        """Test the analysisRunner config update"""

        new_config_path = 'gs://cpg-config/<ar-guid-pending>.toml'
        await self.insert_with_config_paths(
            graphql_query, '<ar-guid-pending>', 'SEQERA-PENDING'
        )

        await graphql_query(
            UPDATE_MUTATION,
            {
                'project': self.project_name,
                'arGuid': '<ar-guid-pending>',
                'update': {'configPath': new_config_path},
            },
        )

        assert await self.get_ids_given_config_path(graphql_query, 'SEQERA-PENDING') == []
        assert await self.get_ids_given_config_path(graphql_query, new_config_path) == [
            '<ar-guid-pending>'
        ]
