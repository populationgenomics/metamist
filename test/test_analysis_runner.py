import datetime

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.utils import NotFoundError
from models.models.analysis_runner import (
    AnalysisRunnerInternal,
    AnalysisRunnerUpdateBody,
)
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
    analysisRunners(configPath: {eq: "SEQERA-PENDING"}) {
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
            project { name }
        }
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
        """Test filter Analysis Runner entries given config path"""

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

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graphql_ar_guid_filter(self, graphql_query) -> None:
        """Test query AnalysisRunner entries on arGuids"""

        await self.al.insert_analysis_runner_entry(
            self.get_test_analysis('<ar-guid-1>')
        )
        await self.al.insert_analysis_runner_entry(
            self.get_test_analysis('<ar-guid-2>')
        )

        result = await graphql_query(
            QUERY_BY_AR_GUID,
            {'arGuids': ['<ar-guid-1>', '<ar-guid-2>', '<ar-guid-3>']},
        )
        ar_entries = result['data']['analysisRunners']
        ar_guids = [ar['arGuid'] for ar in ar_entries]
        assert '<ar-guid-1>' in ar_guids
        assert '<ar-guid-2>' in ar_guids
        assert '<ar-guid-3>' not in ar_guids

    @pytest.mark.asyncio
    async def test_graphql_requires_project_or_ar_guid(self, graphql_query) -> None:
        """Test throws error when neither project nor arGuid is given"""

        result = await graphql_query(QUERY_WITHOUT_PROJECT_OR_AR_GUID)
        assert result['data'] is None
        assert 'Must provide project or ar_guid' in result['errors'][0]['message']

    @pytest.mark.asyncio
    async def test_graphql_config_path_filter(
        self, graphql_query: GraphQLQueryFunction
    ) -> None:
        """Test query AnalysisRunner entries given config path"""

        pending_marker = 'SEQERA-PENDING'
        resolved_value = 'gs://bucket/<ar-guid-set>.toml'
        for ar_guid, config_path in (
            ('<ar-guid-set>', resolved_value),
            ('<ar-guid-pending>', pending_marker),
        ):
            ar_entry = self.get_test_analysis(ar_guid)
            ar_entry.config_path = config_path
            await self.al.insert_analysis_runner_entry(ar_entry)

        async def get_ids_given_config_path(path: str) -> list[str]:
            result = await graphql_query(
                QUERY_BY_CONFIG_PATH,
                {'projects': [self.project_id], 'configPath': path},
            )
            return [ar['arGuid'] for ar in result['data']['analysisRunners']]

        assert await get_ids_given_config_path(pending_marker) == ['<ar-guid-pending>']
        assert await get_ids_given_config_path(resolved_value) == ['<ar-guid-set>']
        assert await get_ids_given_config_path('missing') == []

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graphql_update(self, graphql_query: GraphQLQueryFunction) -> None:
        """Test updateAnalysisRunnerLog sets the config path"""

        ar_entry = self.get_test_analysis('<ar-guid>')
        ar_entry.config_path = 'SEQERA-PENDING'
        await self.al.insert_analysis_runner_entry(ar_entry)

        new_config_path = 'gs://bucket/<ar-guid>.toml'
        result = await graphql_query(
            UPDATE_MUTATION,
            {'arGuid': '<ar-guid>', 'update': {'configPath': new_config_path}},
        )

        assert result['data']['analysisRunner']['updateAnalysisRunnerLog'] == {
            'arGuid': '<ar-guid>',
            'configPath': new_config_path,
            'project': {'name': self.project_name},
        }
        ar_entries = await self.al.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq='<ar-guid>'))
        )
        assert len(ar_entries) == 1
        assert ar_entries[0].config_path == new_config_path

    @pytest.mark.project_roles(['writer'])
    @pytest.mark.asyncio
    async def test_graphql_update_nonexistent_entry(
        self, graphql_query: GraphQLQueryFunction
    ) -> None:
        """Test updateAnalysisRunnerLog errors for an unknown arGuid"""

        result = await graphql_query(
            UPDATE_MUTATION,
            {'arGuid': '<missing-ar-guid>', 'update': {'configPath': 'gs://x.toml'}},
        )

        assert result['data'] is None
        assert "Couldn't find analysis runner log" in result['errors'][0]['message']
