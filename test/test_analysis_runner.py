import datetime

import pytest

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from models.models.analysis_runner import AnalysisRunnerInternal


class TestAnalysisRunner:
    """Test sample class"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection) -> None:
        self.al = AnalysisRunnerLayer(connection_with_project)
        self.project_id = connection_with_project.project_id

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
    async def test_query_throws_error_for_empty_filters(self):
        """
        Test that the query throws an error if the filters are empty
        """

        analysis = self.get_test_analysis('<ar-guid>')
        await self.al.insert_analysis_runner_entry(analysis)

        with pytest.raises(ValueError):
            _ = await self.al.query(AnalysisRunnerFilter())
