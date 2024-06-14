# pylint: disable=invalid-overridden-method
import datetime
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from models.models.analysis_runner import AnalysisRunnerInternal


class TestAnalysisRunner(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()

        self.al = AnalysisRunnerLayer(self.connection)

    @run_as_sync
    async def test_insert(self):
        """Test insert"""
        analysis = AnalysisRunnerInternal(
            ar_guid='<ar-guid>',
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
        analyses = [
            AnalysisRunnerInternal(
                ar_guid=f'<ar-guid-{i+1}>',
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
            for i in range(3)
        ]
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
