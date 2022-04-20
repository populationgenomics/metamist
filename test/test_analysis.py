# pylint: disable=invalid-overridden-method
from test.testbase import DbIsolatedTest, run_test_as_sync

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.sample import SampleLayer, SampleType
from models.enums import AnalysisType, AnalysisStatus


class TestAnalysis(DbIsolatedTest):
    """Test sample class"""

    @run_test_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        sl = SampleLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        await sl.insert_sample(
            'Test01',
            SampleType.BLOOD,
            meta={'meta': 'meta ;)'},
            active=True,
        )

    @run_test_as_sync
    async def test_add_cram(self):
        """
        Test adding an analysis of type CRAM
        """
        await self.al.insert_analysis(
            analysis_type=AnalysisType.CRAM,
            status=AnalysisStatus.COMPLETED,
            sample_ids=[1],
            meta={},
        )

        analyses = await self.connection.connection.fetch_all('SELECT * FROM analysis')
        analysis_samples = await self.connection.connection.fetch_all(
            'SELECT * FROM analysis_sample'
        )
        print(analyses, analysis_samples)

        self.assertEqual(1, len(analyses))

        self.assertEqual(1, analysis_samples[0]['sample_id'])
        self.assertEqual(analyses[0]['id'], analysis_samples[0]['analysis_id'])
