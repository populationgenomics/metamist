# pylint: disable=invalid-overridden-method
from datetime import date, timedelta

from test.testbase import DbIsolatedTest, run_test_as_sync

from models.enums.sequencing import SequenceType
from models.enums import AnalysisType, AnalysisStatus

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.sample import SampleLayer, SampleType


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

    @run_test_as_sync
    async def test_get_sample_file_sizes(self):
        """
        Test retrieval of sample file sizes over time
        """

        await self.al.insert_analysis(
            analysis_type=AnalysisType.CRAM,
            status=AnalysisStatus.COMPLETED,
            sample_ids=[1],
            meta={'sequence_type': 'genome', 'size': 1024},
        )

        result = await self.al.get_sample_file_sizes(project_ids=[1])
        expected = [
            {
                'project': 1,
                'samples': [
                    {
                        'sample': 1,
                        'dates': [
                            {
                                'start': date.today(),
                                'end': None,
                                'size': {SequenceType.GENOME: 1024},
                            }
                        ],
                    }
                ],
            }
        ]

        # Check output is formatted correctly
        self.assertDictEqual(expected[0], result[0])

        # Add exome cram
        await self.al.insert_analysis(
            analysis_type=AnalysisType.CRAM,
            status=AnalysisStatus.COMPLETED,
            sample_ids=[1],
            meta={'sequence_type': 'exome', 'size': 3141},
        )

        expected[0]['samples'][0]['dates'][0]['size'][SequenceType.EXOME] = 3141

        # Assert that the exome size was added correctly
        result = await self.al.get_sample_file_sizes(project_ids=[1])
        self.assertDictEqual(expected[0], result[0])

        # Assert that if we select a date range outside of sample creation date
        # that is doesn't show up in the map
        yesterday = date.today() - timedelta(days=1)
        result = await self.al.get_sample_file_sizes(
            project_ids=[1], end_date=yesterday
        )

        self.assertEqual([], [])
