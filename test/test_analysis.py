# pylint: disable=invalid-overridden-method
from datetime import timedelta, datetime

from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.sample import SampleLayer

from models.models.sample import SampleUpsertInternal
from models.enums import AnalysisStatus


class TestAnalysis(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.sl = SampleLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test01',
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
            )
        )
        self.sample_id = sample.id

        await self.al.insert_analysis(
            analysis_type='cram',
            status=AnalysisStatus.COMPLETED,
            sample_ids=[self.sample_id],
            meta={'sequence_type': 'genome', 'size': 1024},
        )

    @run_as_sync
    async def test_add_cram(self):
        """
        Test adding an analysis of type CRAM
        """

        analyses = await self.connection.connection.fetch_all('SELECT * FROM analysis')
        analysis_samples = await self.connection.connection.fetch_all(
            'SELECT * FROM analysis_sample'
        )
        print(analyses, analysis_samples)

        self.assertEqual(1, len(analyses))

        self.assertEqual(1, analysis_samples[0]['sample_id'])
        self.assertEqual(analyses[0]['id'], analysis_samples[0]['analysis_id'])

    @run_as_sync
    async def test_get_sample_file_sizes(self):
        """
        Test retrieval of sample file sizes over time
        """

        today = datetime.utcnow().date()

        result = await self.al.get_sample_file_sizes(project_ids=[1])
        expected = [
            {
                'project': 1,
                'samples': [
                    {
                        'sample': self.sample_id,
                        'dates': [
                            {
                                'start': today,
                                'end': None,
                                'size': {'genome': 1024},
                            }
                        ],
                    }
                ],
            }
        ]

        # Check output is formatted correctly
        self.assertEqual(1, len(result))
        self.assertDictEqual(expected[0], result[0])

        # Add exome cram
        await self.al.insert_analysis(
            analysis_type='cram',
            status=AnalysisStatus.COMPLETED,
            sample_ids=[self.sample_id],
            meta={'sequence_type': 'exome', 'size': 3141},
        )

        expected[0]['samples'][0]['dates'][0]['size']['exome'] = 3141

        # Assert that the exome size was added correctly
        result = await self.al.get_sample_file_sizes(project_ids=[1])
        self.assertDictEqual(expected[0], result[0])

        # Assert that if we select a date range outside of sample creation date
        # that is doesn't show up in the map
        yesterday = today - timedelta(days=1)
        result = await self.al.get_sample_file_sizes(
            project_ids=[1], end_date=yesterday
        )

        self.assertEqual([], result)

        # Add another genome cram that's newer
        await self.al.insert_analysis(
            analysis_type='cram',
            status=AnalysisStatus.COMPLETED,
            sample_ids=[self.sample_id],
            meta={'sequence_type': 'genome', 'size': 11111},
        )

        expected[0]['samples'][0]['dates'][0]['size']['genome'] = 11111
        result = await self.al.get_sample_file_sizes(project_ids=[1])
        self.assertDictEqual(expected[0], result[0])

        # Add another sample and it's analysis cram as well
        sample_2 = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test02',
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
            )
        )
        await self.al.insert_analysis(
            analysis_type='cram',
            status=AnalysisStatus.COMPLETED,
            sample_ids=[sample_2.id],
            meta={'sequence_type': 'genome', 'size': 987654321},
        )

        sample_2_data = {
            'sample': sample_2.id,
            'dates': [
                {
                    'start': today,
                    'end': None,
                    'size': {'genome': 987654321},
                }
            ],
        }
        expected[0]['samples'].append(sample_2_data)

        result = await self.al.get_sample_file_sizes(project_ids=[1])
        self.assertDictEqual(expected[0], result[0])
