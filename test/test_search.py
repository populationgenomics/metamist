from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.search import SearchLayer
from models.enums import SampleType
from models.models.sample import sample_id_format
from test.testbase import DbIsolatedTest, run_as_sync


class TestSample(DbIsolatedTest):
    """Test sample class"""

    # tests run in 'sorted by ascii' order
    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.schlay = SearchLayer(self.connection)
        self.slayer = SampleLayer(self.connection)
        self.player = ParticipantLayer(self.connection)

    @run_as_sync
    async def test_search_isolated_sample_by_id(self):
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        cpg_id = sample_id_format(sample_id)
        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id])

        self.assertEqual(1, len(results))
        self.assertEqual(cpg_id, results[0].title)
        self.assertEqual(cpg_id, results[0].data.id)
        self.assertListEqual(['EX001'], results[0].data.sample_external_ids)

    @run_as_sync
    async def test_search_isolated_sample_by_external_id(self):
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        results = await self.schlay.search(query='EX001', project_ids=[self.project_id])

        cpg_id = sample_id_format(sample_id)

        self.assertEqual(1, len(results))
        self.assertEqual(cpg_id, results[0].title)
        self.assertEqual(sample_id, results[0].data.id)
        self.assertListEqual(['EX001'], results[0].data.sample_external_ids)

    @run_as_sync
    async def test_search_non_existent_sample_by_internal_id(self):
        # hopefully this is never reached in testing
        cpg_id = sample_id_format(9_999_999_999)
        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id])
        self.assertEqual(0, len(results))

    @run_as_sync
    async def test_search_unavailable_sample_by_internal_id(self):
        # hopefully this is never reached in testing
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        cpg_id = sample_id_format(sample_id)

        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id + 1])
        self.assertEqual(1, len(results))

    @run_as_sync
    async def test_search_participant_isolated(self):
        p_id = await self.player.create_participant(external_id='PART01')
        results = await self.schlay.search(query='PART01', project_ids=[self.project_id])
        self.assertEqual(1, len(results))
        result = results[0]
        self.assertEqual(p_id, result.data.id)
        self.assertEqual('PART01', result.title)
        self.assertListEqual([], result.data.family_external_ids)








