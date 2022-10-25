from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.search import SearchLayer
from db.python.layers.family import FamilyLayer
from db.python.tables.family_participant import FamilyParticipantTable

from models.enums import SampleType, SearchResponseType
from models.models.sample import sample_id_format


class TestSample(DbIsolatedTest):
    """Test sample class"""

    # tests run in 'sorted by ascii' order
    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.schlay = SearchLayer(self.connection)
        self.slayer = SampleLayer(self.connection)
        self.player = ParticipantLayer(self.connection)
        self.flayer = FamilyLayer(self.connection)

    @run_as_sync
    async def test_search_non_existent_sample_by_internal_id(self):
        """
        Search by CPG sample ID that doesn't exist
        """
        cpg_id = sample_id_format(9_999_999_999)
        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id])
        self.assertEqual(0, len(results))

    @run_as_sync
    async def test_search_unavailable_sample_by_internal_id(self):
        """
        Search by CPG sample ID that you do not have access to
        Mock this in testing by limiting scope to non-existent project IDs
        """
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        cpg_id = sample_id_format(sample_id)

        results = await self.schlay.search(
            query=cpg_id, project_ids=[self.project_id + 1]
        )
        self.assertEqual(1, len(results))

    @run_as_sync
    async def test_search_isolated_sample_by_id(self):
        """
        Search by valid CPG sample ID (special case)
        """
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        cpg_id = sample_id_format(sample_id)
        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id])

        self.assertEqual(1, len(results))
        self.assertEqual(cpg_id, results[0].title)
        self.assertEqual(cpg_id, results[0].data.id)
        self.assertListEqual(['EX001'], results[0].data.sample_external_ids)

    @run_as_sync
    async def test_search_isolated_sample_by_external_id(self):
        """
        Search by External sample ID with no participant / family,
        should only return one result
        """
        sample_id = await self.slayer.insert_sample('EX001', SampleType.BLOOD)
        results = await self.schlay.search(query='EX001', project_ids=[self.project_id])

        cpg_id = sample_id_format(sample_id)

        self.assertEqual(1, len(results))
        result = results[0]
        self.assertEqual(cpg_id, result.title)
        self.assertEqual(cpg_id, result.data.id)
        self.assertListEqual(['EX001'], result.data.sample_external_ids)
        self.assertListEqual([], result.data.participant_external_ids)
        self.assertListEqual([], result.data.family_external_ids)

    @run_as_sync
    async def test_search_participant_isolated(self):
        """
        Search participant w/ no family by External ID
        should only return one result
        """
        p_id = await self.player.create_participant(external_id='PART01')
        results = await self.schlay.search(
            query='PART01', project_ids=[self.project_id]
        )
        self.assertEqual(1, len(results))
        result = results[0]
        self.assertEqual(p_id, result.data.id)
        self.assertEqual('PART01', result.title)
        self.assertListEqual(['PART01'], result.data.participant_external_ids)
        self.assertListEqual([], result.data.family_external_ids)
        self.assertRaises(AttributeError, lambda: result.data.sample_external_ids)

    @run_as_sync
    async def test_search_family(self):
        """
        Search family by External ID
        should only return one result
        """
        f_id = await self.flayer.create_family(external_id='FAMXX01')
        results = await self.schlay.search(
            query='FAMXX01', project_ids=[self.project_id]
        )
        self.assertEqual(1, len(results))
        result = results[0]
        self.assertEqual(f_id, result.data.id)
        self.assertEqual('FAMXX01', result.title)
        self.assertListEqual(['FAMXX01'], result.data.family_external_ids)
        self.assertRaises(AttributeError, lambda: result.data.participant_external_ids)
        self.assertRaises(AttributeError, lambda: result.data.sample_external_ids)

    @run_as_sync
    async def test_search_mixed(self):
        """Create a number of resources, and search for all of them"""
        fptable = FamilyParticipantTable(self.connection)

        p_id = await self.player.create_participant(external_id='X:PART01')
        f_id = await self.flayer.create_family(external_id='X:FAM01')
        await fptable.create_rows(
            [
                {
                    'family_id': f_id,
                    'participant_id': p_id,
                    'paternal_participant_id': None,
                    'maternal_participant_id': None,
                    'affected': 0,
                    'notes': None,
                }
            ]
        )

        s_id = await self.slayer.insert_sample(
            'X:SAM001', SampleType.BLOOD, participant_id=p_id
        )

        all_results = await self.schlay.search(
            query='X:', project_ids=[self.connection.project]
        )
        self.assertEqual(3, len(all_results))

        family_result = next(
            r for r in all_results if r.type == SearchResponseType.FAMILY
        )
        participant_result = next(
            r for r in all_results if r.type == SearchResponseType.PARTICIPANT
        )
        sample_result = next(
            r for r in all_results if r.type == SearchResponseType.SAMPLE
        )

        # linked family matches
        self.assertEqual('X:FAM01', family_result.title)

        # linked participant matches
        self.assertEqual('X:PART01', participant_result.title)
        self.assertListEqual(['X:FAM01'], participant_result.data.family_external_ids)

        # linked sample matches
        cpg_id = sample_id_format(s_id)
        self.assertEqual(cpg_id, sample_result.data.id)
        self.assertListEqual(['X:SAM001'], sample_result.data.sample_external_ids)
        self.assertListEqual(['X:FAM01'], participant_result.data.family_external_ids)
        self.assertListEqual(
            ['X:PART01'], participant_result.data.participant_external_ids
        )
