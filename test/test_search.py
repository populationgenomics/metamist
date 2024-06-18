from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.search import SearchLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.family_participant import FamilyParticipantTable
from models.enums import SearchResponseType
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    FamilySearchResponseData,
    ParticipantSearchResponseData,
    ParticipantUpsertInternal,
    PedRowInternal,
    SampleSearchResponseData,
    SampleUpsertInternal,
    SequencingGroupSearchResponseData,
    SequencingGroupUpsertInternal,
)
from models.models.sample import sample_id_format
from models.models.sequencing_group import sequencing_group_id_format


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
        self.sglayer = SequencingGroupLayer(self.connection)

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
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EX001'}, type='blood')
        )
        cpg_id = sample_id_format(sample.id)

        results = await self.schlay.search(
            query=cpg_id, project_ids=[self.project_id + 1]
        )
        self.assertEqual(1, len(results))

    @run_as_sync
    async def test_search_isolated_sample_by_id(self):
        """
        Search by valid CPG sample ID (special case)
        """
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EX001'}, type='blood')
        )
        cpg_id = sample_id_format(sample.id)
        results = await self.schlay.search(query=cpg_id, project_ids=[self.project_id])

        self.assertEqual(1, len(results))
        self.assertEqual(cpg_id, results[0].title)
        self.assertEqual(cpg_id, results[0].data.id)

        result_data = results[0].data
        self.assertIsInstance(result_data, SampleSearchResponseData)
        assert isinstance(result_data, SampleSearchResponseData)
        self.assertListEqual(['EX001'], result_data.sample_external_ids)

    @run_as_sync
    async def test_search_isolated_sequencing_group_by_id(self):
        """
        Search by valid CPG sequencing group ID (special case)
        """
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EXS001'}, type='blood')
        )
        sg = await self.sglayer.upsert_sequencing_groups(
            [
                SequencingGroupUpsertInternal(
                    sample_id=sample.id,
                    technology='long-read',
                    platform='illumina',
                    meta={
                        'sequencing_type': 'transcriptome',
                        'sequencing_technology': 'long-read',
                        'sequencing_platform': 'illumina',
                    },
                    type='transcriptome',
                    assays=[
                        AssayUpsertInternal(
                            type='sequencing',
                            meta={
                                'sequencing_type': 'transcriptome',
                                'sequencing_technology': 'long-read',
                                'sequencing_platform': 'illumina',
                            },
                        )
                    ],
                )
            ]
        )
        cpg_sg_id = sequencing_group_id_format(sg[0].id)
        results = await self.schlay.search(
            query=cpg_sg_id, project_ids=[self.project_id]
        )

        self.assertEqual(1, len(results))
        self.assertEqual(cpg_sg_id, results[0].title)
        result_data = results[0].data
        assert isinstance(result_data, SequencingGroupSearchResponseData)
        self.assertIsInstance(result_data, SequencingGroupSearchResponseData)
        self.assertEqual(cpg_sg_id, result_data.id)
        self.assertEqual(cpg_sg_id, result_data.sg_external_id)

    @run_as_sync
    async def test_search_isolated_sample_by_external_id(self):
        """
        Search by External sample ID with no participant / family,
        should only return one result
        """
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EX001'}, type='blood')
        )
        results = await self.schlay.search(query='EX001', project_ids=[self.project_id])

        cpg_id = sample_id_format(sample.id)

        self.assertEqual(1, len(results))

        self.assertEqual(cpg_id, results[0].title)
        result_data = results[0].data

        self.assertIsInstance(result_data, SampleSearchResponseData)
        assert isinstance(result_data, SampleSearchResponseData)
        self.assertEqual(cpg_id, result_data.id)
        self.assertListEqual(['EX001'], result_data.sample_external_ids)
        self.assertListEqual([], result_data.participant_external_ids)
        self.assertListEqual([], result_data.family_external_ids)

    @run_as_sync
    async def test_search_participant_isolated(self):
        """
        Search participant w/ no family by External ID
        should only return one result
        """
        p = await self.player.upsert_participant(
            ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'PART01'})
        )
        results = await self.schlay.search(
            query='PART01', project_ids=[self.project_id]
        )
        self.assertEqual(1, len(results))

        self.assertEqual('PART01', results[0].title)
        result_data = results[0].data
        assert isinstance(result_data, ParticipantSearchResponseData)
        self.assertEqual(p.id, result_data.id)
        self.assertListEqual(['PART01'], result_data.participant_external_ids)
        self.assertListEqual([], result_data.family_external_ids)

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
        self.assertEqual('FAMXX01', result.title)
        result_data = result.data
        assert isinstance(result_data, FamilySearchResponseData)
        self.assertEqual(f_id, result_data.id)
        self.assertListEqual(['FAMXX01'], result_data.family_external_ids)

    @run_as_sync
    async def test_search_mixed(self):
        """Create a number of resources, and search for all of them"""
        fptable = FamilyParticipantTable(self.connection)

        p = await self.player.upsert_participant(
            ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'X:PART01'})
        )
        f_id = await self.flayer.create_family(external_id='X:FAM01')
        await fptable.create_rows(
            [
                PedRowInternal(
                    family_id=f_id,
                    individual_id=p.id,
                    paternal_id=None,
                    maternal_id=None,
                    affected=0,
                    sex=0,
                    notes=None,
                )
            ]
        )

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'X:SAM001'},
                type='blood',
                participant_id=p.id,
            )
        )

        all_results = await self.schlay.search(
            query='X:', project_ids=[self.connection.project_id]
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
        family_result_data = family_result.data
        participant_result_data = participant_result.data
        sample_result_data = sample_result.data

        assert isinstance(family_result_data, FamilySearchResponseData)
        assert isinstance(participant_result_data, ParticipantSearchResponseData)
        assert isinstance(sample_result_data, SampleSearchResponseData)

        # linked family matches
        self.assertEqual('X:FAM01', family_result.title)

        # linked participant matches
        self.assertEqual('X:PART01', participant_result.title)
        self.assertListEqual(['X:FAM01'], participant_result_data.family_external_ids)

        # linked sample matches
        cpg_id = sample_id_format(sample.id)
        self.assertEqual(cpg_id, sample_result_data.id)
        self.assertListEqual(['X:SAM001'], sample_result_data.sample_external_ids)
        self.assertListEqual(['X:FAM01'], participant_result_data.family_external_ids)
        self.assertListEqual(
            ['X:PART01'], participant_result_data.participant_external_ids
        )
