from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.participant import ParticipantLayer
from models.models.participant import ParticipantUpsertInternal


class TestParticipant(DbIsolatedTest):
    """Test getting participants"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        pl = ParticipantLayer(self.connection)
        await pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_id='EX01',
                    reported_sex=2,
                    karyotype='XX',
                    meta={'field': 1},
                ),
                ParticipantUpsertInternal(
                    external_id='EX02',
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                ),
            ]
        )

    @run_as_sync
    async def test_get_all_participants(self):
        """Test getting all participants"""
        pl = ParticipantLayer(self.connection)
        ps = await pl.get_participants(project=1)

        self.assertEqual(2, len(ps))

        self.assertEqual('EX01', ps[0].external_id)
        self.assertEqual(1, ps[0].meta['field'])
        self.assertEqual('XX', ps[0].karyotype)

        self.assertEqual('EX02', ps[1].external_id)

    @run_as_sync
    async def test_get_participant_by_eid(self):
        """Test to see what's in the database"""
        pl = ParticipantLayer(self.connection)
        ps = await pl.get_participants(project=1, external_participant_ids=['EX02'])

        self.assertEqual(1, len(ps))

        self.assertEqual('EX02', ps[0].external_id)
        self.assertEqual(2, ps[0].meta['field'])
        self.assertEqual('XY', ps[0].karyotype)
