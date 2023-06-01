from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.participant import ParticipantLayer
from db.python.layers.family import FamilyLayer


class TestParticipantFamily(DbIsolatedTest):
    """Test getting participants"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        fl = FamilyLayer(self.connection)
        await fl.create_family(external_id='FAM01')
        await fl.create_family(external_id='FAM02')

        pl = ParticipantLayer(self.connection)
        pid = await pl.create_participant(external_id='EX01', reported_sex=2)
        await pl.create_participant(external_id='EX01_pat', reported_sex=1)
        await pl.create_participant(external_id='EX01_mat', reported_sex=2)

        await pl.add_participant_to_family(
            family_id=1, participant_id=pid, paternal_id=2, maternal_id=3, affected=2
        )

    @run_as_sync
    async def test_get_and_update_family_participant_data(self):
        """
        Tests getting and updating a participants family data
        """
        pl = ParticipantLayer(self.connection)

        p = await pl.get_participants(project=1)
        pid = p[0].id

        fp_row = await pl.get_family_participant_data(family_id=1, participant_id=pid)

        expected_fp_row = {
            'family_id': 1,
            'individual_id': pid,
            'paternal_id': 2,
            'maternal_id': 3,
            'sex': 2,
            'affected': 2,
        }
        self.assertDictEqual(fp_row, expected_fp_row)

        await pl.remove_participant_from_family(family_id=1, participant_id=pid)

        await pl.add_participant_to_family(
            family_id=2,
            participant_id=pid,
            paternal_id=fp_row['paternal_id'],
            maternal_id=fp_row['maternal_id'],
            affected=fp_row['affected'],
        )

        updated_fp_row = await pl.get_family_participant_data(
            family_id=2, participant_id=pid
        )

        expected_updated_fp_row = {
            'family_id': 2,
            'individual_id': pid,
            'paternal_id': 2,
            'maternal_id': 3,
            'sex': 2,
            'affected': 2,
        }
        self.assertDictEqual(updated_fp_row, expected_updated_fp_row)
