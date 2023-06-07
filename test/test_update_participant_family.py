from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.participant import ParticipantLayer
from db.python.layers.family import FamilyLayer


class TestParticipantFamily(DbIsolatedTest):
    """Test moving a participant from one family to another and then back"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        fl = FamilyLayer(self.connection)

        self.fid_1 = await fl.create_family(external_id='FAM01')
        self.fid_2 = await fl.create_family(external_id='FAM02')

        pl = ParticipantLayer(self.connection)
        self.pid = await pl.create_participant(external_id='EX01', reported_sex=2)
        self.pat_pid = await pl.create_participant(
            external_id='EX01_pat', reported_sex=1
        )
        self.mat_pid = await pl.create_participant(
            external_id='EX01_mat', reported_sex=2
        )

        await pl.add_participant_to_family(
            family_id=self.fid_1,
            participant_id=self.pid,
            paternal_id=self.pat_pid,
            maternal_id=self.mat_pid,
            affected=2,
        )

    @run_as_sync
    async def test_get_remove_add_family_participant_data(self):
        """
        Tests getting, removing, and adding a participants family data
        """
        pl = ParticipantLayer(self.connection)

        fp_row = await pl.get_family_participant_data(
            family_id=self.fid_1, participant_id=self.pid
        )

        expected_fp_row = {
            'family_id': self.fid_1,
            'individual_id': self.pid,
            'paternal_id': self.pat_pid,
            'maternal_id': self.mat_pid,
            'sex': 2,
            'affected': 2,
        }
        self.assertDictEqual(fp_row, expected_fp_row)

        await pl.remove_participant_from_family(
            family_id=self.fid_1, participant_id=self.pid
        )

        await pl.add_participant_to_family(
            family_id=self.fid_2,
            participant_id=self.pid,
            paternal_id=fp_row['paternal_id'],
            maternal_id=fp_row['maternal_id'],
            affected=fp_row['affected'],
        )

        updated_fp_row = await pl.get_family_participant_data(
            family_id=self.fid_2, participant_id=self.pid
        )

        expected_updated_fp_row = {
            'family_id': self.fid_2,
            'individual_id': self.pid,
            'paternal_id': self.pat_pid,
            'maternal_id': self.mat_pid,
            'sex': 2,
            'affected': 2,
        }
        self.assertDictEqual(updated_fp_row, expected_updated_fp_row)

    @run_as_sync
    async def test_update_participant_family(self):
        """Tests updating a participants family data"""
        pl = ParticipantLayer(self.connection)

        pl.update_participant_family(
            participant_id=self.pid, old_family_id=self.fid_2, new_family_id=self.fid_1
        )

        updated_fp_row = await pl.get_family_participant_data(
            family_id=self.fid_1, participant_id=self.pid
        )

        expected_updated_fp_row = {
            'family_id': self.fid_1,
            'individual_id': self.pid,
            'paternal_id': self.pat_pid,
            'maternal_id': self.mat_pid,
            'sex': 2,
            'affected': 2,
        }
        self.assertDictEqual(updated_fp_row, expected_updated_fp_row)
