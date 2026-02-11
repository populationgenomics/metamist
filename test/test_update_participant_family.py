import pytest
from psycopg import IntegrityError

from db.python.connect import Connection
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from models.models import PRIMARY_EXTERNAL_ORG, ParticipantUpsertInternal


class TestParticipantFamily:
    """Test moving a participant from one family to another and then back"""

    @pytest.fixture(autouse=True)
    @pytest.mark.project_roles(['reader', 'writer'])
    async def set_up(self, connection_with_project: Connection) -> None:
        fl = FamilyLayer(connection_with_project)

        self.fid_1 = await fl.create_family(external_ids={'forg': 'FAM01'})
        self.fid_2 = await fl.create_family(external_ids={'forg': 'FAM02'})
        # Also exercise update_family()
        await fl.update_family(self.fid_2, external_ids={'otherorg': 'OFAM02'})

        pl = ParticipantLayer(connection_with_project)
        self.pid = (
            await pl.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'}, reported_sex=2
                )
            )
        ).id
        self.pat_pid = (
            await pl.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_pat'}, reported_sex=1
                )
            )
        ).id
        self.mat_pid = (
            await pl.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_mat'}, reported_sex=2
                )
            )
        ).id

        await pl.add_participant_to_family(
            family_id=self.fid_1,
            participant_id=self.pid,
            paternal_id=self.pat_pid,
            maternal_id=self.mat_pid,
            affected=2,
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_remove_add_family_participant_data(
        self, connection_with_project: Connection
    ):
        """
        Tests getting, removing, and adding a participants family data
        """
        pl = ParticipantLayer(connection_with_project)

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
            'notes': None,
        }
        assert expected_fp_row == fp_row.to_dict()

        await pl.remove_participant_from_family(
            family_id=self.fid_1, participant_id=self.pid
        )

        await pl.add_participant_to_family(
            family_id=self.fid_2,
            participant_id=self.pid,
            paternal_id=fp_row.paternal_id,
            maternal_id=fp_row.maternal_id,
            affected=fp_row.affected,
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
            'notes': None,
        }
        assert expected_updated_fp_row == updated_fp_row.to_dict()

        await pl.remove_participant_from_family(
            family_id=self.fid_2, participant_id=self.pid
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update_participant_family(self, connection_with_project: Connection):
        """Tests updating a participants family data"""
        pl = ParticipantLayer(connection_with_project)
        await pl.update_participant_family(
            participant_id=self.pid, old_family_id=self.fid_1, new_family_id=self.fid_2
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
            'notes': None,
        }
        assert expected_updated_fp_row == updated_fp_row.to_dict()

        await pl.remove_participant_from_family(
            family_id=self.fid_2, participant_id=self.pid
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update_participant_to_nonexistent_family(
        self, connection_with_project: Connection
    ):
        """Tests if error is raised and transaction rolled back for nonexistent new_family_id"""
        pl = ParticipantLayer(connection_with_project)

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
            'notes': None,
        }
        assert expected_fp_row == fp_row.to_dict()

        with pytest.raises(IntegrityError):
            await pl.update_participant_family(
                participant_id=self.pid, old_family_id=self.fid_1, new_family_id=-99
            )

        rollback_fp_row = await pl.get_family_participant_data(
            family_id=self.fid_1, participant_id=self.pid
        )

        # Update transaction should rollback, so no change expected
        assert expected_fp_row == rollback_fp_row.to_dict()
