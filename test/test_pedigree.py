import pytest

from db.python.connect import Connection
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from models.models import PRIMARY_EXTERNAL_ORG, ParticipantUpsertInternal


class TestPedigree:
    """Pedigree testing methods"""

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_import_get_pedigree(self, connection_with_project: Connection):
        """Test import + get pedigree"""
        fl = FamilyLayer(connection_with_project)

        rows: list[list[str]] = [
            ['FAM01', 'EX01_father', '', '', '1', '1'],
            ['FAM01', 'EX01_mother', '', '', '2', '1'],
            ['FAM01', 'EX01_subject', 'EX01_father', 'EX01_mother', '1', '2'],
        ]

        await fl.import_pedigree(
            header=None, rows=rows, create_missing_participants=True
        )

        pedigree_dicts = await fl.get_pedigree(
            project=connection_with_project.project_id,
            replace_with_participant_external_ids=True,
            replace_with_family_external_ids=True,
        )

        by_key = {r['individual_id']: r for r in pedigree_dicts}

        assert len(pedigree_dicts) == 3
        father = by_key['EX01_father']
        mother = by_key['EX01_mother']
        subject = by_key['EX01_subject']

        assert father['paternal_id'] is None
        assert mother['paternal_id'] is None
        assert subject['paternal_id'] == 'EX01_father'
        assert subject['maternal_id'] == 'EX01_mother'

    @pytest.mark.asyncio
    async def test_pedigree_without_family(self, connection_with_project: Connection):
        """
        Test getting pedigree where participants do not belong to a family
        """
        pl = ParticipantLayer(connection_with_project)
        fl = FamilyLayer(connection_with_project)

        await pl.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
                reported_sex=1,
            )
        )
        await pl.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'EX02'}, reported_sex=None
            )
        )

        rows = await fl.get_pedigree(
            project=connection_with_project.project_id,
            include_participants_not_in_families=True,
            replace_with_participant_external_ids=True,
        )

        by_id = {r['individual_id']: r for r in rows}
        assert len(rows) == 2
        assert by_id['EX01']['sex'] == 1
        assert by_id['EX02']['sex'] is None
