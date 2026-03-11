import pytest

from db.python.connect import Connection
from db.python.layers.participant import ParticipantLayer
from models.models import PRIMARY_EXTERNAL_ORG, ParticipantUpsertInternal


class TestImportIndividualMetadata:
    """Test importing individual metadata"""

    @pytest.mark.asyncio
    async def test_import_many_hpo_terms(self, connection_with_project: Connection):
        """Test import hpo terms from many columns"""
        pl = ParticipantLayer(connection_with_project)

        await pl.upsert_participant(
            ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'TP01'})
        )

        headers = [
            'Individual ID',
            'HPO Term 1',
            'HPO Term 2',
            'HPO Term 3',
            'HPO Term 20',
        ]
        rows_to_insert = [
            ['TP01', 'HP:0000001', 'HP:0000002', 'HP:0000003', 'HP:0000004']
        ]

        await pl.generic_individual_metadata_importer(headers, rows_to_insert)

        cur = await connection_with_project.pg_connection.execute(
            'SELECT participant_id, description, value FROM participant_phenotypes'
        )
        db_rows = list(await cur.fetchall())

        assert len(db_rows) == 1
        assert db_rows[0]['description'] == 'HPO Terms (present)'
        assert db_rows[0]['value'] == 'HP:0000001,HP:0000002,HP:0000003,HP:0000004'

    @pytest.mark.asyncio
    async def test_import_basic_metadata(self, connection_with_project: Connection):
        """Test basic data for 2 participants and 2 columns"""
        pl = ParticipantLayer(connection_with_project)

        await pl.upsert_participants(
            [
                ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'TP01'}),
                ParticipantUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'TP02'}),
            ]
        )

        headers = ['Individual ID', 'HPO Term 20', 'Age of Onset']
        rows_to_insert = [
            ['TP01', 'HP:0000020', 'Congenital'],
            ['TP02', 'HP:00000021; HP:023', 'Infantile'],
        ]

        await pl.generic_individual_metadata_importer(headers, rows_to_insert)

        cur = await connection_with_project.pg_connection.execute(
            'SELECT participant_id, description, value FROM participant_phenotypes'
        )
        rows = list(await cur.fetchall())

        assert len(rows) == 4

        first_p_rows = rows[:2]
        second_p_rows = rows[2:]

        assert first_p_rows[0]['description'] == 'Age of Onset'
        assert first_p_rows[0]['value'] == 'Congenital onset'
        assert first_p_rows[1]['description'] == 'HPO Terms (present)'
        assert first_p_rows[1]['value'] == 'HP:0000020'

        assert second_p_rows[0]['description'] == 'Age of Onset'
        assert second_p_rows[0]['value'] == 'Infantile onset'
        assert second_p_rows[1]['description'] == 'HPO Terms (present)'
        assert second_p_rows[1]['value'] == 'HP:00000021,HP:023'
