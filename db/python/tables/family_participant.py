from typing import Tuple, List, Dict, Optional

from db.python.connect import DbBase


class FamilyParticipantTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'family_participant'

    async def create_row(
        self,
        family_id: int,
        participant_id: int,
        paternal_id: int,
        maternal_id: int,
        sex: int,
        affected: int,
        notes: str = None,
        author=None,
    ) -> Tuple[int, int]:
        """
        Create a new sample, and add it to database
        """
        updater = {
            'family_id': family_id,
            'participant_id': participant_id,
            'paternal_participant_id': paternal_id,
            'maternal_participant_id': maternal_id,
            'sex': sex,
            'affected': affected,
            'notes': notes,
            'author': author or self.author,
        }
        keys = list(updater.keys())
        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f':{k}' for k in keys)
        _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
        """

        await self.connection.execute(_query, updater)

        return family_id, participant_id

    async def create_rows(
        self,
        dictionaries: List[Dict[str, str]],
        author=None,
    ):
        """
        Create many rows, dictionaries must have keys:
        - family_id
        - participant_id
        - paternal_participant_id
        - maternal_participant_id
        - sex
        - affected
        - notes
        - author
        """
        keys = {
            'family_id',
            'participant_id',
            'paternal_participant_id',
            'maternal_participant_id',
            'sex',
            'affected',
            'notes',
            'author',
        }
        remapped_ds = []
        for row in dictionaries:
            d = {k: row.get(k) for k in keys}
            d['author'] = author or self.author
            remapped_ds.append(d)

        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f':{k}' for k in keys)
        _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
"""
        return await self.connection.execute_many(_query, remapped_ds)

    async def get_rows(self, family_ids: Optional[int] = None):
        """
        Get rows from database, return ALL rows unless family_ids is specified.
        """
        keys = [
            'family_id',
            'participant_id',
            'paternal_participant_id',
            'maternal_participant_id',
            'sex',
            'affected',
        ]
        keys_str = ', '.join(keys)
        _query = f'SELECT {keys_str} FROM family_participant'
        values = {}
        if family_ids:
            _query += ' WHERE family_id in :family_ids'
            values = {'family_ids': family_ids}

        rows = await self.connection.fetch_all(_query, values)
        return rows
