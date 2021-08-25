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
        ignore_keys_during_update = {'participant_id'}

        remapped_ds = []
        for row in dictionaries:
            d = {k: row.get(k) for k in keys}
            d['author'] = author or self.author
            remapped_ds.append(d)

        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f':{k}' for k in keys)
        update_keys = ', '.join(
            f'{k}=:{k}' for k in keys if k not in ignore_keys_during_update
        )
        _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
ON DUPLICATE KEY UPDATE
    {update_keys}
"""
        return await self.connection.execute_many(_query, remapped_ds)

    async def get_rows(
        self, family_ids: Optional[int] = None, project: Optional[int] = None
    ):
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
        keys_str = ', '.join('fp.' + k for k in keys)

        values = {'project': project or self.project}
        wheres = ['project = :project']
        if family_ids:
            wheres.append('family_id in :family_ids')
            values['family_ids'] = family_ids

        conditions = ' AND '.join(wheres)

        _query = f"""
SELECT {keys_str} FROM family_participant fp
INNER JOIN family f ON f.id = fp.family_id
WHERE {conditions}
        """

        rows = await self.connection.fetch_all(_query, values)
        return rows
