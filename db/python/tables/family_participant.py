from collections import defaultdict
from typing import Tuple, List, Dict, Optional, Set, Any

from db.python.tables.base import DbBase
from db.python.tables.project import ProjectId
from models.models.family import PedRowInternal


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
        affected: int,
        notes: str = None,
    ) -> Tuple[int, int]:
        """
        Create a new sample, and add it to database
        """
        updater = {
            'family_id': family_id,
            'participant_id': participant_id,
            'paternal_participant_id': paternal_id,
            'maternal_participant_id': maternal_id,
            'affected': affected,
            'notes': notes,
            'changelog_id': self.changelog_id,
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
        rows: list[PedRowInternal],
    ):
        """
        Create many rows, dictionaries must have keys:
        - family_id
        - participant_id
        - paternal_participant_id
        - maternal_participant_id
        - affected
        - notes
        - author
        """
        ignore_keys_during_update = {'participant_id'}

        remapped_ds_by_keys: Dict[Tuple, List[Dict]] = defaultdict(list)
        # this now works when only a portion of the keys are specified
        for row in rows:
            d: dict[str, Any] = {
                'family_id': row.family_id,
                'participant_id': row.participant_id,
                'paternal_participant_id': row.paternal_id,
                'maternal_participant_id': row.maternal_id,
                'affected': row.affected,
                'notes': row.notes,
                'changelog_id': self.changelog_id,
            }

            remapped_ds_by_keys[tuple(sorted(d.keys()))].append(d)

        for d_keys, remapped_ds in remapped_ds_by_keys.items():
            str_keys = ', '.join(d_keys)
            placeholder_keys = ', '.join(f':{k}' for k in d_keys)
            update_keys = ', '.join(
                f'{k}=:{k}' for k in d_keys if k not in ignore_keys_during_update
            )
            _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
ON DUPLICATE KEY UPDATE
    {update_keys}
    """
            await self.connection.execute_many(_query, remapped_ds)

        return True

    async def get_rows(
        self,
        project: ProjectId,
        family_ids: Optional[List[int]] = None,
        include_participants_not_in_families=False,
    ):
        """
        Get rows from database, return ALL rows unless family_ids is specified.
        If family_ids is not specified, and `include_participants_not_in_families` is True,
            Get all participants from project and include them in pedigree
        """
        keys = [
            'fp.family_id',
            'p.id as individual_id',
            'fp.paternal_participant_id',
            'fp.maternal_participant_id',
            'p.reported_sex as sex',
            'fp.affected',
        ]
        keys_str = ', '.join(keys)

        values: Dict[str, Any] = {'project': project or self.project}
        wheres = ['p.project = :project']
        if family_ids:
            wheres.append('family_id in :family_ids')
            values['family_ids'] = family_ids

        if not include_participants_not_in_families:
            wheres.append('f.project = :project')

        conditions = ' AND '.join(wheres)

        _query = f"""
            SELECT {keys_str} FROM family_participant fp
            INNER JOIN family f ON f.id = fp.family_id
            INNER JOIN participant p on fp.participant_id = p.id
            WHERE {conditions}"""
        if not family_ids and include_participants_not_in_families:
            # rewrite the query to LEFT join from participants
            # to include all participants
            _query = f"""
                SELECT {keys_str} FROM participant p
                LEFT JOIN family_participant fp ON fp.participant_id = p.id
                LEFT JOIN family f ON f.id = fp.family_id
                WHERE {conditions}"""

        rows = await self.connection.fetch_all(_query, values)

        ordered_keys = [
            'family_id',
            'individual_id',
            'paternal_id',
            'maternal_id',
            'sex',
            'affected',
        ]
        ds = [dict(zip(ordered_keys, row)) for row in rows]

        return ds

    async def get_row(
        self,
        family_id: int,
        participant_id: int,
    ):
        """Get a single row from the family_participant table"""
        values: Dict[str, Any] = {
            'family_id': family_id,
            'participant_id': participant_id,
        }

        _query = """
SELECT fp.family_id, p.id as individual_id, fp.paternal_participant_id, fp.maternal_participant_id, p.reported_sex as sex, fp.affected
FROM family_participant fp
INNER JOIN family f ON f.id = fp.family_id
INNER JOIN participant p on fp.participant_id = p.id
WHERE f.id = :family_id AND p.id = :participant_id
"""

        row = await self.connection.fetch_one(_query, values)

        ordered_keys = [
            'family_id',
            'individual_id',
            'paternal_id',
            'maternal_id',
            'sex',
            'affected',
        ]
        ds = dict(zip(ordered_keys, row))

        return ds

    async def get_participant_family_map(
        self, participant_ids: List[int]
    ) -> Tuple[Set[int], Dict[int, int]]:
        """
        Get {participant_id: family_id} map
        w/ projects
        """

        if len(participant_ids) == 0:
            return set(), {}

        _query = """
SELECT p.project, p.id, fp.family_id
FROM family_participant fp
INNER JOIN participant p ON p.id = fp.participant_id
WHERE fp.participant_id in :participant_ids
"""
        rows = await self.connection.fetch_all(
            _query, {'participant_ids': participant_ids}
        )
        projects = set(r['project'] for r in rows)
        m = {r['id']: r['family_id'] for r in rows}

        return projects, m

    async def delete_family_participant_row(self, family_id: int, participant_id: int):
        """
        Delete a participant from a family
        """

        if not participant_id or not family_id:
            return False

        _query = """
DELETE FROM family_participant
WHERE participant_id = :participant_id
AND family_id = :family_id
        """

        await self.connection.execute(
            _query, {'family_id': family_id, 'participant_id': participant_id}
        )

        return True
