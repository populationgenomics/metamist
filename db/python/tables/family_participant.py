import dataclasses
from collections import defaultdict
from typing import Any

from psycopg.rows import class_row

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from models.models.family import PedRowInternal
from models.models.project import ProjectId


@dataclasses.dataclass
class FamilyParticipantFilter(GenericFilterModel):
    """Filter for family_participant table"""

    project: GenericFilter[ProjectId] | None = None
    participant_id: GenericFilter[int] | None = None
    family_id: GenericFilter[int] | None = None


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
        notes: str | None = None,
    ) -> tuple[int, int]:
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
            'audit_log_id': await self.audit_log_id(),
        }
        keys = list(updater.keys())
        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f'%({k})s' for k in keys)
        _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
        """

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(_query, updater)

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

        remapped_ds_by_keys: dict[tuple, list[dict]] = defaultdict(list)
        # this now works when only a portion of the keys are specified
        for row in rows:
            d: dict[str, Any] = {
                'family_id': row.family_id,
                'participant_id': row.individual_id,
                'paternal_participant_id': row.paternal_id,
                'maternal_participant_id': row.maternal_id,
                'affected': row.affected,
                'notes': row.notes,
                # sex is NOT inserted here
                'audit_log_id': await self.audit_log_id(),
            }

            remapped_ds_by_keys[tuple(sorted(d.keys()))].append(d)

        for d_keys, remapped_ds in remapped_ds_by_keys.items():
            str_keys = ', '.join(d_keys)
            placeholder_keys = ', '.join(f'%({k})s' for k in d_keys)
            update_keys = ', '.join(
                f'{k}=EXCLUDED.{k}'
                for k in d_keys
                if k not in ignore_keys_during_update
            )

            # TODO:piyumi ON CONFLICT(participant_id), for primary key
            _query = f"""
INSERT INTO family_participant
    ({str_keys})
VALUES
    ({placeholder_keys})
ON CONFLICT(participant_id)
DO UPDATE SET
    {update_keys}
    """
            async with self.connection.pool.connection() as conn:
                async with conn.cursor() as curr:
                    await curr.execute(_query, remapped_ds)

        return True

    async def query(
        self,
        filter_: FamilyParticipantFilter,
        include_participants_not_in_families: bool = False,
    ) -> tuple[set[ProjectId], list[PedRowInternal]]:
        """
        Query the family_participant table
        """

        wheres, values = filter_.to_sql()  # TODO: fix this util function
        if not wheres:
            raise ValueError('No filter provided')

        join_type = 'LEFT' if include_participants_not_in_families else 'INNER'
        query = f"""
        SELECT
            fp.family_id,
            p.id as individual_id,
            fp.paternal_participant_id as paternal_id,
            fp.maternal_participant_id as maternal_id,
            p.reported_sex as sex,
            fp.affected,
            fp.notes as notes,
            p.project
        FROM participant p
        {join_type} JOIN family_participant fp on fp.participant_id = p.id
        WHERE {wheres}
        """

        async with self.connection.pool.connection() as conn:
            async with conn.cursor(row_factory=class_row(PedRowInternal)) as curr:
                await curr.execute(query, values)
                ped_row_internal_list = await curr.fetchall()

        projects: set[ProjectId] = set()
        pedrows: list[PedRowInternal] = []
        for ped_row_internal in ped_row_internal_list:
            projects.add(ped_row_internal.project)
            pedrows.append(ped_row_internal)

        return projects, pedrows

    async def get_participant_family_map(
        self,
        participant_ids: list[int],
    ) -> tuple[set[int], dict[int, int]]:
        """
        Get {participant_id: family_id} map
        """

        if len(participant_ids) == 0:
            return set(), {}

        _query = """
SELECT p.project, p.id, fp.family_id
FROM family_participant fp
INNER JOIN participant p ON p.id = fp.participant_id
WHERE fp.participant_id in :participant_ids
"""
        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(_query, {'participant_ids': participant_ids})
                rows = await curr.fetchall()

        projects = set(r['project'] for r in rows)
        conflicts: dict[int, list[int]] = {}
        pid_to_fid_map: dict[int, int] = {}
        for r in rows:
            r_id = r['id']

            if r_id in pid_to_fid_map:
                if r_id not in conflicts:
                    conflicts[r_id] = [pid_to_fid_map[r_id]]
                conflicts[r_id].append(r['family_id'])

            pid_to_fid_map[r_id] = r['family_id']

        if conflicts:
            raise ValueError(
                f'Participants were found in more than one family ({{pid: [fids]}}): {conflicts}'
            )

        return projects, pid_to_fid_map

    async def delete_family_participant_row(self, family_id: int, participant_id: int):
        """
        Delete a participant from a family
        """

        if not participant_id or not family_id:
            return False

        _update_before_delete = """
        UPDATE family_participant
        SET audit_log_id = %(audit_log_id)s
        WHERE family_id = %(family_id)s AND participant_id = %(participant_id)s
        """

        _query = """
        DELETE FROM family_participant
        WHERE participant_id = %(participant_id)s AND family_id = %(family_id)s
        """

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as cur:
                # committed separately as autocommit = True
                cur.execute(
                    _update_before_delete,
                    {
                        'family_id': family_id,
                        'participant_id': participant_id,
                        'audit_log_id': await self.audit_log_id(),
                    },
                )

                cur.execute(
                    _query, {'family_id': family_id, 'participant_id': participant_id}
                )

        return True
