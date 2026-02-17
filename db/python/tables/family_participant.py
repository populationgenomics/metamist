import dataclasses
from string.templatelib import Template

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
        audit_log_id = await self.audit_log_id()

        _query = t"""INSERT INTO family_participant (family_id, participant_id, paternal_participant_id,
        maternal_participant_id, affected, notes, audit_log_id)
        VALUES ({family_id}, {participant_id}, {paternal_id}, {maternal_id}, {affected}, {notes}, {audit_log_id})"""

        await self.connection.pg_connection.execute(_query)
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
        upsert_values = []
        for row in rows:
            upsert_values.append(
                {
                    'family_id': row.family_id,
                    'participant_id': row.individual_id,
                    'paternal_participant_id': row.paternal_id,
                    'maternal_participant_id': row.maternal_id,
                    'affected': row.affected,
                    'notes': row.notes,
                    # sex is NOT inserted here
                    'audit_log_id': await self.audit_log_id(),
                }
            )

        _query = """
        INSERT INTO family_participant (family_id, participant_id, paternal_participant_id, maternal_participant_id, affected, notes, audit_log_id)
        VALUES
        (%(family_id)s, %(participant_id)s, %(paternal_participant_id)s, %(maternal_participant_id)s, %(affected)s, %(notes)s, %(audit_log_id)s)
        ON CONFLICT(participant_id)
        DO UPDATE SET
            family_id=EXCLUDED.family_id,
            paternal_participant_id=EXCLUDED.paternal_participant_id,
            maternal_participant_id=EXCLUDED.maternal_participant_id,
            affected=EXCLUDED.affected,
            notes=EXCLUDED.notes,
            audit_log_id=EXCLUDED.audit_log_id
        """
        async with self.connection.pg_connection.cursor() as cur:
            await cur.executemany(_query, upsert_values)

        return True

    async def query(
        self,
        filter_: FamilyParticipantFilter,
        include_participants_not_in_families: bool = False,
    ) -> tuple[set[ProjectId], list[PedRowInternal]]:
        """
        Query the family_participant table
        """

        where_params: Template = filter_.to_sql()
        if not list(where_params):
            raise ValueError('No filter provided')

        join_type = t'LEFT' if include_participants_not_in_families else t'INNER'
        query = t"""
            SELECT fp.family_id, p.id as individual_id, fp.paternal_participant_id as paternal_id,
            fp.maternal_participant_id as maternal_id, p.reported_sex as sex, fp.affected,
            fp.notes as notes, p.project FROM participant p
            {join_type:q}
            JOIN family_participant fp on fp.participant_id = p.id
            WHERE {where_params:q}
        """

        rows = await (await self.connection.pg_connection.execute(query)).fetchall()
        projects: set[ProjectId] = set()
        pedrows: list[PedRowInternal] = []
        for row in rows:
            projects.add(row.pop('project'))
            pedrows.append(PedRowInternal(**row))

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

        _query = t"""
            SELECT p.project, p.id, fp.family_id FROM family_participant fp
            INNER JOIN participant p ON p.id = fp.participant_id
            WHERE fp.participant_id = ANY({participant_ids})
        """

        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()

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

        audit_log_id = await self.audit_log_id()

        _update_before_delete = t"""
        UPDATE family_participant SET audit_log_id = {audit_log_id}
        WHERE family_id = {family_id} AND participant_id = {participant_id}
        """
        _delete_query = t'DELETE FROM family_participant WHERE participant_id = {participant_id} AND family_id = {family_id}'

        async with self.connection.pg_connection.transaction():
            await self.connection.pg_connection.execute(_update_before_delete)
            await self.connection.pg_connection.execute(_delete_query)

        return True
