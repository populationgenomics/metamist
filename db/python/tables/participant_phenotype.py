import json
from collections import defaultdict
from typing import Any

from psycopg.types.json import Jsonb

from db.python.tables.base import DbBase


class ParticipantPhenotypeTable(DbBase):
    """
    Capture Participant_Phenotype table operations and queries
    """

    table_name = 'participant_phenotype'

    async def add_key_value_rows(self, rows: list[tuple[int, str, Any]]) -> None:
        """
        Create a new sample, and add it to database
        """
        if not rows:
            return None
        _query = """
            MERGE INTO participant_phenotypes AS target
            USING (
                VALUES (%(participant_id)s, %(description)s, %(value)s, %(audit_log_id)s, 'DESCRIPTION')
            ) AS source (participant_id, description, value, audit_log_id, hpo_term)
            ON target.participant_id = source.participant_id
            AND LOWER(target.description) = LOWER(source.description)
            AND target.hpo_term = source.hpo_term
            WHEN MATCHED THEN
                UPDATE SET
                    description = source.description,
                    value = source.value,
                    audit_log_id = source.audit_log_id
            WHEN NOT MATCHED THEN
                INSERT (participant_id, description, value, audit_log_id, hpo_term)
                VALUES (source.participant_id, source.description, source.value, source.audit_log_id, source.hpo_term)
        """

        audit_log_id = await self.audit_log_id()

        conn = self.connection.pg_connection
        async with conn.cursor() as cur:
            return await cur.executemany(
                _query,
                [
                    {
                        'participant_id': r[0],
                        'description': r[1],
                        'value': Jsonb(r[2]),
                        'audit_log_id': audit_log_id,
                    }
                    for r in rows
                ],
            )

    async def get_key_value_rows_for_participant_ids(
        self, participant_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        """
        Get (participant_id, description, value),
        for individual level metadata template,
        for specified participant ids
        """
        if len(participant_ids) == 0:
            return {}

        _query = t"""
            SELECT participant_id, description, value
            FROM participant_phenotypes
            WHERE participant_id = ANY({participant_ids}) AND value IS NOT NULL
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        formed_key_value_pairs: dict[int, dict[str, Any]] = defaultdict(dict)
        for row in rows:
            pid = row['participant_id']
            key = row['description']
            value = row['value']
            formed_key_value_pairs[pid][key] = value

        return formed_key_value_pairs

    async def get_key_value_rows_for_all_participants(
        self, project: int
    ) -> dict[int, dict[str, Any]]:
        """
        Get (participant_id, description, value),
        for individual level metadata template,
        for all participants in project
        """
        _query = t"""
            SELECT pp.participant_id, pp.description, pp.value
            FROM participant_phenotypes pp
            INNER JOIN participant p ON p.id = pp.participant_id
            WHERE p.project = {project} AND pp.value IS NOT NULL
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        formed_key_value_pairs: dict[int, dict[str, Any]] = defaultdict(dict)
        for row in rows:
            pid = row['participant_id']
            key = row['description']
            value = row['value']
            formed_key_value_pairs[pid][key] = value

        return formed_key_value_pairs
