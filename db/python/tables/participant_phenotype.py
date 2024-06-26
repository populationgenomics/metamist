from collections import defaultdict
from typing import Any, Dict, List, Tuple

from db.python.tables.base import DbBase
from db.python.utils import from_db_json, to_db_json


class ParticipantPhenotypeTable(DbBase):
    """
    Capture Participant_Phenotype table operations and queries
    """

    table_name = 'participant_phenotype'

    async def add_key_value_rows(self, rows: List[Tuple[int, str, Any]]) -> None:
        """
        Create a new sample, and add it to database
        """
        if not rows:
            return None
        _query = """
INSERT INTO participant_phenotypes
    (participant_id, description, value, audit_log_id, hpo_term)
VALUES
    (:participant_id, :description, :value, :audit_log_id, 'DESCRIPTION')
ON DUPLICATE KEY UPDATE
    description=:description, value=:value, audit_log_id=:audit_log_id
        """

        audit_log_id = await self.audit_log_id()
        formatted_rows = [
            {
                'participant_id': r[0],
                'description': r[1],
                'value': to_db_json(r[2]),
                'audit_log_id': audit_log_id,
            }
            for r in rows
        ]

        return await self.connection.execute_many(
            _query,
            formatted_rows,
        )

    async def get_key_value_rows_for_participant_ids(
        self, participant_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get (participant_id, description, value),
        for individual level metadata template,
        for specified participant ids
        """
        if len(participant_ids) == 0:
            return {}

        _query = """
SELECT participant_id, description, value
FROM participant_phenotypes
WHERE participant_id in :participant_ids AND value IS NOT NULL
"""
        rows = await self.connection.fetch_all(
            _query, {'participant_ids': participant_ids}
        )
        formed_key_value_pairs: Dict[int, Dict[str, Any]] = defaultdict(dict)
        for row in rows:
            pid = row['participant_id']
            key = row['description']
            value = row['value']
            formed_key_value_pairs[pid][key] = from_db_json(value)

        return formed_key_value_pairs

    async def get_key_value_rows_for_all_participants(
        self, project: int
    ) -> Dict[int, Dict[str, Any]]:
        """
        Get (participant_id, description, value),
        for individual level metadata template,
        for all participants in project
        """
        _query = """
SELECT pp.participant_id, pp.description, pp.value
FROM participant_phenotypes pp
INNER JOIN participant p ON p.id = pp.participant_id
WHERE p.project = :project AND pp.value IS NOT NULL
"""
        rows = await self.connection.fetch_all(_query, {'project': project})
        formed_key_value_pairs: Dict[int, Dict[str, Any]] = defaultdict(dict)
        for row in rows:
            pid = row['participant_id']
            key = row['description']
            value = row['value']
            formed_key_value_pairs[pid][key] = from_db_json(value)

        return formed_key_value_pairs
