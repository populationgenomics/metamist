from typing import List, Dict

from db.python.connect import DbBase, NotFoundError, to_db_json


class ParticipantTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'participant'

    async def create_participant(
        self,
        external_id: str,
        author: str = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        _query = f"""
INSERT INTO family (external_id, author)
VALUES (:external_id, :author)
RETURNING id
        """

        return await self.connection.fetch_val(
            _query,
            {
                'external_id': external_id,
                'author': author or self.author,
            },
        )

    async def get_id_map_by_external_ids(
        self, external_participant_ids: List[str], allow_missing=False
    ) -> Dict[str, int]:
        _query = (
            'SELECT external_id, id FROM participant WHERE external_id in :external_ids'
        )
        results = await self.connection.fetch_all(_query, {'external_ids': family_ids})
        id_map = {r['external_id']: r for r in results}

        if not allow_missing and len(id_map) != len(external_participant_ids):
            provided_external_ids = set(external_participant_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_external_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_family_ids = provided_external_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find families with IDS: {', '.join(missing_family_ids)}"
                )

        return id_map
