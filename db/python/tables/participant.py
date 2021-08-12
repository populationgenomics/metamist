from typing import List, Dict

from db.python.connect import DbBase, NotFoundError


class ParticipantTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'participant'

    async def create_participant(
        self, external_id: str, author: str = None, project: int = None
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        _query = f"""
INSERT INTO participant (external_id, author, project)
VALUES (:external_id, :author, :project)
RETURNING id
        """

        return await self.connection.fetch_val(
            _query,
            {
                'external_id': external_id,
                'author': author or self.author,
                'project': project or self.project,
            },
        )

    async def get_id_map_by_external_ids(
        self, external_participant_ids: List[str], allow_missing=False
    ) -> Dict[str, int]:
        """Get map of {external_id: internal_participant_id}"""
        _query = (
            'SELECT external_id, id FROM participant WHERE external_id in :external_ids'
        )
        results = await self.connection.fetch_all(
            _query, {'external_ids': external_participant_ids}
        )
        id_map = {r['external_id']: r['id'] for r in results}

        if not allow_missing and len(id_map) != len(external_participant_ids):
            provided_external_ids = set(external_participant_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_external_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_participant_ids = provided_external_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find participants with IDS: {', '.join(missing_participant_ids)}"
                )

        return id_map

    async def get_id_map_by_internal_ids(
        self, internal_participant_ids: List[int], allow_missing=False
    ) -> Dict[str, int]:
        """Get map of {internal_id: external_participant_id}"""
        _query = 'SELECT id, external_id FROM participant WHERE id in :ids'
        results = await self.connection.fetch_all(
            _query, {'ids': internal_participant_ids}
        )
        id_map = {r['id']: r['external_id'] for r in results}

        if not allow_missing and len(id_map) != len(internal_participant_ids):
            provided_internal_ids = set(internal_participant_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_internal_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_participant_ids = provided_internal_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find participants with internal IDS: {', '.join(missing_participant_ids)}"
                )

        return id_map
