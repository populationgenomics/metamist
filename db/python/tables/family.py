from typing import List, Optional

from db.python.connect import DbBase, NotFoundError


class FamilyTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'family_participant'

    async def create_family(
        self,
        external_id: str,
        description: Optional[str],
        coded_phenotype: Optional[str],
        author: str = None,
        project: int = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        updater = {
            'external_id': external_id,
            'description': description,
            'coded_phenotype': coded_phenotype,
            'author': author or self.author,
            'project': project or self.project,
        }
        keys = list(updater.keys())
        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f':{k}' for k in keys)
        _query = f"""
INSERT INTO family
    ({str_keys})
VALUES
    ({placeholder_keys})
RETURNING id
        """

        return await self.connection.fetch_val(_query, updater)

    async def get_id_map_by_external_ids(
        self, family_ids: List[str], allow_missing=False, project: Optional[int] = None
    ):
        """Get map of {external_id: internal_id} for a family"""
        _query = 'SELECT external_id, id FROM family WHERE external_id in :external_ids AND project = :project'
        results = await self.connection.fetch_all(
            _query, {'external_ids': family_ids, 'project': project or self.project}
        )
        id_map = {r['external_id']: r['id'] for r in results}

        if not allow_missing and len(id_map) != len(family_ids):
            provided_external_ids = set(family_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_external_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_family_ids = provided_external_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find families with external IDS: {', '.join(missing_family_ids)}"
                )

        return id_map

    async def get_id_map_by_internal_ids(
        self, family_ids: List[int], allow_missing=False
    ):
        """Get map of {external_id: internal_id} for a family"""
        if len(family_ids) == 0:
            return {}
        _query = 'SELECT id, external_id FROM family WHERE id in :ids'
        results = await self.connection.fetch_all(_query, {'ids': family_ids})
        id_map = {r['id']: r['external_id'] for r in results}
        if not allow_missing and len(id_map) != len(family_ids):
            provided_external_ids = set(family_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_external_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_family_ids = provided_external_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find families with internal IDS: {', '.join(str(m) for m in missing_family_ids)}"
                )

        return id_map
