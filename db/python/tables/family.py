from collections import defaultdict
from typing import List, Optional, Set, Any, Dict

from db.python.connect import DbBase, NotFoundError
from db.python.tables.project import ProjectId
from models.models.family import Family


class FamilyTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'family'

    async def get_projects_by_family_ids(self, family_ids: List[int]) -> Set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT project FROM family
            WHERE id in :family_ids
            GROUP BY project
        """
        if len(family_ids) == 0:
            raise ValueError('Received no family IDs to get project ids for')
        rows = await self.connection.fetch_all(_query, {'family_ids': family_ids})
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given families, this is likely an error'
            )
        return projects

    async def get_families(
        self, project: int = None, participant_ids: List[int] = None
    ) -> List[Family]:
        """Get all families for some project"""
        _query = """
            SELECT id, external_id, description, coded_phenotype, project
            FROM family
        """

        values: Dict[str, Any] = {'project': project or self.project}
        where: List[str] = []

        if participant_ids:
            _query += """
                JOIN family_participant
                ON family.id = family_participant.family_id
            """
            where.append(f'participant_id IN :pids')
            values['pids'] = participant_ids

        if project or self.project:
            where.append('project = :project')

        if where:
            _query += 'WHERE ' + ' AND '.join(where)

        rows = await self.connection.fetch_all(_query, values)
        seen = set()
        families = []
        for r in rows:
            if r.id not in seen:
                families.append(Family.from_db(dict(r)))
                seen.add(r.id)
        return families

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, FamilyId, ExternalId]
        """
        _query = """
        SELECT project, id, external_id
        FROM family
        WHERE project in :project_ids AND external_id LIKE :search_pattern
        LIMIT :limit
        """
        rows = await self.connection.fetch_all(
            _query,
            {
                'project_ids': project_ids,
                'search_pattern': self.escape_like_term(query) + '%',
                'limit': limit,
            },
        )
        return [(r['project'], r['id'], r['external_id']) for r in rows]

    async def get_family_external_ids_by_participant_ids(
        self, participant_ids
    ) -> dict[int, list[str]]:
        """Get family external IDs by participant IDs, useful for search"""
        if not participant_ids:
            return {}

        _query = """
        SELECT f.external_id, fp.participant_id
        FROM family_participant fp
        INNER JOIN family f ON fp.family_id = f.id
        WHERE fp.participant_id in :pids
        """
        rows = await self.connection.fetch_all(_query, {'pids': participant_ids})
        result = defaultdict(list)
        for r in rows:
            result[r['participant_id']].append(r['external_id'])

        return result

    async def update_family(
        self,
        id_: int,
        external_id: str = None,
        description: str = None,
        coded_phenotype: str = None,
        author: str = None,
    ) -> bool:
        """Update values for a family"""
        values: Dict[str, Any] = {'author': author or self.author}
        if external_id:
            values['external_id'] = external_id
        if description:
            values['description'] = description
        if coded_phenotype:
            values['coded_phenotype'] = coded_phenotype

        setters = ', '.join(f'{field} = :{field}' for field in values)
        _query = f"""
UPDATE family
SET {setters}
WHERE id = :id
        """
        await self.connection.execute(_query, {**values, 'id': id_})
        return True

    async def create_family(
        self,
        external_id: str,
        description: Optional[str],
        coded_phenotype: Optional[str],
        author: str = None,
        project: ProjectId = None,
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

    async def insert_or_update_multiple_families(
        self,
        external_ids: List[str],
        descriptions: List[str],
        coded_phenotypes: List[Optional[str]],
        project: int = None,
        author: str = None,
    ):
        """Upsert"""
        updater = [
            {
                'external_id': eid,
                'description': descr,
                'coded_phenotype': cph,
                'author': author or self.author,
                'project': project or self.project,
            }
            for eid, descr, cph in zip(external_ids, descriptions, coded_phenotypes)
        ]

        keys = list(updater[0].keys())
        str_keys = ', '.join(keys)
        placeholder_keys = ', '.join(f':{k}' for k in keys)

        update_only_keys = [k for k in keys if k not in ('external_id', 'project')]
        str_uo_placeholder_keys = ', '.join(f'{k} = :{k}' for k in update_only_keys)

        _query = f"""\
INSERT INTO family
    ({str_keys})
VALUES
    ({placeholder_keys})
ON DUPLICATE KEY UPDATE
    {str_uo_placeholder_keys}
        """

        await self.connection.execute_many(_query, updater)
        return True

    async def get_id_map_by_external_ids(
        self, family_ids: List[str], allow_missing=False, project: Optional[int] = None
    ) -> Dict:
        """Get map of {external_id: internal_id} for a family"""

        if not family_ids:
            return {}

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
