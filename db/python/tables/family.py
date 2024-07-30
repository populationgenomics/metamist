import dataclasses
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, escape_like_term
from models.models.family import FamilyInternal
from models.models.project import ProjectId


@dataclasses.dataclass
class FamilyFilter(GenericFilterModel):
    """Filter mode for querying Families

    Args:
        GenericFilterModel (_type_): _description_
    """

    id: GenericFilter[int] | None = None
    external_id: GenericFilter[str] | None = None

    project: GenericFilter[ProjectId] | None = None
    participant_id: GenericFilter[int] | None = None
    sample_id: GenericFilter[int] | None = None


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

    async def query(
        self, filter_: FamilyFilter
    ) -> tuple[set[ProjectId], list[FamilyInternal]]:
        """Get all families for some project"""
        _query = """
            SELECT f.id, JSON_OBJECTAGG(feid.name, feid.external_id) AS external_ids,
                   f.description, f.coded_phenotype, f.project
            FROM family f
            INNER JOIN family_external_id feid ON f.id = feid.family_id
        """

        if not filter_.project and not filter_.id:
            raise ValueError('Project or ID filter is required for family queries')

        has_participant_join = False
        field_overrides = {
            'id': 'f.id',
            'external_id': 'feid.external_id',
            'project': 'f.project',
        }

        has_participant_join = False
        if filter_.participant_id:
            field_overrides['participant_id'] = 'fp.participant_id'
            has_participant_join = True
            _query += """
                JOIN family_participant fp ON f.id = fp.family_id
            """

        if filter_.sample_id:
            field_overrides['sample_id'] = 's.id'
            if not has_participant_join:
                _query += """
                    JOIN family_participant fp ON f.id = fp.family_id
                """

            _query += """
                INNER JOIN sample s ON fp.participant_id = s.participant_id
            """

        wheres, values = filter_.to_sql(field_overrides)
        if wheres:
            _query += f'WHERE {wheres}'

        _query += """
            GROUP BY f.id, f.description, f.coded_phenotype, f.project
        """

        rows = await self.connection.fetch_all(_query, values)
        seen = set()
        families = []
        projects: set[ProjectId] = set()
        for r in rows:
            if r['id'] not in seen:
                projects.add(r['project'])
                families.append(FamilyInternal.from_db(dict(r)))
                seen.add(r['id'])

        return projects, families

    async def get_families_by_participants(
        self, participant_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[FamilyInternal]]]:
        """Get families, keyed by participants"""
        if not participant_ids:
            return set(), {}

        _query = """
            SELECT f.id, JSON_OBJECTAGG(feid.name, feid.external_id) AS external_ids,
                   f.description, f.coded_phenotype, f.project, fp.participant_id
            FROM family f
            INNER JOIN family_external_id feid ON f.id = feid.family_id
            INNER JOIN family_participant fp ON f.id = fp.family_id
            WHERE fp.participant_id in :pids
            GROUP BY f.id, f.description, f.coded_phenotype, f.project, fp.participant_id
        """
        ret_map = defaultdict(list)
        projects: set[ProjectId] = set()
        for row in await self.connection.fetch_all(_query, {'pids': participant_ids}):
            drow = dict(row)
            pid = drow.pop('participant_id')
            projects.add(drow.get('project'))
            ret_map[pid].append(FamilyInternal.from_db(drow))

        return projects, ret_map

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, FamilyId, ExternalId]
        """
        _query = """
        SELECT project, family_id, external_id
        FROM family_external_id
        WHERE project in :project_ids AND external_id LIKE :search_pattern
        LIMIT :limit
        """
        rows = await self.connection.fetch_all(
            _query,
            {
                'project_ids': project_ids,
                'search_pattern': escape_like_term(query) + '%',
                'limit': limit,
            },
        )
        return [(r['project'], r['family_id'], r['external_id']) for r in rows]

    async def get_family_external_ids_by_participant_ids(
        self, participant_ids
    ) -> dict[int, list[str]]:
        """Get family external IDs by participant IDs, useful for search"""
        if not participant_ids:
            return {}

        _query = """
        SELECT feid.external_id, fp.participant_id
        FROM family_participant fp
        INNER JOIN family_external_id feid ON fp.family_id = feid.family_id
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
        external_ids: dict[str, str | None] | None = None,
        description: str | None = None,
        coded_phenotype: str | None = None,
    ) -> bool:
        """Update values for a family"""
        audit_log_id = await self.audit_log_id()
        values: Dict[str, Any] = {'audit_log_id': audit_log_id}

        if external_ids:
            to_delete = [k.lower() for k, v in external_ids.items() if v is None]
            to_update = {k.lower(): v for k, v in external_ids.items() if v is not None}

            if to_delete:
                # Set audit_log_id to this transaction before deleting the rows
                _audit_update_query = """
UPDATE family_external_id
SET audit_log_id = :audit_log_id
WHERE family_id = :id AND name IN :names
                """
                await self.connection.execute(
                    _audit_update_query,
                    {'id': id_, 'names': to_delete, 'audit_log_id': audit_log_id},
                )

                _delete_query = """
DELETE FROM family_exernal_id
WHERE family_id = :id AND name in :names
                """
                await self.connection.execute(
                    _delete_query, {'id': id_, 'names': to_delete}
                )

            if to_update:
                _query = 'SELECT project FROM family WHERE id = :id'
                row = await self.connection.fetch_one(_query, {'id': id_})
                project = row['project']

                _update_query = """
INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
VALUES (:project, :id, :name, :external_id, :audit_log_id)
ON DUPLICATE KEY UPDATE external_id = :external_id, audit_log_id = :audit_log_id
                """
                _update_values = [
                    {
                        'project': project,
                        'id': id_,
                        'name': name,
                        'external_id': eid,
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in to_update.items()
                ]
                await self.connection.execute_many(_update_query, _update_values)

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
        external_ids: dict[str, str],
        description: Optional[str],
        coded_phenotype: Optional[str],
        project: ProjectId | None = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        audit_log_id = await self.audit_log_id()
        updater = {
            'description': description,
            'coded_phenotype': coded_phenotype,
            'audit_log_id': audit_log_id,
            'project': project or self.project_id,
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

        new_id = await self.connection.fetch_val(_query, updater)

        _eid_query = """
INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
VALUES (:project, :family_id, :name, :external_id, :audit_log_id)
        """
        values = [
            {
                'project': project or self.project_id,
                'family_id': new_id,
                'name': name,
                'external_id': eid,
                'audit_log_id': audit_log_id,
            }
            for name, eid in external_ids.items()
        ]
        await self.connection.execute_many(_eid_query, values)

        return new_id

    async def insert_or_update_multiple_families(
        self,
        external_ids: List[str],
        descriptions: List[str],
        coded_phenotypes: List[Optional[str]],
        project: ProjectId | None = None,
    ):
        """Upsert"""
        updater = [
            {
                'description': descr,
                'coded_phenotype': cph,
                'audit_log_id': await self.audit_log_id(),
                'project': project or self.project_id,
            }
            for descr, cph in zip(descriptions, coded_phenotypes)
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

        print(f'Need to insert {external_ids} into family_external_id')  # FIXME

        return True

    async def get_id_map_by_external_ids(
        self, family_ids: List[str], allow_missing=False, project: Optional[int] = None
    ) -> Dict:
        """Get map of {external_id: internal_id} for a family"""

        if not family_ids:
            return {}

        _query = """\
SELECT external_id, family_id AS id FROM family_external_id
WHERE external_id in :external_ids AND project = :project
        """
        results = await self.connection.fetch_all(
            _query, {'external_ids': family_ids, 'project': project or self.project_id}
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
        """Get map of {internal_id: external_id} for a family"""
        if len(family_ids) == 0:
            return {}
        # FIXME needs to be uniqueified!!
        _query = 'SELECT family_id, external_id FROM family_external_id WHERE family_id in :ids'
        results = await self.connection.fetch_all(_query, {'ids': family_ids})
        id_map = {r['family_id']: r['external_id'] for r in results}
        if not allow_missing and len(id_map) != len(family_ids):
            provided_internal_ids = set(family_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_internal_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_family_ids = provided_internal_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find families with internal IDS: {', '.join(str(m) for m in missing_family_ids)}"
                )

        return id_map
