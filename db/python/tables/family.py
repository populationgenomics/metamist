import dataclasses
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, escape_like_term
from models.models import PRIMARY_EXTERNAL_ORG, FamilyInternal, ProjectId


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
        if description:
            values['description'] = description
        if coded_phenotype:
            values['coded_phenotype'] = coded_phenotype

        async with self.connection.transaction():
            if external_ids is None:
                external_ids = {}

            to_delete = [k.lower() for k, v in external_ids.items() if v is None]
            to_update = {k.lower(): v for k, v in external_ids.items() if v is not None}

            if to_delete:
                await self.connection.execute(
                    """
                    -- Set audit_log_id to this transaction before deleting the rows
                    UPDATE family_external_id
                    SET audit_log_id = :audit_log_id
                    WHERE family_id = :id AND name IN :names;

                    DELETE FROM family_external_id
                    WHERE family_id = :id AND name in :names
                    """,
                    {'id': id_, 'names': to_delete, 'audit_log_id': audit_log_id},
                )

            if to_update:
                project = await self.connection.fetch_val(
                    'SELECT project FROM family WHERE id = :id',
                    {'id': id_},
                )

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

            setters = ', '.join(f'{field} = :{field}' for field in values)
            await self.connection.execute(
                f"""
                UPDATE family
                SET {setters}
                WHERE id = :id
                """,
                {**values, 'id': id_},
            )

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

        async with self.connection.transaction():
            new_id = await self.connection.fetch_val(
                """
                INSERT INTO family (project, description, coded_phenotype, audit_log_id)
                VALUES (:project, :description, :coded_phenotype, :audit_log_id)
                RETURNING id
                """,
                {
                    'project': project or self.project_id,
                    'description': description,
                    'coded_phenotype': coded_phenotype,
                    'audit_log_id': audit_log_id,
                },
            )

            await self.connection.execute_many(
                """
                INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                VALUES (:project, :family_id, :name, :external_id, :audit_log_id)
                """,
                [
                    {
                        'project': project or self.project_id,
                        'family_id': new_id,
                        'name': name,
                        'external_id': eid,
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in external_ids.items()
                ],
            )

        return new_id

    async def insert_or_update_multiple_families(
        self,
        external_ids: List[str],
        descriptions: List[str],
        coded_phenotypes: List[Optional[str]],
        project: ProjectId | None = None,
    ):
        """
        Upsert several families.
        At present, this function only supports upserting the primary external id.
        """
        audit_log_id = await self.audit_log_id()

        for eid, descr, cph in zip(external_ids, descriptions, coded_phenotypes):
            existing_id = await self.connection.fetch_val(
                """
                SELECT family_id FROM family_external_id
                WHERE project = :project AND external_id = :external_id
                """,
                {'project': project or self.project_id, 'external_id': eid},
            )

            if existing_id is None:
                new_id = await self.connection.fetch_val(
                    """
                    INSERT INTO family (project, description, coded_phenotype, audit_log_id)
                    VALUES (:project, :description, :coded_phenotype, :audit_log_id)
                    RETURNING id
                    """,
                    {
                        'project': project or self.project_id,
                        'description': descr,
                        'coded_phenotype': cph,
                        'audit_log_id': audit_log_id,
                    },
                )
                await self.connection.execute(
                    """
                    INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                    VALUES (:project, :family_id, :name, :external_id, :audit_log_id)
                    """,
                    {
                        'project': project or self.project_id,
                        'family_id': new_id,
                        'name': PRIMARY_EXTERNAL_ORG,
                        'external_id': eid,
                        'audit_log_id': audit_log_id,
                    },
                )

            else:
                await self.connection.execute(
                    """
                    UPDATE family
                    SET description = :description, coded_phenotype = :coded_phenotype,
                        audit_log_id = :audit_log_id
                    WHERE id = :id
                    """,
                    {
                        'id': existing_id,
                        'description': descr,
                        'coded_phenotype': cph,
                        'audit_log_id': audit_log_id,
                    },
                )

        return True

    async def get_id_map_by_external_ids(
        self, family_ids: List[str], allow_missing=False, project: Optional[int] = None
    ) -> Dict:
        """Get map of {external_id: internal_id} for a family"""

        if not family_ids:
            return {}

        results = await self.connection.fetch_all(
            """
            SELECT external_id, family_id AS id FROM family_external_id
            WHERE external_id in :external_ids AND project = :project
            """,
            {'external_ids': family_ids, 'project': project or self.project_id},
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
        """Get map of {internal_id: primary_external_id} for a family"""
        if len(family_ids) == 0:
            return {}

        results = await self.connection.fetch_all(
            """
            SELECT family_id, external_id
            FROM family_external_id
            WHERE family_id in :ids AND name = :PRIMARY_EXTERNAL_ORG
            """,
            {'ids': family_ids, 'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG},
        )
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
