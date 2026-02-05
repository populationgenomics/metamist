import dataclasses
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from psycopg import AsyncConnection
from psycopg.rows import scalar_row, class_row

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
            WHERE id in %(family_ids)s
            GROUP BY project
        """
        if len(family_ids) == 0:
            raise ValueError('Received no family IDs to get project ids for')

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(_query, {'family_ids': family_ids})
                rows = await curr.fetchall()

        # async with self._execute(_query, {'family_ids': family_ids}) as curr:
        #     rows = await curr.fetchall()

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
        SELECT f.id, jsonb_object_agg(feid.name, feid.external_id) AS external_ids,
               f.description, f.coded_phenotype, f.project
        FROM family f
        INNER JOIN family_external_id feid ON f.id = feid.family_id
        """

        if not filter_.project and not filter_.id:
            raise ValueError('Project or ID filter is required for family queries')

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

        wheres, values = filter_.to_sql(field_overrides)  # TODO: fix this util function
        if wheres:
            _query += f'WHERE {wheres}'

        _query += """
            GROUP BY f.id, f.description, f.coded_phenotype, f.project
        """

        async with self.connection.pool.connection() as conn:
            # TODO: remove comment later. INNER JOIN family_external_id with family.
            #  feid.name, feid.external_id are NOT NULL  -> external_ids won't encounter NulL scenarios -> safe to remove from_db
            async with conn.cursor(row_factory=class_row(FamilyInternal)) as curr:
                await curr.execute(_query, values)
                family_internal_list = await curr.fetchall()

        seen = set()
        families = []
        projects: set[ProjectId] = set()
        for family_internal in family_internal_list:
            if family_internal.id not in seen:
                projects.add(family_internal.project)
                families.append(family_internal)
                seen.add(family_internal.id)

        return projects, families

    async def get_families_by_participants(
        self, participant_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[FamilyInternal]]]:
        """Get families, keyed by participants"""
        if not participant_ids:
            return set(), {}

        _query = """
            SELECT
                f.id,
                jsonb_object_agg(feid.name, feid.external_id) AS external_ids,
                f.description, f.coded_phenotype, f.project, fp.participant_id
            FROM family f
            INNER JOIN family_external_id feid ON f.id = feid.family_id
            INNER JOIN family_participant fp ON f.id = fp.family_id
            WHERE fp.participant_id = ANY(%(pids)s)
            GROUP BY f.id, f.description, f.coded_phenotype, f.project, fp.participant_id
        """
        ret_map = defaultdict(list)
        projects: set[ProjectId] = set()

        async with self.connection.pool.connection() as conn:
            # TODO: remove comment later. INNER JOIN family_external_id with family.
            #  feid.name, feid.external_id are NOT NULL  -> external_ids won't encounter NulL scenarios -> safe to remove from_db
            async with conn.cursor(row_factory=class_row(FamilyInternal)) as curr:
                await curr.execute(_query, {'pids': participant_ids})
                family_internal_list = await curr.fetchall()

        for family_internal in family_internal_list:
            projects.add(family_internal.project)
            ret_map[family_internal.participant_id].append(family_internal)

        return projects, ret_map

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, FamilyId, ExternalId]
        """
        # TODO:piyumi ILIKE for case insensitive matches
        _query = """
            SELECT project, family_id, external_id
            FROM family_external_id
            WHERE project = ANY(%(project_ids)s) AND external_id ILIKE %(search_pattern)s
            LIMIT %(limit)s
        """
        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(
                    _query,
                    {
                        'project_ids': project_ids,
                        'search_pattern': escape_like_term(query) + '%',
                        'limit': limit,
                    },
                )
                rows = await curr.fetchall()

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
        WHERE fp.participant_id = ANY(%(pids)s)
        """

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(_query, {'pids': participant_ids})
                rows = await curr.fetchall()

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

        async with self.connection.pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=scalar_row) as curr:
                    if external_ids is None:
                        external_ids = {}

                    to_delete = [
                        k.lower() for k, v in external_ids.items() if v is None
                    ]
                    to_update = {
                        k.lower(): v for k, v in external_ids.items() if v is not None
                    }

                    if to_delete:
                        # psycopg does no support executing multiple parameterized statements in a single execution
                        # https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html#multiple-results-returned-from-multiple-statements

                        await curr.execute(
                            """
                            -- Set audit_log_id to this transaction before deleting the rows
                            UPDATE family_external_id
                            SET audit_log_id = %(audit_log_id)s
                            WHERE family_id = %(id)s AND name = ANY(%(names)s);
                            """,
                            {
                                'id': id_,
                                'names': to_delete,
                                'audit_log_id': audit_log_id,
                            },
                        )

                        await curr.execute(
                            """
                            DELETE FROM family_external_id
                            WHERE family_id = %(id)s AND name = ANY(%(names)s)
                            """,
                            {
                                'id': id_,
                                'names': to_delete,
                            },
                        )

                    if to_update:
                        await curr.execute(
                            'SELECT project FROM family WHERE id = %(id)s',
                            {'id': id_},
                        )
                        project = await curr.fetchone()

                        # TODO:piyumi ON CONFLICT with primary key
                        _update_query = """
                            INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                            VALUES (%(project)s, %(id)s, %(name)s, %(external_id)s, %(audit_log_id)s)
                            ON CONFLICT (family_id, name)
                            DO UPDATE SET
                                external_id = EXCLUDED.external_id,
                                audit_log_id = EXCLUDED.audit_log_id;
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
                        await curr.executemany(_update_query, _update_values)

                    setters = ', '.join(f'{field} = %({field})s' for field in values)
                    await curr.execute(
                        f"""
                        UPDATE family
                        SET {setters}
                        WHERE id = %(id)s
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
        async_connection_oj: AsyncConnection = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        audit_log_id = await self.audit_log_id()

        async with self._get_connection(async_connection_oj) as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=scalar_row) as curr:
                    await curr.execute(
                        """
                        INSERT INTO family (project, description, coded_phenotype, audit_log_id)
                        VALUES (%(project)s, %(description)s, %(coded_phenotype)s, %(audit_log_id)s)
                        RETURNING id
                        """,
                        {
                            'project': project or self.project_id,
                            'description': description,
                            'coded_phenotype': coded_phenotype,
                            'audit_log_id': audit_log_id,
                        },
                    )

                    new_id = await curr.fetchone()

                    curr.executemany(
                        """
                        INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                        VALUES (%(project)s, %(family_id)s, %(name)s, %(external_id)s, %(audit_log_id)s)
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

        # base level autocommit=true -> these individual executes will be committed independently
        async with self.connection.pool.connection() as conn:
            async with conn.cursor(row_factory=scalar_row) as curr:
                for eid, descr, cph in zip(
                    external_ids, descriptions, coded_phenotypes
                ):
                    await curr.execute(
                        """
                        SELECT family_id FROM family_external_id
                        WHERE project = %(project)s AND external_id = %(external_id)s
                        """,
                        {'project': project or self.project_id, 'external_id': eid},
                    )
                    existing_id = await curr.fetchone()

                    if existing_id is None:
                        await curr.execute(
                            """
                            INSERT INTO family (project, description, coded_phenotype, audit_log_id)
                            VALUES (%(project)s, %(description)s, %(coded_phenotype)s, %(audit_log_id)s)
                            RETURNING id
                            """,
                            {
                                'project': project or self.project_id,
                                'description': descr,
                                'coded_phenotype': cph,
                                'audit_log_id': audit_log_id,
                            },
                        )
                        new_id = await curr.fetchone()

                        await curr.execute(
                            """
                            INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                            VALUES (%(project)s, %(family_id)s, %(name)s, %(external_id)s, %(audit_log_id)s)
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
                        await curr.execute(
                            """
                            UPDATE family
                                SET description = %(description)s,
                                    coded_phenotype = %(coded_phenotype)s,
                                    audit_log_id = %(audit_log_id)s
                                WHERE id = %(id)s
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

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(
                    """
                    SELECT external_id, family_id AS id
                        FROM family_external_id
                        WHERE external_id = ANY(%(external_ids)s) AND project = %(project)s
                """,
                    {'external_ids': family_ids, 'project': project or self.project_id},
                )

                results = await curr.fetchall()

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

        async with self.connection.pool.connection() as conn:
            async with conn.cursor() as curr:
                await curr.execute(
                    """
                        SELECT family_id, external_id
                        FROM family_external_id
                        WHERE family_id = ANY(%(ids)s) AND name = %(PRIMARY_EXTERNAL_ORG)s
                    """,
                    {'ids': family_ids, 'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG},
                )
                results = await curr.fetchall()

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
