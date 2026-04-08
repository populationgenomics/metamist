import dataclasses
from collections import defaultdict
from string.templatelib import Template
from typing import Any

from psycopg import sql
from psycopg.rows import class_row, scalar_row
from psycopg.types.json import Jsonb

from db.python.filters import GenericFilter, GenericFilterModel, GenericMetaFilter
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, escape_like_term
from models.models import PRIMARY_EXTERNAL_ORG, FamilyInternal, ProjectId


@dataclasses.dataclass
class FamilyFilter(GenericFilterModel):
    """
    Filter mode for querying Families

    Args:
        GenericFilterModel (_type_): _description_
    """

    id: GenericFilter[int] | None = None
    external_id: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None

    project: GenericFilter[ProjectId] | None = None
    participant_id: GenericFilter[int] | None = None
    sample_id: GenericFilter[int] | None = None


class FamilyTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'family'

    async def get_projects_by_family_ids(self, family_ids: list[int]) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""

        if len(family_ids) == 0:
            raise ValueError('Received no family IDs to get project ids for')

        rows = await (
            await self.connection.pg_connection.execute(
                t'SELECT project FROM family WHERE id = ANY({family_ids}) GROUP BY project'
            )
        ).fetchall()

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

        _query = [
            t"""
        SELECT f.id, jsonb_object_agg(feid.name, feid.external_id) AS external_ids,
        f.description, f.coded_phenotype, f.meta, f.project FROM family f
        INNER JOIN family_external_id feid ON f.id = feid.family_id
        """
        ]

        if not filter_.project and not filter_.id:
            raise ValueError('Project or ID filter is required for family queries')

        field_overrides = {
            'id': 'f.id',
            'external_id': 'feid.external_id',
            'project': 'f.project',
            'meta': 'f.meta',
        }

        has_participant_join = False
        if filter_.participant_id:
            field_overrides['participant_id'] = 'fp.participant_id'
            has_participant_join = True
            _query.append(t' JOIN family_participant fp ON f.id = fp.family_id ')

        if filter_.sample_id:
            field_overrides['sample_id'] = 's.id'
            if not has_participant_join:
                _query.append(t' JOIN family_participant fp ON f.id = fp.family_id ')

            _query.append(
                t' INNER JOIN sample s ON fp.participant_id = s.participant_id '
            )

        where_params: Template = filter_.to_sql(field_overrides)
        joined_query = sql.SQL(' ').join(_query)

        async with self.connection.pg_connection.cursor(
            row_factory=class_row(FamilyInternal)
        ) as cur:
            await cur.execute(t'{joined_query:q} WHERE {where_params:q} GROUP BY f.id')
            family_internal_list = await cur.fetchall()

        families = []
        projects: set[ProjectId] = set()
        for family_internal in family_internal_list:
            projects.add(family_internal.project)
            families.append(family_internal)

        return projects, families

    async def get_families_by_participants(
        self, participant_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[FamilyInternal]]]:
        """Get families, keyed by participants"""
        if not participant_ids:
            return set(), {}

        _query = t"""
        SELECT f.id, jsonb_object_agg(feid.name, feid.external_id) AS external_ids,
        f.description, f.coded_phenotype, f.meta, f.project, fp.participant_id
        FROM family f
        INNER JOIN family_external_id feid ON f.id = feid.family_id
        INNER JOIN family_participant fp ON f.id = fp.family_id
        WHERE fp.participant_id = ANY({participant_ids})
        GROUP BY f.id, f.description, f.coded_phenotype, f.meta, f.project, fp.participant_id
        """

        ret_map = defaultdict(list)
        projects: set[ProjectId] = set()

        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()

        for row in rows:
            pid = row.pop('participant_id')
            projects.add(row['project'])
            ret_map[pid].append(FamilyInternal(**row))

        return projects, ret_map

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, FamilyId, ExternalId]
        """
        search_pattern = escape_like_term(query) + '%'
        rows = await (
            await self.connection.pg_connection.execute(t"""
            SELECT project, family_id, external_id FROM family_external_id
            WHERE project = ANY({project_ids}) AND external_id ILIKE {search_pattern} LIMIT {limit}
            """)
        ).fetchall()

        return [(r['project'], r['family_id'], r['external_id']) for r in rows]

    async def get_family_external_ids_by_participant_ids(
        self, participant_ids
    ) -> dict[int, list[str]]:
        """Get family external IDs by participant IDs, useful for search"""
        if not participant_ids:
            return {}

        _query = t"""
        SELECT feid.external_id, fp.participant_id FROM family_participant fp
        INNER JOIN family_external_id feid ON fp.family_id = feid.family_id
        WHERE fp.participant_id = ANY({participant_ids})
        """

        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()

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
        meta: dict[str, Any] | None = None,
    ) -> bool:
        """Update values for a family"""
        audit_log_id = await self.audit_log_id()

        updaters = [t'audit_log_id = {audit_log_id}']
        if description:
            updaters.append(t'description = {description}')
        if coded_phenotype:
            updaters.append(t'coded_phenotype = {coded_phenotype}')
        if meta is not None:
            meta_param = Jsonb(meta)
            updaters.append(
                t'meta = json_merge_patch(COALESCE(meta, {"{}"}::jsonb), {meta_param})'
            )

        conn = self.connection.pg_connection
        async with conn.transaction(), conn.cursor(row_factory=scalar_row) as cur:
            if external_ids is None:
                external_ids = {}

            to_delete = [k.lower() for k, v in external_ids.items() if v is None]
            to_update = {k.lower(): v for k, v in external_ids.items() if v is not None}

            if to_delete:
                # Set audit_log_id to this transaction before deleting the rows
                await cur.execute(t"""
                     UPDATE family_external_id SET audit_log_id = {audit_log_id}
                     WHERE family_id = {id_} AND LOWER(name) = ANY({to_delete})
                """)

                await cur.execute(
                    t'DELETE FROM family_external_id WHERE family_id = {id_} AND LOWER(name) = ANY({to_delete})'
                )

            if to_update:
                await cur.execute(t'SELECT project FROM family WHERE id = {id_}')
                project = await cur.fetchone()

                # Use MERGE to handle both the primary key (family_id, name) and
                # the unique index (project, external_id) conflicts.
                # Mimics MariaDB ON DUPLICATE KEY UPDATE behavior.
                _update_query = """MERGE INTO family_external_id AS target
                USING (SELECT %(project)s AS project, %(id)s AS family_id, %(name)s AS name,
                              %(external_id)s AS external_id, %(audit_log_id)s AS audit_log_id) AS source
                ON (target.family_id = source.family_id AND target.name = source.name)
                   OR (target.project = source.project AND target.external_id = source.external_id)
                WHEN MATCHED THEN
                    UPDATE SET external_id = source.external_id,
                               audit_log_id = source.audit_log_id
                WHEN NOT MATCHED THEN
                    INSERT (project, family_id, name, external_id, audit_log_id)
                    VALUES (source.project, source.family_id, source.name, source.external_id, source.audit_log_id)"""

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
                await cur.executemany(_update_query, _update_values)

            # Only update if more than just audit_log_id has changed
            if len(updaters) > 1:
                joined = sql.SQL(',').join(updaters)
                await cur.execute(t'UPDATE family SET {joined:q} WHERE id = {id_:s}')

        return True

    async def create_family(
        self,
        external_ids: dict[str, str],
        description: str | None,
        coded_phenotype: str | None,
        meta: dict[str, Any] | None = None,
        project: ProjectId | None = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        audit_log_id = await self.audit_log_id()

        project_param = project or self.project_id
        meta_param = Jsonb(meta or {})

        conn = self.connection.pg_connection
        async with conn.transaction(), conn.cursor(row_factory=scalar_row) as cur:
            await cur.execute(t"""
            INSERT INTO family (project, description, coded_phenotype, meta, audit_log_id)
            VALUES ({project_param}, {description}, {coded_phenotype}, {meta_param}, {audit_log_id})
            RETURNING id
            """)
            new_id = await cur.fetchone()
            assert isinstance(new_id, int)

            await cur.executemany(
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
        external_ids: list[str],
        descriptions: list[str | None],
        coded_phenotypes: list[str | None],
        project: ProjectId | None = None,
        meta: list[dict[str, Any] | None] | None = None,
    ):
        """
        Upsert several families.
        At present, this function only supports upserting the primary external id.
        """
        audit_log_id = await self.audit_log_id()

        # Default to list of None if meta not provided
        meta_list = meta if meta is not None else [None] * len(external_ids)

        # each query executes independently
        project_param = project or self.project_id

        conn = self.connection.pg_connection
        async with conn.transaction(), conn.cursor(row_factory=scalar_row) as cur:
            for eid, descr, cph, mt in zip(
                external_ids,
                descriptions,
                coded_phenotypes,
                meta_list,
                strict=False,
            ):
                await cur.execute(
                    t'SELECT family_id FROM family_external_id WHERE project = {project_param} AND LOWER(external_id)={eid.lower()}'
                )
                existing_id = await cur.fetchone()
                meta_param = Jsonb(mt or {})
                if existing_id is None:
                    await cur.execute(t"""
                        INSERT INTO family (project, description, coded_phenotype, meta, audit_log_id)
                        VALUES ({project_param}, {descr}, {cph}, {meta_param}, {audit_log_id})
                        RETURNING id
                    """)
                    new_id = await cur.fetchone()

                    await cur.execute(t"""
                    INSERT INTO family_external_id (project, family_id, name, external_id, audit_log_id)
                    VALUES ({project_param}, {new_id}, {PRIMARY_EXTERNAL_ORG}, {eid}, {audit_log_id})""")

                else:
                    await cur.execute(t"""UPDATE family
                    SET description = {descr}, coded_phenotype = {cph}, meta = json_merge_patch(COALESCE(meta, {'{}'}::jsonb), {meta_param}),
                    audit_log_id = {audit_log_id} WHERE id = {existing_id}
                    """)

        return True

    async def get_id_map_by_external_ids(
        self, family_ids: list[str], allow_missing=False, project: int | None = None
    ) -> dict:
        """Get map of {external_id: internal_id} for a family"""

        if not family_ids:
            return {}

        fids_case_insensitive = [fid.lower() for fid in family_ids]

        project_param = project or self.project_id
        _query = t"""
        SELECT external_id, family_id AS id FROM family_external_id
        WHERE LOWER(external_id) = ANY({fids_case_insensitive}) AND project = {project_param}
        """

        results = await (await self.connection.pg_connection.execute(_query)).fetchall()

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
        self, family_ids: list[int], allow_missing=False
    ):
        """Get map of {internal_id: primary_external_id} for a family"""
        if len(family_ids) == 0:
            return {}

        _query = t"""SELECT family_id, external_id FROM family_external_id
        WHERE family_id = ANY({family_ids}) AND name = {PRIMARY_EXTERNAL_ORG}"""

        results = await (await self.connection.pg_connection.execute(_query)).fetchall()

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
