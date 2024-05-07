# pylint: disable=too-many-locals,too-many-arguments
import dataclasses
import re
from collections import defaultdict
from typing import Any

from db.python.tables.base import DbBase
from db.python.utils import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    NoOpAenter,
    NotFoundError,
    to_db_json,
)
from models.models.assay import AssayInternal
from models.models.project import ProjectId

REPLACEMENT_KEY_INVALID_CHARS = re.compile(r'[^\w\d_]')


def fix_replacement_key(k):
    """Fix a DB replacement key"""
    if not k or not isinstance(k, str):
        raise ValueError(f'Replacement key was not valid: {k} {type(k)}')
    k = REPLACEMENT_KEY_INVALID_CHARS.sub('_', k)
    if not k[0].isalpha():
        k = 'k' + k
    return k


@dataclasses.dataclass(kw_only=True)
class AssayFilter(GenericFilterModel):
    """
    Filter for Assay model
    """

    id: GenericFilter[int] | None = None
    sample_id: GenericFilter[int] | None = None
    external_id: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None
    sample_meta: GenericMetaFilter | None = None
    project: GenericFilter[int] | None = None
    type: GenericFilter | None = None

    def __hash__(self):  # pylint: disable=useless-super-delegation
        return hash(
            (self.id, self.sample_id, self.external_id, self.project, self.type)
        )


class AssayTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'assay'

    COMMON_GET_KEYS = [
        'a.id',
        'a.sample_id',
        'a.meta',
        'a.type',
        's.project',
    ]

    # region GETS

    async def query(
        self, filter_: AssayFilter
    ) -> tuple[set[ProjectId], list[AssayInternal]]:
        """Query assays"""
        sql_overides = {
            'sample_id': 'a.sample_id',
            'id': 'a.id',
            'external_id': 'aeid.external_id',
            'meta': 'a.meta',
            'sample_meta': 's.meta',
            'project': 's.project',
            'type': 'a.type',
        }
        if filter_.external_id is not None and filter_.project is None:
            raise ValueError('Must provide a project if filtering by external_id')

        conditions, values = filter_.to_sql(sql_overides)
        keys = ', '.join(self.COMMON_GET_KEYS)
        _query = f"""
            SELECT {keys}
            FROM assay a
            LEFT JOIN sample s ON s.id = a.sample_id
            LEFT JOIN assay_external_id aeid ON aeid.assay_id = a.id
            WHERE {conditions}
        """

        assay_rows = await self.connection.fetch_all(_query, values)

        # this will unique on the id, which we want due to joining on 1:many eid table
        assay_ids = [a['id'] for a in assay_rows]
        seq_eids = await self._get_assays_eids(assay_ids)
        assays = []

        project_ids: set[ProjectId] = set()
        for row in assay_rows:
            drow = dict(row)
            project_ids.add(drow.pop('project'))
            assay = AssayInternal.from_db(drow)
            assay.external_ids = seq_eids.get(assay.id, {}) if assay.id else {}
            assays.append(assay)

        return project_ids, assays

    async def get_projects_by_assay_ids(self, assay_ids: list[int]) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM assay a
            INNER JOIN sample s ON s.id = a.sample_id
            WHERE a.id in :assay_ids
            GROUP BY s.project
        """
        if len(assay_ids) == 0:
            return set()

        rows = await self.connection.fetch_all(_query, {'assay_ids': assay_ids})
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given assays, this is likely an error'
            )
        return projects

    async def get_assay_by_id(self, assay_id: int) -> tuple[ProjectId, AssayInternal]:
        """Get assay by internal ID"""
        f = AssayFilter(id=GenericFilter(eq=assay_id))
        pjcts, assays = await self.query(f)

        if not assays:
            raise NotFoundError(f'assay with id = {assay_id}')

        return pjcts.pop(), assays.pop()

    async def get_assay_by_external_id(
        self, external_sequence_id: str, project: ProjectId | None = None
    ) -> AssayInternal:
        """Get assay by EXTERNAL ID"""
        if not (project or self.project):
            raise ValueError('Getting assay by external ID requires a project')

        f = AssayFilter(
            external_id=GenericFilter(eq=external_sequence_id),
            project=GenericFilter(eq=project or self.project),
        )

        _, assays = await self.query(f)

        if not assays:
            raise NotFoundError(f'assay with external id = {external_sequence_id}')

        return assays.pop()

    async def get_assay_type_numbers_for_project(self, project: ProjectId):
        """
        This groups by samples, so one sample with many sequences ONLY reports one here,
        In the future, this should report the number of sequence groups (or something like that).
        """

        _query = """
            SELECT type, COUNT(*) as n
            FROM (
                SELECT a.type
                FROM assay a
                INNER JOIN sample s ON s.id = a.sample_id
                WHERE s.project = :project
                GROUP BY s.id, a.type
            ) as s
            GROUP BY type
        """

        rows = await self.connection.fetch_all(_query, {'project': project})

        return {r['type']: r['n'] for r in rows}

    async def get_assay_type_numbers_by_batch_for_project(self, project: ProjectId):
        """
        Grouped by the meta.batch field on an assay
        """

        # During the query, cast to the string null with IFNULL, as the GROUP BY
        # treats a SQL NULL and JSON NULL (selected from the meta.batch) differently.
        _query = """
            SELECT
                IFNULL(JSON_EXTRACT(a.meta, '$.batch'), 'null') as batch,
                sg.type,
                COUNT(*) AS n
            FROM assay a
            INNER JOIN sample s ON s.id = a.sample_id
            LEFT JOIN sequencing_group sg ON sg.sample_id = s.id
            WHERE s.project = :project
            GROUP BY batch, sg.type
        """
        rows = await self.connection.fetch_all(_query, {'project': project})
        batch_result: dict[str, dict[str, str]] = defaultdict(dict)
        for row in rows:
            batch, seqType, count = row['batch'], row['type'], row['n']
            batch = str(batch).strip('\"') if batch != 'null' else 'no-batch'
            batch_result[batch][seqType] = str(count)
        if len(batch_result) == 1 and 'no-batch' in batch_result:
            # if there are no batches, ignore the no-batch option
            return {}
        return batch_result

    # endregion GETS

    # region INSERTS
    async def insert_assay(
        self,
        sample_id,
        external_ids: dict[str, str] | None,
        assay_type: str,
        meta: dict[str, Any] | None,
        project: int | None = None,
        open_transaction: bool = True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        if not assay_type:
            raise ValueError('An assay MUST have a type')

        if not sample_id:
            raise ValueError('An assay MUST be assigned to a sample ID')
        meta = meta or {}
        # TODO: revise this based on outcome of https://centrepopgen.slack.com/archives/C03FZL2EF24/p1681274510159009
        if assay_type == 'sequencing':
            required_fields = [
                'sequencing_type',
                'sequencing_platform',
                'sequencing_technology',
            ]
            missing_fields = [f for f in required_fields if f not in meta]
            if missing_fields:
                raise ValueError(
                    f'Assay of type sequencing is missing required meta fields: {missing_fields}'
                )

        _query = """\
            INSERT INTO assay
                (sample_id, meta, type, audit_log_id)
            VALUES (:sample_id, :meta, :type, :audit_log_id)
            RETURNING id;
        """

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            id_of_new_assay = await self.connection.fetch_val(
                _query,
                {
                    'sample_id': sample_id,
                    'meta': to_db_json(meta),
                    'type': assay_type,
                    'audit_log_id': await self.audit_log_id(),
                },
            )

            if external_ids:
                _project = project or self.project
                if not _project:
                    raise ValueError(
                        'When inserting an external identifier for a sequence, a '
                        'project must be specified. This might be a server error.'
                    )

                _eid_query = """
                INSERT INTO assay_external_id
                    (project, assay_id, external_id, name, audit_log_id)
                VALUES (:project, :assay_id, :external_id, :name, :audit_log_id);
                """
                audit_log_id = await self.audit_log_id()
                eid_values = [
                    {
                        'project': project or self.project,
                        'assay_id': id_of_new_assay,
                        'external_id': eid,
                        'name': name.lower(),
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in external_ids.items()
                ]

                await self.connection.execute_many(_eid_query, eid_values)

        return id_of_new_assay

    async def insert_many_assays(
        self, assays: list[AssayInternal], open_transaction: bool = True
    ):
        """Insert many sequencing, returning no IDs"""
        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            assay_ids = []
            for assay in assays:
                # need to do it one by one to insert into relevant tables
                # at least do it in a transaction
                assay_ids.append(
                    await self.insert_assay(
                        sample_id=assay.sample_id,
                        external_ids=assay.external_ids,
                        meta=assay.meta,
                        assay_type=assay.type,
                        open_transaction=False,
                    )
                )
            return assay_ids

    # endregion INSERTS

    async def update_assay(
        self,
        assay_id: int,
        *,
        external_ids: dict[str, str] | None = None,
        meta: dict | None = None,
        assay_type: str | None = None,
        sample_id: int | None = None,
        project: ProjectId | None = None,
        open_transaction: bool = True,
    ):
        """Update an assay"""
        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            audit_log_id = await self.audit_log_id()
            fields: dict[str, Any] = {
                'assay_id': assay_id,
                'audit_log_id': audit_log_id,
            }

            updaters = ['audit_log_id = :audit_log_id']
            if meta is not None:
                updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')
                fields['meta'] = to_db_json(meta)

            if assay_type is not None:
                updaters.append('type = :assay_type')
                fields['assay_type'] = assay_type

            if sample_id is not None:
                updaters.append('sample_id = :sample_id')
                fields['sample_id'] = sample_id

            _query = f"""
                UPDATE assay
                SET {", ".join(updaters)}
                WHERE id = :assay_id
            """
            await self.connection.execute(_query, fields)

            if external_ids:
                _project = project or self.project
                if not _project:
                    raise ValueError(
                        'When inserting or updating an external identifier for an '
                        'assay, a project must be specified. This might be a '
                        'server error.'
                    )

                to_delete = {k.lower() for k, v in external_ids.items() if v is None}
                to_update = {
                    k.lower(): v for k, v in external_ids.items() if v is not None
                }

                if to_delete:
                    _assay_eid_update_before_delete = """
                    UPDATE assay_external_id
                    SET audit_log_id = :audit_log_id
                    WHERE assay_id = :assay_id AND name in :names
                    """
                    _delete_query = 'DELETE FROM assay_external_id WHERE assay_id = :assay_id AND name in :names'
                    await self.connection.execute(
                        _assay_eid_update_before_delete,
                        {
                            'assay_id': assay_id,
                            'names': list(to_delete),
                            'audit_log_id': audit_log_id,
                        },
                    )
                    await self.connection.execute(
                        _delete_query,
                        {'assay_id': assay_id, 'names': list(to_delete)},
                    )
                if to_update:
                    # we actually need the project here, get first value from list
                    project = next(
                        iter(await self.get_projects_by_assay_ids([assay_id]))
                    )

                    _update_query = """\
                        INSERT INTO assay_external_id (project, assay_id, external_id, name, audit_log_id)
                            VALUES (:project, :assay_id, :external_id, :name, :audit_log_id)
                            ON DUPLICATE KEY UPDATE external_id = :external_id, audit_log_id = :audit_log_id
                    """
                    audit_log_id = await self.audit_log_id()
                    values = [
                        {
                            'project': project,
                            'assay_id': assay_id,
                            'external_id': eid,
                            'name': name,
                            'audit_log_id': audit_log_id,
                        }
                        for name, eid in to_update.items()
                    ]
                    await self.connection.execute_many(_update_query, values)

            return True

    async def get_assays_by(
        self,
        assay_ids: list[int] | None = None,
        sample_ids: list[int] | None = None,
        assay_types: list[str] | None = None,
        assay_meta: dict[str, Any] | None = None,
        sample_meta: dict[str, Any] | None = None,
        external_assay_ids: list[str] | None = None,
        project_ids: list[int] | None = None,
        active: bool = True,
    ) -> tuple[list[ProjectId], list[AssayInternal]]:
        """Get sequences by some criteria"""
        keys = [
            'a.id',
            'a.sample_id',
            'a.type',
            'a.meta',
            's.project',
        ]
        keys_str = ', '.join(keys)

        where = []
        replacements: dict[str, Any] = {}

        if project_ids:
            where.append('s.project in :project_ids')
            replacements['project_ids'] = project_ids

        if sample_ids:
            where.append('s.id in :sample_ids')
            replacements['sample_ids'] = sample_ids

        if assay_meta:
            for k, v in assay_meta.items():
                k_replacer = fix_replacement_key(f'assay_meta_{k}')
                while k_replacer in replacements:
                    k_replacer += '_breaker'
                where.append(f"JSON_EXTRACT(a.meta, '$.{k}') = :{k_replacer}")
                replacements[k_replacer] = v

        if sample_meta:
            for k, v in sample_meta.items():
                k_replacer = fix_replacement_key(f'sample_meta_{k}')
                while k_replacer in replacements:
                    k_replacer += '_breaker'
                where.append(f"JSON_EXTRACT(s.meta, '$.{k}') = :{k_replacer}")
                replacements[k_replacer] = v

        if assay_ids:
            where.append('a.id in :assay_ids')
            replacements['assay_ids'] = assay_ids

        if external_assay_ids:
            if not project_ids:
                raise ValueError(
                    'To search assays by external_ids, you MUST supply a list of projects.'
                )
            where.append(
                'aeid.external_id in :external_ids AND aeid.project in :project_ids'
            )
            replacements['external_ids'] = external_assay_ids

        if assay_types:
            where.append('a.type in :types')
            replacements['types'] = assay_types

        if active is True:
            where.append('s.active')
        elif active is False:
            where.append('NOT active')

        _query = f"""\
            SELECT {keys_str}
            FROM assay a
            INNER JOIN sample s ON a.sample_id = s.id
            LEFT OUTER JOIN assay_external_id aeid ON a.id = aeid.assay_id
        """
        if where:
            _query += f' WHERE {" AND ".join(where)};'

        rows = await self.connection.fetch_all(_query, replacements)

        assay_rows = [dict(s) for s in rows]

        # this will unique on the id, which we want due to joining on 1:many eid table
        assays = {s['id']: AssayInternal.from_db(s) for s in assay_rows}
        seq_eids = await self._get_assays_eids(list(assays.keys()))
        for assay_id, assay in assays.items():
            assay.external_ids = seq_eids.get(assay_id, {})

        projs = list(set([s['project'] for s in assay_rows]))

        return projs, list(assays.values())

    async def get_assays_for_sequencing_group_ids(
        self, sequencing_group_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[AssayInternal]]]:
        """Get all assays for sequencing group ids"""
        keys = [
            'a.id',
            'a.sample_id',
            'a.type',
            'a.meta',
            's.project',
            'ae.name',
            'ae.external_id',
        ]
        wheres = [
            'sga.sequencing_group_id IN :sequencing_group_ids',
        ]

        _query = f"""
            SELECT {', '.join(keys)}, sga.sequencing_group_id
            FROM sequencing_group_assay sga
            INNER JOIN assay a ON sga.assay_id = a.id
            INNER JOIN sample s ON a.sample_id = s.id
            LEFT JOIN assay_external_id ae ON a.id = ae.assay_id
            WHERE {' AND '.join(wheres)}
        """

        rows = await self.connection.fetch_all(
            _query, {'sequencing_group_ids': sequencing_group_ids}
        )
        by_sequencing_group_id: dict[int, list[AssayInternal]] = defaultdict(list)
        projects: set[ProjectId] = set()
        for row in rows:
            drow = dict(row)

            external_id = drow.pop('external_id', None)
            if external_id:
                drow['external_ids'] = {drow.pop('name'): external_id}
            else:
                drow['external_ids'] = {}
                del drow['name']

            sequencing_group_id = drow.pop('sequencing_group_id')
            projects.add(drow.pop('project'))
            assay = AssayInternal.from_db(drow)
            by_sequencing_group_id[sequencing_group_id].append(assay)

        return projects, by_sequencing_group_id

    # region EIDs

    async def _get_assay_external_ids(self, assay_id):
        return (await self._get_assays_eids([assay_id])).get(assay_id, {})

    async def _get_assays_eids(self, assay_ids: list[int]) -> dict[int, dict[str, str]]:
        if len(assay_ids) == 0:
            return {}

        _query = """\
            SELECT assay_id, name, external_id
            FROM assay_external_id
            WHERE assay_id IN :assay_ids
        """

        rows = await self.connection.fetch_all(_query, {'assay_ids': assay_ids})
        by_assay_id: dict[int, dict[str, str]] = defaultdict(dict)
        for row in rows:
            seq_id = row['assay_id']
            eid_name = row['name']
            by_assay_id[seq_id][eid_name] = row['external_id']

        return by_assay_id

    # endregion EIDs
