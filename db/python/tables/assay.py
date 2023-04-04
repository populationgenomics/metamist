# pylint: disable=too-many-locals,too-many-arguments

import re
import asyncio
from collections import defaultdict
from typing import Iterable, Any
from api.utils import group_by

from db.python.connect import DbBase, NotFoundError, NoOpAenter
from db.python.utils import to_db_json
from db.python.tables.project import ProjectId
from models.models.assay import AssayInternal


REPLACEMENT_KEY_INVALID_CHARS = re.compile(r'[^\w\d_]')


def fix_replacement_key(k):
    """Fix a DB replacement key"""
    if not k or not isinstance(k, str):
        raise ValueError(f'Replacement key was not valid: {k} {type(k)}')
    k = REPLACEMENT_KEY_INVALID_CHARS.sub('_', k)
    if not k[0].isalpha():
        k = 'k' + k
    return k


class AssayTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'assay'

    COMMON_GET_KEYS = [
        'id',
        'sample_id',
        'meta',
        'type',
    ]

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

    async def insert_many_assays(
        self,
        assays: list[AssayInternal],
        author=None,
    ):
        """Insert many sequencing, returning no IDs"""

        async with self.connection.transaction():
            promises = []
            for assay in assays:
                # need to do it one by one to insert into relevant tables
                # at least do it in a transaction
                promises.append(
                    self.insert_assay(
                        sample_id=assay.sample_id,
                        external_ids=assay.external_ids,
                        meta=assay.meta,
                        author=author,
                        assay_type=assay.type,
                        open_transaction=False,
                    )
                )
            return await asyncio.gather(*promises)

    async def insert_assay(
        self,
        sample_id,
        external_ids: dict[str, str] | None,
        assay_type: str,
        meta: dict[str, Any] | None,
        author: str | None = None,
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

        _query = """\
            INSERT INTO assay
                (sample_id, meta, type, author)
            VALUES (:sample_id, :meta, :type, :author)
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
                    'author': author or self.author,
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
                INSERT INTO assay_eid
                    (project, assay_id, external_id, name, author)
                VALUES (:project, :assay_id, :external_id, :name, :author);
                """
                eid_values = [
                    {
                        'project': project or self.project,
                        'assay_id': id_of_new_assay,
                        'external_id': eid,
                        'name': name.lower(),
                        'author': author or self.author,
                    }
                    for name, eid in external_ids.items()
                ]

                await self.connection.execute_many(_eid_query, eid_values)

        return id_of_new_assay

    async def get_assay_by_id(
        self, sequence_id: int
    ) -> tuple[ProjectId, AssayInternal]:
        """Get assay by internal ID"""
        keys_str = ', '.join('a.' + k for k in self.COMMON_GET_KEYS)

        # left join allows project-less ASSAYs, can be sometimes useful
        _query = f"""
            SELECT {keys_str}, s.project as project
            FROM assay a
            LEFT JOIN sample s ON a.sample_id = s.id
            WHERE a.id = :id
        """
        d = await self.connection.fetch_one(_query, {'id': sequence_id})
        if not d:
            raise NotFoundError(f'sequence with id = {sequence_id}')

        d = dict(d)
        d['external_ids'] = await self._get_assay_eids(sequence_id)
        return d.pop('project'), AssayInternal.from_db(d)

    async def get_assay_by_external_id(
        self, external_sequence_id: str, project: int = None
    ) -> AssayInternal:
        """Get assay by EXTERNAL ID"""
        if not (project or self.project):
            raise ValueError('Getting assay by external ID requires a project')

        keys_str = ', '.join('a.' + k for k in self.COMMON_GET_KEYS)
        _query = f"""
            SELECT {keys_str}
            FROM assay a
            INNER JOIN assay_eid aeid ON aeid.assay_id = a.id
            WHERE aeid.external_id = :external_id AND project = :project
        """
        d = await self.connection.fetch_one(
            _query,
            {'external_id': external_sequence_id, 'project': project or self.project},
        )
        if not d:
            raise NotFoundError(f'assay with external id = {external_sequence_id}')
        d = dict(d)
        d['external_ids'] = await self._get_assay_eids(d['id'])

        return AssayInternal.from_db(d)

    async def get_assay_ids_for_sample_id(
        self, sample_id: int
    ) -> tuple[ProjectId, dict[str, list[int]]]:
        """
        Get the assay IDs from internal sample_id
        Map them to be keyed on an assay type (eg: sequencing, proteomics, etc)
        """
        # TODO: how should we address this, because we ultimately want to split on the technology I think
        _query = """\
            SELECT a.id, a.type, s.project
            FROM assay a
            INNER JOIN sample s ON a.sample_id = s.id
            WHERE s.sample_id = :sample_id
            ORDER by a.id DESC
        """
        result = await self.connection.fetch_all(_query, {'sample_id': sample_id})
        if not result:
            raise NotFoundError

        assay_by_type = defaultdict(list)
        for r in result:
            assay_by_type[r['type']].append(r['id'])

        project = result[0]['project']

        return project, assay_by_type

    async def get_sequence_ids_for_sample_ids_by_type(
        self, sample_ids: list[int]
    ) -> tuple[Iterable[ProjectId], dict[int, dict[str, list[int]]]]:
        """
        Get the IDs of sequences for a sample, keyed by the internal sample ID
        """
        if not sample_ids:
            return [], {}

        _query = """
             SELECT a.id, a.type, a.sample_id, s.project
             FROM assay a
             INNER JOIN sample s ON a.sample_id = s.id
             WHERE a.sample_id IN :sample_ids
             ORDER by a.id DESC;
         """

        # hopefully there aren't too many
        assays = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        projects = set(s['project'] for s in assays)
        sample_id_to_assay_id: dict[int, dict[str, list[int]]] = defaultdict(dict)

        # group_by preserves ordering
        for key, _assays in group_by(
            assays, lambda s: (s['sample_id'], s['type'])
        ).items():
            sample_id, atype = key
            # get all
            sample_id_to_assay_id[sample_id][atype] = [s['id'] for s in _assays]

        return projects, sample_id_to_assay_id

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
                a.type,
                COUNT(*) AS n
            FROM assay a
            INNER JOIN sample s ON s.id = a.sample_id
            WHERE s.project = :project
            GROUP BY batch, type
        """
        rows = await self.connection.fetch_all(_query, {'project': project})
        batch_result: dict[str, dict[str, str]] = defaultdict(dict)
        for batch, seqType, count in rows:
            batch = str(batch).strip('\"') if batch != 'null' else 'no-batch'
            batch_result[batch][seqType] = str(count)
        if len(batch_result) == 1 and 'no-batch' in batch_result:
            # if there are no batches, ignore the no-batch option
            return {}
        return batch_result

    async def update_assay(
        self,
        assay_id: int,
        *,
        external_ids: dict[str, str] | None = None,
        meta: dict | None = None,
        assay_type: str | None = None,
        sample_id: int | None = None,
        project: ProjectId | None = None,
        author=None,
    ):
        """Update an assay"""

        async with self.connection.transaction():
            promises = []

            fields = {'assay_id': assay_id, 'author': author or self.author}

            updaters = ['author = :author']
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
            promises.append(self.connection.execute(_query, fields))

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
                    _delete_query = 'DELETE FROM assay_eid WHERE assay_id = :assay_id AND name in :names'
                    promises.append(
                        self.connection.execute(
                            _delete_query,
                            {'assay_id': assay_id, 'names': list(to_delete)},
                        )
                    )
                if to_update:
                    # we actually need the project here, get first value from list
                    project = next(
                        iter(await self.get_projects_by_assay_ids([assay_id]))
                    )

                    _update_query = """\
                        INSERT INTO assay_eid (project, assay_id, external_id, name, author)
                            VALUES (:project, :assay_id, :external_id, :name, :author)
                            ON DUPLICATE KEY UPDATE external_id = :external_id, author = :author
                    """
                    values = [
                        {
                            'project': project,
                            'assay_id': assay_id,
                            'external_id': eid,
                            'name': name,
                            'author': author or self.author,
                        }
                        for name, eid in to_update.items()
                    ]
                    promises.append(self.connection.execute_many(_update_query, values))

            await asyncio.gather(*promises)

            return True

    async def get_assays_by(
        self,
        assay_ids: list[int] = None,
        sample_ids: list[int] = None,
        assay_types: list[str] = None,
        assay_meta: dict[str, Any] = None,
        sample_meta: dict[str, Any] = None,
        external_assay_ids: list[str] = None,
        project_ids: list[int] = None,
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
                'aeid.external_id in :external_ids AND sqeid.project in :project_ids'
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
            LEFT OUTER JOIN assay_eid aeid ON a.id = aeid.assay_id
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

    # region EIDs

    async def _get_assay_eids(self, assay_id):
        return (await self._get_assays_eids([assay_id])).get(assay_id, {})

    async def _get_assays_eids(self, assay_ids: list[int]) -> dict[int, dict[str, str]]:
        if len(assay_ids) == 0:
            return {}

        _query = """\
            SELECT assay_id, name, external_id
            FROM assay_eid
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
