# pylint: disable=too-many-locals,too-many-arguments

import re
import asyncio
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Iterable, Set, Any
from api.utils import group_by

from db.python.connect import DbBase, NotFoundError
from db.python.utils import to_db_json
from db.python.tables.project import ProjectId
from models.enums import SequenceType, SequenceStatus, SequenceTechnology
from models.models.sequence import SampleSequencing


class NoOpAenter:
    """
    Sometimes it's useful to use `async with VARIABLE()`, and have
    either VARIABLE be a transaction, or noop (eg: when a transaction
    is already taking place). Use this in place.
    """

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


REPLACEMENT_KEY_INVALID_CHARS = re.compile('[^A-z0-9_]')


def fix_replacement_key(k):
    """Fix a DB replacement key"""
    if not k or not isinstance(k, str):
        raise ValueError(f'Replacement key was not valid: {k} {type(k)}')
    k = REPLACEMENT_KEY_INVALID_CHARS.sub('_', k)
    if not k[0].isalpha():
        k = 'k' + k
    return k


class SampleSequencingTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample_sequencing'

    COMMON_GET_KEYS = [
        'id',
        'sample_id',
        'type',
        'meta',
        'status',
        'technology',
    ]

    async def get_projects_by_sequence_ids(
        self, sequence_ids: List[int]
    ) -> Set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM sample_sequencing sq
            INNER JOIN sample s ON s.id = sq.sample_id
            WHERE sq.id in :sequence_ids
            GROUP BY s.project
        """
        if len(sequence_ids) == 0:
            raise ValueError('Received no sequence IDs to get project ids for')
        rows = await self.connection.fetch_all(_query, {'sequence_ids': sequence_ids})
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given sequences, this is likely an error'
            )
        return projects

    async def insert_many_sequencing(
        self,
        sequencing: List[SampleSequencing],
        author=None,
    ):
        """Insert many sequencing, returning no IDs"""

        async with self.connection.transaction():
            promises = []
            for sequence in sequencing:
                # need to do it one by one to insert into relevant tables
                # at least do it in a transaction
                promises.append(
                    self.insert_sequencing(
                        sample_id=sequence.sample_id,
                        external_ids=sequence.external_ids,
                        sequence_type=sequence.type,
                        status=sequence.status,
                        technology=sequence.technology,
                        sequence_meta=sequence.meta,
                        author=author,
                        open_transaction=False,
                    )
                )
            return await asyncio.gather(*promises)

    async def insert_sequencing(
        self,
        sample_id,
        external_ids: Optional[dict[str, str]],
        sequence_type: SequenceType,
        technology: SequenceTechnology,
        status: SequenceStatus,
        sequence_meta: Optional[Dict[str, Any]] = None,
        author: Optional[str] = None,
        project: Optional[int] = None,
        open_transaction: bool = True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        if technology is None:
            raise ValueError('Sequence technology cannot be null')

        _query = """\
            INSERT INTO sample_sequencing
                (sample_id, type, technology, meta, status, author)
            VALUES (:sample_id, :type, :technology, :meta, :status, :author)
            RETURNING id;
        """

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            id_of_new_sequence = await self.connection.fetch_val(
                _query,
                {
                    'sample_id': sample_id,
                    'type': sequence_type.value,
                    'technology': technology.value if technology else None,
                    'meta': to_db_json(sequence_meta),
                    'status': status.value,
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
                INSERT INTO sample_sequencing_eid
                    (project, sequencing_id, external_id, name, author)
                VALUES (:project, :sequencing_id, :external_id, :name, :author);
                """
                eid_values = [
                    {
                        'project': project or self.project,
                        'sequencing_id': id_of_new_sequence,
                        'external_id': eid,
                        'name': name.lower(),
                        'author': author or self.author,
                    }
                    for name, eid in external_ids.items()
                ]

                await self.connection.execute_many(_eid_query, eid_values)

        return id_of_new_sequence

    async def get_sequence_by_id(
        self, sequence_id: int
    ) -> Tuple[ProjectId, SampleSequencing]:
        """Get sequence by sequence ID"""
        keys_str = ', '.join('sq.' + k for k in self.COMMON_GET_KEYS)
        _query = f"""
            SELECT {keys_str}, s.project as project
            FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            WHERE sq.id = :id
        """
        d = await self.connection.fetch_one(_query, {'id': sequence_id})
        if not d:
            raise NotFoundError(f'sequence with id = {sequence_id}')

        d = dict(d)
        d['external_ids'] = await self._get_sequence_eids(sequence_id)
        return d.pop('project'), SampleSequencing.from_db(d)

    async def get_sequence_by_external_id(
        self, external_sequence_id: str, project: int = None
    ) -> SampleSequencing:
        """Get sequence by EXTERNAL sequence ID"""
        if not (project or self.project):
            raise ValueError(
                'No project scope was specified when getting sequence by external ID'
            )

        keys_str = ', '.join('sq.' + k for k in self.COMMON_GET_KEYS)
        _query = f"""
            SELECT {keys_str}
            FROM sample_sequencing sq
            INNER JOIN sample_sequencing_eid sqeid ON sqeid.sequencing_id = sq.id
            WHERE sqeid.external_id = :external_id AND project = :project
        """
        d = await self.connection.fetch_one(
            _query,
            {'external_id': external_sequence_id, 'project': project or self.project},
        )
        if not d:
            raise NotFoundError(f'sequence with external id = {external_sequence_id}')
        d = dict(d)
        d['external_ids'] = await self._get_sequence_eids(d['id'])

        return SampleSequencing.from_db(d)

    async def get_sequence_ids_for_sample_id(
        self, sample_id: int
    ) -> Tuple[List[ProjectId], Dict[str, List[int]]]:
        """
        Get the sequence IDs from internal sample_id and sequence type
        Map them to be keyed on sequence type
        """
        # TODO: how should we address this, because we ultimately want to split on the technology I think
        _query = """\
            SELECT sq.id, s.project, sq.type
            FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            WHERE sample_id = :sample_id
            ORDER by sq.id DESC
        """
        result = await self.connection.fetch_all(_query, {'sample_id': sample_id})
        if not result:
            raise NotFoundError

        seq_map: Dict[str, List[int]] = defaultdict(list)
        projects: Set[int] = set()
        for sid, pid, stype in result:
            seq_map[stype].append(sid)
            projects.add(pid)

        return list(projects), seq_map

    async def get_sequence_ids_for_sample_ids_by_type(
        self, sample_ids: List[int]
    ) -> Tuple[Iterable[ProjectId], Dict[int, Dict[SequenceType, list[int]]]]:
        """
        Get the IDs of sequences for a sample, keyed by the internal sample ID
        """
        if not sample_ids:
            return [], {}

        _query = """
             SELECT sq.id, sq.type, sq.sample_id, s.project
             FROM sample_sequencing sq
             INNER JOIN sample s ON sq.sample_id = s.id
             WHERE sample_id IN :sample_ids
             ORDER by sq.id DESC;
         """

        # hopefully there aren't too many
        sequences = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        projects = set(s['project'] for s in sequences)
        sample_id_to_seq_id: Dict[int, Dict[SequenceType, list[int]]] = defaultdict(
            dict
        )

        # group_by preserves ordering
        for key, seqs in group_by(
            sequences, lambda s: (s['sample_id'], s['type'])
        ).items():
            sample_id, stype = key
            # get all
            sample_id_to_seq_id[sample_id][SequenceType(stype)] = [
                s['id'] for s in seqs
            ]

        return projects, sample_id_to_seq_id

    async def get_sequence_type_numbers_for_project(self, project: ProjectId):
        """
        This groups by samples, so one sample with many sequences ONLY reports one here,
        In the future, this should report the number of sequence groups (or something like that).
        """

        _query = """
            SELECT type, COUNT(*) as n
            FROM (
                SELECT sq.type
                FROM sample_sequencing sq
                INNER JOIN sample s ON s.id = sq.sample_id
                WHERE s.project = :project
                GROUP BY s.id, sq.type
            ) as s
            GROUP BY type
        """

        rows = await self.connection.fetch_all(_query, {'project': project})

        return {r['type']: r['n'] for r in rows}

    async def get_sequence_type_numbers_by_batch_for_project(self, project: ProjectId):
        """
        Grouped by the meta.batch field on a sequence
        """

        # During the query, cast to the string null with IFNULL, as the GROUP BY
        # treats a SQL NULL and JSON NULL (selected from the meta.batch) differently.
        _query = """
            SELECT IFNULL(JSON_EXTRACT(sq.meta, '$.batch'), 'null') as batch, sq.type, COUNT(*) AS n
            FROM sample_sequencing sq
            INNER JOIN sample s ON s.id = sq.sample_id
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

    async def update_status(
        self,
        sequencing_id: int,
        status: SequenceStatus,
        author=None,
    ):
        """Update status of sequencing with sequencing_id"""
        _query = """
            UPDATE sample_sequencing
            SET status = :status, author=:author
            WHERE id = :sequencing_id
        """

        await self.connection.execute(
            _query,
            {
                'status': status.value,
                'author': author or self.author,
                'sequencing_id': sequencing_id,
            },
        )

    async def update_sequence(
        self,
        sequencing_id: int,
        *,
        external_ids: Optional[dict[str, str]] = None,
        status: Optional[SequenceStatus] = None,
        technology: Optional[SequenceTechnology] = None,
        sequence_type: Optional[SequenceType] = None,
        meta: Optional[Dict] = None,
        project: Optional[ProjectId] = None,
        author=None,
    ):
        """Update a sequence"""

        async with self.connection.transaction():
            promises = []

            fields = {'sequencing_id': sequencing_id, 'author': author or self.author}

            updaters = ['author = :author']
            if status is not None:
                updaters.append('status = :status')
                fields['status'] = status.value
            if meta is not None:
                updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')
                fields['meta'] = to_db_json(meta)
            if technology:
                updaters.append('technology = :technology')
                fields['technology'] = technology.value
            if sequence_type:
                updaters.append('type = :sequence_type')
                fields['sequence_type'] = sequence_type.value

            _query = f"""
                UPDATE sample_sequencing
                SET {", ".join(updaters)}
                WHERE id = :sequencing_id
            """
            promises.append(self.connection.execute(_query, fields))

            if external_ids:
                _project = project or self.project
                if not _project:
                    raise ValueError(
                        'When inserting or updating an external identifier for a '
                        'sequence, a project must be specified. This might be a '
                        'server error.'
                    )

                to_delete = {k.lower() for k, v in external_ids.items() if v is None}
                to_update = {
                    k.lower(): v for k, v in external_ids.items() if v is not None
                }

                if to_delete:
                    _delete_query = 'DELETE FROM sample_sequencing_eid WHERE sequencing_id = :seq_id AND name in :names'
                    promises.append(
                        self.connection.execute(
                            _delete_query,
                            {'seq_id': sequencing_id, 'names': list(to_delete)},
                        )
                    )
                if to_update:
                    # we actually need the project here, get first value from list
                    project = next(
                        iter(await self.get_projects_by_sequence_ids([sequencing_id]))
                    )

                    _update_query = """\
                        INSERT INTO sample_sequencing_eid (project, sequencing_id, external_id, name, author)
                            VALUES (:project, :seq_id, :external_id, :name, :author)
                            ON DUPLICATE KEY UPDATE external_id = :external_id, author = :author
                    """
                    values = [
                        {
                            'project': project,
                            'seq_id': sequencing_id,
                            'external_id': eid,
                            'name': name,
                            'author': author or self.author,
                        }
                        for name, eid in to_update.items()
                    ]
                    promises.append(self.connection.execute_many(_update_query, values))

            await asyncio.gather(*promises)

            return True

    async def get_sequences_by(
        self,
        sample_ids: List[int] = None,
        seq_meta: Dict[str, Any] = None,
        sample_meta: Dict[str, Any] = None,
        sequence_ids: List[int] = None,
        external_sequence_ids: List[str] = None,
        project_ids=None,
        active=True,
        types: list[SequenceType] | list[str] = None,
        technologies: list[SequenceTechnology] | list[str] = None,
        statuses: list[SequenceStatus] | list[str] = None,
    ) -> Tuple[list[ProjectId], list[SampleSequencing]]:
        """Get sequences by some criteria"""
        keys = [
            'sq.id',
            'sq.sample_id',
            'sq.type',
            'sq.status',
            'sq.technology',
            'sq.meta',
            's.project',
        ]
        keys_str = ', '.join(keys)

        where = []
        replacements = {}

        if project_ids:
            where.append('s.project in :project_ids')
            replacements['project_ids'] = project_ids

        if sample_ids:
            where.append('s.id in :sample_ids')
            replacements['sample_ids'] = sample_ids

        if seq_meta:
            for k, v in seq_meta.items():
                k_replacer = fix_replacement_key(f'seq_meta_{k}')
                while k_replacer in replacements:
                    k_replacer += '_breaker'
                where.append(f"JSON_EXTRACT(sq.meta, '$.{k}') = :{k_replacer}")
                replacements[k_replacer] = v

        if sample_meta:
            for k, v in sample_meta.items():
                k_replacer = fix_replacement_key(f'sample_meta_{k}')
                while k_replacer in replacements:
                    k_replacer += '_breaker'
                where.append(f"JSON_EXTRACT(s.meta, '$.{k}') = :{k_replacer}")
                replacements[k_replacer] = v

        if sequence_ids:
            where.append('sq.id in :sequence_ids')
            replacements['sequence_ids'] = sequence_ids

        if external_sequence_ids:
            if not project_ids:
                raise ValueError(
                    'To search sequences by external_ids, you MUST a list of projects.'
                )
            where.append(
                'sqeid.external_id in :external_ids AND sqeid.project in :project_ids'
            )
            replacements['external_ids'] = [s.lower() for s in external_sequence_ids]

        if types:
            seq_types = [s.value if isinstance(s, SequenceType) else s for s in types]
            where.append('sq.type in :types')
            replacements['types'] = seq_types

        if technologies:
            seq_techs = [
                s.value if isinstance(s, SequenceTechnology) else s
                for s in technologies
            ]
            where.append('sq.technology in :techs')
            replacements['techs'] = seq_techs

        if statuses:
            where.append('sq.status in :statuses')
            replacements['statuses'] = statuses

        if active is True:
            where.append('s.active')
        elif active is False:
            where.append('NOT active')

        _query = f"""\
            SELECT {keys_str}
            FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            LEFT OUTER JOIN sample_sequencing_eid sqeid ON sq.id = sqeid.sequencing_id
        """
        if where:
            _query += f' WHERE {" AND ".join(where)};'

        rows = await self.connection.fetch_all(_query, replacements)

        sequence_dicts = [dict(s) for s in rows]

        # this will unique on the id, which we want due to joining on 1:many eid table
        sequences = {s['id']: SampleSequencing.from_db(s) for s in sequence_dicts}
        seq_eids = await self._get_sequences_eids(list(sequences.keys()))
        for seqid, seq in sequences.items():
            seq.external_ids = seq_eids.get(seqid, {})

        projs = list(set([s['project'] for s in sequence_dicts]))

        return projs, list(sequences.values())

    async def get_participant_ids_and_sample_ids_for_sequence_type(
        self, sequence_type: SequenceType
    ) -> tuple[set[ProjectId], dict[int, list[int]]]:
        """
        Get participant IDs for a specific sequence type.
        Particularly useful for seqr like cases
        """
        _query = """
    SELECT s.project as project, s.id as sid, s.participant_id as pid
    FROM sample_sequencing sq
    INNER JOIN sample s ON sq.sample_id = s.id
    WHERE sq.type = :seqtype AND project = :project
        """

        rows = list(
            await self.connection.fetch_all(
                _query, {'seqtype': sequence_type.value, 'project': self.project}
            )
        )

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    # region EIDs

    async def _get_sequence_eids(self, sequence_id):
        return (await self._get_sequences_eids([sequence_id])).get(sequence_id, {})

    async def _get_sequences_eids(
        self, sequence_ids: list[int]
    ) -> dict[int, dict[str, str]]:
        if len(sequence_ids) == 0:
            return {}

        _query = """\
            SELECT sequencing_id, name, external_id
            FROM sample_sequencing_eid
            WHERE sequencing_id IN :sequence_ids
        """

        rows = await self.connection.fetch_all(_query, {'sequence_ids': sequence_ids})
        by_sequence_id: dict[int, dict[str, str]] = defaultdict(dict)
        for row in rows:
            seq_id = row['sequencing_id']
            eid_name = row['name']
            by_sequence_id[seq_id][eid_name] = row['external_id']

        return by_sequence_id

    # endregion EIDs