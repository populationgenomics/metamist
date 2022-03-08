from collections import defaultdict
from itertools import groupby
from typing import Optional, Dict, List, Tuple, Iterable, Set, Any

from db.python.connect import DbBase, to_db_json, NotFoundError
from db.python.tables.project import ProjectId
from models.enums import SequenceType, SequenceStatus
from models.models.sequence import SampleSequencing


class SampleSequencingTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample_sequencing'

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

        _query = """\
            INSERT INTO sample_sequencing
                (sample_id, type, meta, status, author)
            VALUES (:sample_id, :type, :meta, :status, :author);
        """

        values = [
            {
                'sample_id': s.sample_id,
                'type': s.type.value,
                'meta': to_db_json(s.meta),
                'status': s.status.value,
                'author': author or self.author,
            }
            for s in sequencing
        ]

        # with encode/database, can't execute many and collect the results
        await self.connection.execute_many(_query, values)

    async def insert_sequencing(
        self,
        sample_id,
        sequence_type: SequenceType,
        status: SequenceStatus,
        sequence_meta: Optional[Dict[str, Any]] = None,
        author: Optional[str] = None,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        _query = """\
            INSERT INTO sample_sequencing
                (sample_id, type, meta, status, author)
            VALUES (:sample_id, :type, :meta, :status, :author)
            RETURNING id;
        """

        id_of_new_sample = await self.connection.fetch_val(
            _query,
            {
                'sample_id': sample_id,
                'type': sequence_type.value,
                'meta': to_db_json(sequence_meta),
                'status': status.value,
                'author': author or self.author,
            },
        )

        return id_of_new_sample

    async def get_sequence_by_id(
        self, sequence_id: int
    ) -> Tuple[ProjectId, SampleSequencing]:
        """Get sequence by sequence ID"""
        keys = [
            'id',
            'sample_id',
            'type',
            'meta',
            'status',
        ]
        keys_str = ', '.join('sq.' + k for k in keys)
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

        return d.pop('project'), SampleSequencing.from_db(d)

    async def get_latest_sequence_id_for_sample_id(
        self, sample_id: int
    ) -> Tuple[ProjectId, int]:
        """
        Get latest added sequence ID from internal sample_id
        """
        _query = """\
            SELECT sq.id, s.project FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            WHERE sample_id = :sample_id
            ORDER by sq.id DESC
            LIMIT 1
        """
        result = await self.connection.fetch_one(_query, {'sample_id': sample_id})
        if not result:
            raise NotFoundError

        return result['project'], result['id']

    async def get_sequence_id_from_sample_id_and_type(
        self, sample_id: int, stype: SequenceType
    ) -> Tuple[ProjectId, int]:
        """
        Get the sequence ID from internal sample_id and sequence type
        """
        _query = """\
            SELECT sq.id, s.project
            FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            WHERE sample_id = :sample_id
            AND type = :type
            ORDER by sq.id DESC
            LIMIT 1
        """
        result = await self.connection.fetch_one(
            _query, {'sample_id': sample_id, 'type': stype}
        )
        if not result:
            raise NotFoundError

        return result['project'], result['id']

    async def get_all_sequence_id_for_sample_id(self, sample_id: int):
        """
        Get the sequence IDs from internal sample_id and sequence type
        Map them to be keyed on sequence type
        """
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

        seq_map: Dict[str, int] = defaultdict(int)
        projects: List[str] = []
        for sid, pid, stype in result:
            seq_map[stype] = sid
            projects.append(pid)

        return projects, seq_map

    async def get_latest_sequence_id_for_external_sample_id(
        self, project: ProjectId, external_sample_id
    ) -> int:
        """
        Get latest added sequence ID from external sample_id
        """
        _query = """\
            SELECT sq.id from sample_sequencing sq
            INNER JOIN sample s ON s.id = sq.sample_id
            WHERE s.external_id = :external_id AND s.project = :project
            ORDER by s.id DESC
            LIMIT 1
        """
        result = await self.connection.fetch_val(
            _query, {'project': project, 'external_id': external_sample_id}
        )
        return result

    async def get_sequence_ids_from_sample_ids(
        self, sample_ids: List[int]
    ) -> Tuple[Iterable[ProjectId], Dict[int, Dict[SequenceType, int]]]:
        """
        Get the IDs of the latest sequence for a sample, keyed by the internal sample ID
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
        sample_id_to_seq_id: Dict[int, Dict[SequenceType, int]] = defaultdict(dict)

        # groupby preserves ordering
        for key, seqs in groupby(sequences, lambda s: (s['sample_id'], s['type'])):
            sample_id, stype = key
            print(sample_id, type(sample_id))
            sample_id_to_seq_id[sample_id][SequenceType(stype)] = list(seqs)[-1][
                'id'
            ]  # get last one

        return projects, sample_id_to_seq_id

    async def get_latest_sequence_ids_for_sample_ids(
        self, sample_ids: List[int]
    ) -> Tuple[Iterable[ProjectId], Dict[int, int]]:
        """
        Get the IDs of the latest sequence for a sample, keyed by the internal sample ID
        """
        if not sample_ids:
            return [], {}

        _query = """
            SELECT sq.id, sq.sample_id, s.project FROM sample_sequencing sq
            INNER JOIN sample s on sq.sample_id = s.id
            WHERE sq.sample_id in :sample_ids
            ORDER by sq.id DESC;
        """
        # hopefully there aren't too many
        sequences = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        projects = set(s['project'] for s in sequences)
        sample_id_to_seq_id = {}
        # groupby preserves ordering
        for sample_id, seqs in groupby(sequences, lambda seq: seq['sample_id']):
            sample_id_to_seq_id[sample_id] = list(seqs)[-1]['id']  # get last one

        return projects, sample_id_to_seq_id

    async def get_latest_sequence_for_sample_ids(
        self, sample_ids: List[int], get_latest_sequence_only=True
    ) -> Tuple[Iterable[ProjectId], List[SampleSequencing]]:
        """Get a list of sequence objects by their internal sample IDs"""
        # there's an implicit ordering by id
        _query = f"""
            SELECT sq.id, sq.sample_id, sq.type, sq.meta, sq.status, s.project
            FROM sample_sequencing sq
            INNER JOIN sample s ON sq.sample_id = s.id
            WHERE sq.sample_id in :sample_ids
        """
        rows = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        sequence_dicts = [dict(s) for s in rows]
        projects = set(s.pop('project') for s in sequence_dicts)
        if get_latest_sequence_only:
            # get last one
            sequence_dicts = [
                list(seqs)[-1]
                for _, seqs in groupby(sequence_dicts, lambda seq: seq['sample_id'])
            ]

        sequences = [SampleSequencing.from_db(s) for s in sequence_dicts]
        return projects, sequences

    async def update_status(
        self,
        sequencing_id,
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
        sequence_id,
        status: Optional[SequenceStatus] = None,
        meta: Optional[Dict] = None,
        author=None,
    ):
        """Update a sequence"""

        fields = {'sequencing_id': sequence_id, 'author': author or self.author}

        updaters = ['author = :author']
        if status is not None:
            updaters.append('status = :status')
            fields['status'] = status.value
        if meta is not None:

            updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')
            fields['meta'] = to_db_json(meta)

        _query = f"""
            UPDATE sample_sequencing
            SET {", ".join(updaters)}
            WHERE id = :sequencing_id
        """
        await self.connection.execute(_query, fields)
        return True
