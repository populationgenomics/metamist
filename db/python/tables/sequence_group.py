from collections import defaultdict

from db.python.connect import DbBase
from db.python.tables.sequence import NoOpAenter
from db.python.utils import ProjectId, to_db_json
from models.enums import SequenceType, SequenceTechnology


class SequenceGroupTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sequencing_group'

    async def get_projects_by_sequence_group_ids(
        self, sequence_group_ids: list[int]
    ) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM sequencing_group sqg
            INNER JOIN sample s ON s.id = sqg.sample_id
            WHERE sqg.id in :sequence_group_ids
            GROUP BY s.project
        """
        if len(sequence_group_ids) == 0:
            raise ValueError('Received no sequence group IDs to get project ids for')

        rows = await self.connection.fetch_all(
            _query, {'sequence_group_ids': sequence_group_ids}
        )
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given sequence groups, this is likely an error'
            )

        return projects

    async def get_sequence_groups_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[dict]]:
        _query = """
            SELECT project, sample_id, type, technology, platform, meta, author
            FROM sequencing_group
            WHERE id IN :sqgids
        """

        rows = await self.connection.fetch_all(_query, {'sqgids': ids})
        rows = [dict(r) for r in rows]
        projects = set(r['project'] for r in rows)

        return projects, rows

    async def get_sequence_ids_by_sequence_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        _query = """
            SELECT sequencing_group_id, sequencing_id
            FROM sequencing_group_sequence
            WHERE sequencing_group_id IN :sqgids
        """
        rows = await self.connection.fetch_all(_query, {'sqgids': ids})
        sequence_groups = defaultdict(list)
        for row in rows:
            sequence_groups[row['sequencing_group_id']].append(row['sequencing_id'])

        return dict(sequence_groups)

    async def create_sequence_group(
        self,
        sample_id: int,
        type_: SequenceType,
        technology: SequenceTechnology,
        platform: str,
        meta: dict,
        sequence_ids: list[int],
        author: str = None,
        open_transaction=True,
    ):

        _query = """
        INSERT INTO sequence_group
            (sample_id, type, technology, platform, meta, author)
        VALUES (:sample_id, :type, :technology, :platform,, :meta, :author)
        RETURNING id;
        """

        _seqg_linker_query = """
        INSERT INTO sequencing_group_sequence
            (sequencing_group_id, sequencing_id, author)
        VALUES
            (:seqgroup, :seqid, :author)
        """

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            id_of_seq_group = self.connection.fetch_val(
                _query,
                {
                    'sample_id': sample_id,
                    'type': type_,
                    'technology': technology,
                    'platform': platform.upper(),
                    'meta': to_db_json(meta),
                    'author': author or self.author,
                },
            )
            sequence_insert_values = [
                {
                    'seqgroup': id_of_seq_group,
                    'seqid': s,
                    'author': author or self.author,
                }
                for s in sequence_ids
            ]
            await self.connection.execute_many(
                _seqg_linker_query, sequence_insert_values
            )

            return id_of_seq_group

    async def update_sequence_group(
        self, sequence_group_id: int, meta: dict, platform: str
    ):
        updaters = ['JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)']
        values = {'seqgid': sequence_group_id, 'meta': to_db_json(meta)}
        if platform:
            updaters.append('platform = :platform')
            values['platform'] = platform

        _query = f"""
        UPDATE sequence_group
        SET {', '.join(updaters)}
        WHERE id = :seqgid
        """

        await self.connection.execute(_query, values)

    async def archive_sequence_group(self, sequence_group_id):
        _query = """
        UPDATE sequence_group
        SET archive = 1, author = :author
        WHERE id = :sequence_group_id;
        """
        return await self.connection.execute(
            _query, {'sequence_group_id': sequence_group_id, 'author': self.author}
        )
