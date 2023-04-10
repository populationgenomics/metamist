from collections import defaultdict

from db.python.connect import DbBase, NoOpAenter
from db.python.utils import ProjectId, to_db_json


class SequencingGroupTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sequencing_group'

    async def get_projects_by_sequencing_group_ids(
        self, sequencing_group_ids: list[int]
    ) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM sequencing_group sqg
            INNER JOIN sample s ON s.id = sqg.sample_id
            WHERE sqg.id in :sequencing_group_ids
            GROUP BY s.project
        """
        if len(sequencing_group_ids) == 0:
            raise ValueError('Received no sequence group IDs to get project ids for')

        rows = await self.connection.fetch_all(
            _query, {'sequencing_group_ids': sequencing_group_ids}
        )
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given sequence groups, this is likely an error'
            )

        return projects

    async def get_sequencing_groups_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[dict]]:
        """
        Get sequence groups by internal identifiers
        """
        _query = """
            SELECT project, sample_id, type, technology, platform, meta, author
            FROM sequencing_group
            WHERE id IN :sqgids
        """

        rows = await self.connection.fetch_all(_query, {'sqgids': ids})
        rows = [dict(r) for r in rows]
        projects = set(r['project'] for r in rows)

        return projects, rows

    async def get_sequence_ids_by_sequencing_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequence IDs in a sequencing_group
        """
        _query = """
            SELECT sequencing_group_id, sequencing_id
            FROM sequencing_group_sequence
            WHERE sequencing_group_id IN :sqgids
        """
        rows = await self.connection.fetch_all(_query, {'sqgids': ids})
        sequencing_groups: dict[int, list[int]] = defaultdict(list)
        for row in rows:
            sequencing_groups[row['sequencing_group_id']].append(row['sequencing_id'])

        return dict(sequencing_groups)

    async def get_participant_ids_and_sequence_group_ids_for_sequence_type(
        self, sequence_type: str
    ) -> tuple[set[ProjectId], dict[int, list[int]]]:
        """
        Get participant IDs for a specific sequence type.
        Particularly useful for seqr like cases
        """
        _query = """
    SELECT s.project as project, sg.id as sid, s.participant_id as pid
    FROM sequencing_group sg
    INNER JOIN sample s ON sq.sample_id = s.id
    WHERE sg.type = :seqtype AND project = :project
        """

        rows = list(
            await self.connection.fetch_all(
                _query, {'seqtype': sequence_type, 'project': self.project}
            )
        )

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    async def create_sequencing_group(
        self,
        sample_id: int,
        type_: str,
        technology: str,
        platform: str,
        meta: dict,
        sequence_ids: list[int],
        author: str = None,
        open_transaction=True,
    ):
        """Create sequence group"""
        _query = """
        INSERT INTO sequencing_group
            (sample_id, type, technology, platform, meta, author)
        VALUES (:sample_id, :type, :technology, :platform, :meta, :author)
        RETURNING id;
        """

        _seqg_linker_query = """
        INSERT INTO sequencing_group_assay
            (sequencing_group_id, assay_id, author)
        VALUES
            (:seqgroup, :assayid, :author)
        """

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            id_of_seq_group = await self.connection.fetch_val(
                _query,
                {
                    'sample_id': sample_id,
                    'type': type_,
                    'technology': technology,
                    'platform': platform.upper() if platform else None,
                    'meta': to_db_json(meta),
                    'author': author or self.author,
                },
            )
            sequence_insert_values = [
                {
                    'seqgroup': id_of_seq_group,
                    'assayid': s,
                    'author': author or self.author,
                }
                for s in sequence_ids
            ]
            await self.connection.execute_many(
                _seqg_linker_query, sequence_insert_values
            )

            return id_of_seq_group

    async def update_sequencing_group(
        self, sequencing_group_id: int, meta: dict, platform: str
    ):
        """
        Update meta / platform on sequencing_group
        """
        updaters = ['JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)']
        values = {'seqgid': sequencing_group_id, 'meta': to_db_json(meta)}
        if platform:
            updaters.append('platform = :platform')
            values['platform'] = platform

        _query = f"""
        UPDATE sequencing_group
        SET {', '.join(updaters)}
        WHERE id = :seqgid
        """

        await self.connection.execute(_query, values)

    async def archive_sequencing_groups(self, sequencing_group_id: list[int]):
        """
        Archive sequence group by setting archive flag to TRUE
        """
        _query = """
        UPDATE sequencing_group
        SET archive = 1, author = :author
        WHERE id = :sequencing_group_id;
        """
        return await self.connection.execute(
            _query, {'sequencing_group_id': sequencing_group_id, 'author': self.author}
        )
