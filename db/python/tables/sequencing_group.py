from collections import defaultdict
from datetime import date
from typing import Any

from db.python.connect import DbBase, NoOpAenter
from db.python.utils import ProjectId, to_db_json
from models.models.sequencing_group import SequencingGroupInternal


class SequencingGroupTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sequencing_group'
    common_get_keys = [
        'sg.id',
        's.project',
        'sg.sample_id',
        'sg.type',
        'sg.technology',
        'sg.platform',
        'sg.meta',
        'sg.author',
    ]
    common_get_keys_str = ', '.join(common_get_keys)

    async def get_projects_by_sequencing_group_ids(
        self, sequencing_group_ids: list[int]
    ) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id in :sequencing_group_ids
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
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """
        Get sequence groups by internal identifiers
        """

        _query = f"""
            SELECT {SequencingGroupTable.common_get_keys_str}
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id IN :sgids
        """

        rows = await self.connection.fetch_all(_query, {'sgids': ids})
        rows = [SequencingGroupInternal.from_db(**dict(r)) for r in rows]
        projects = set(r.project for r in rows)

        return projects, rows

    async def get_assay_ids_by_sequencing_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequence IDs in a sequencing_group
        """
        _query = """
            SELECT sgs.sequencing_group_id, sgs.assay_id
            FROM sequencing_group_sequence sgs
            WHERE sequencing_group_id IN :sgids
        """
        rows = await self.connection.fetch_all(_query, {'sgids': ids})
        sequencing_groups: dict[int, list[int]] = defaultdict(list)
        for row in rows:
            sequencing_groups[row['sequencing_group_id']].append(row['assay_id'])

        return dict(sequencing_groups)
    
    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        
        """
        Search by some term, return [ProjectId, SampleID, SequencingGroupID]
        """
        _query = """
        SELECT s.project, sg.sample_id, sg.id
        FROM sequencing_group sg
        INNER JOIN sample s ON s.id = sg.sample_id
        LIMIT :limit
        """
        # WHERE s.project in :project_ids AND sg.id LIKE :search_pattern
        rows = await self.connection.fetch_all(
            _query,
            {
                # 'project_ids': project_ids,
                # 'search_pattern': self.escape_like_term(query) + '%',
                'limit': limit,
            },
        )
        print(rows)
        return 'test'
        return [(r['project'], r['id'], r['external_id']) for r in rows]

    async def query(
        self,
        project_ids: list[ProjectId],
        sample_ids: list[int],
        sequencing_group_ids: list[int],
        types: list[str],
        technologies: list[str],
        platforms: list[str],
        active_only: bool = True,
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """
        Query sequencing groups
        """
        wheres = []
        params: dict[str, Any] = {}
        if project_ids:
            wheres.append('s.project IN :project_ids')
            params['project_ids'] = project_ids
        if sample_ids:
            wheres.append('s.id IN :sample_ids')
            params['sample_ids'] = sample_ids
        if sequencing_group_ids:
            wheres.append('sg.id IN :sequencing_group_ids')
            params['sequencing_group_ids'] = sequencing_group_ids
        if types:
            wheres.append('sg.type IN :types')
            params['types'] = types
        if technologies:
            wheres.append('sg.technology IN :technologies')
            params['technologies'] = technologies
        if platforms:
            wheres.append('sg.platform IN :platforms')
            params['platforms'] = platforms

        if active_only:
            wheres.append('NOT sg.archived')

        where = ' AND '.join(wheres)
        _query = f"""
            SELECT {SequencingGroupTable.common_get_keys_str}
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            {'WHERE ' + where if where else ''}
        """

        rows = await self.connection.fetch_all(_query, params)
        sequencing_groups = [SequencingGroupInternal.from_db(**dict(r)) for r in rows]
        projects = set(r.project for r in sequencing_groups)
        return projects, sequencing_groups

    async def get_all_sequencing_group_ids_by_sample_ids_by_type(
        self,
    ) -> dict[int, dict[str, list[int]]]:
        """
        Get all sequencing group IDs by sample IDs by type
        """
        _query = """
        SELECT s.id as sid, sg.id as sgid, sg.type as sgtype
        FROM sample s
        INNER JOIN sequencing_group sg ON s.id = sg.sample_id
        WHERE project = :project
        """
        rows = await self.connection.fetch_all(_query, {'project': self.project})
        sequencing_group_ids_by_sample_ids_by_type: dict[
            int, dict[str, list[int]]
        ] = defaultdict(lambda: defaultdict(list))
        for row in rows:
            sample_id = row['sid']
            sg_id = row['sgid']
            sg_type = row['sgtype']
            sequencing_group_ids_by_sample_ids_by_type[sample_id][sg_type].append(sg_id)

        return sequencing_group_ids_by_sample_ids_by_type

    async def get_participant_ids_and_sequence_group_ids_for_sequencing_type(
        self, sequencing_type: str
    ) -> tuple[set[ProjectId], dict[int, list[int]]]:
        """
        Get participant IDs for a specific sequence type.
        Particularly useful for seqr like cases
        """
        _query = """
    SELECT s.project as project, sg.id as sid, s.participant_id as pid
    FROM sequencing_group sg
    INNER JOIN sample s ON sg.sample_id = s.id
    WHERE sg.type = :seqtype AND project = :project
        """

        rows = list(
            await self.connection.fetch_all(
                _query, {'seqtype': sequencing_type, 'project': self.project}
            )
        )

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    async def get_sequencing_groups_create_date(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sequencing_group_ids) == 0:
            return {}
        _query = """
        SELECT id, min(row_start)
        FROM sequencing_group FOR SYSTEM_TIME ALL
        WHERE id in :sgids
        GROUP BY id"""
        rows = await self.connection.fetch_all(_query, {'sgids': sequencing_group_ids})
        return {r[0]: r[1].date() for r in rows}

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[SequencingGroupInternal]]]:
        """Get map of samples by analysis_ids"""
        _query = f"""
        SELECT {SequencingGroupTable.common_get_keys_str}, asg.analysis_id
        FROM analysis_sequencing_group asg
        INNER JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE asg.analysis_id IN :aids
        """
        rows = await self.connection.fetch_all(_query, {'aids': analysis_ids})

        mapped_analysis_to_sequencing_group_id: dict[int, list[int]] = defaultdict(list)
        sg_map: dict[int, SequencingGroupInternal] = {}
        projects: set[int] = set()
        for rrow in rows:
            drow = dict(rrow)
            sid = drow['id']
            analysis_id = drow.pop('analysis_id')
            mapped_analysis_to_sequencing_group_id[analysis_id].append(sid)
            projects.add(drow['project'])

            if sid not in sg_map:
                sg_map[sid] = SequencingGroupInternal.from_db(**drow)

        analysis_map: dict[int, list[SequencingGroupInternal]] = {
            analysis_id: [sg_map.get(sgid) for sgid in sgids]
            for analysis_id, sgids in mapped_analysis_to_sequencing_group_id.items()
        }

        return projects, analysis_map

    async def create_sequencing_group(
        self,
        sample_id: int,
        type_: str,
        technology: str,
        platform: str,
        sequence_ids: list[int],
        meta: dict = None,
        author: str = None,
        open_transaction=True,
    ):
        """Create sequence group"""
        _query = """
        INSERT INTO sequencing_group
            (sample_id, type, technology, platform, meta, author, archived)
        VALUES (:sample_id, :type, :technology, :platform, :meta, :author, false)
        RETURNING id;
        """

        _seqg_linker_query = """
        INSERT INTO sequencing_group_assay
            (sequencing_group_id, assay_id, author)
        VALUES
            (:seqgroup, :assayid, :author)
        """

        values = {
            'sample_id': sample_id,
            'type': type_.lower() if type_ else None,
            'technology': technology.lower() if technology else None,
            'platform': platform.lower() if platform else None,
            'meta': to_db_json(meta or {}),
        }
        # check if any values are None and raise an exception if so
        bad_keys = [k for k, v in values.items() if v is None]
        if bad_keys:
            raise ValueError(f'Must provide values for {", ".join(bad_keys)}')

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            id_of_seq_group = await self.connection.fetch_val(
                _query,
                {**values, 'author': author or self.author},
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
        # do this so we can reuse the sequencing_group_ids
        _external_id_query = """
        UPDATE sequencing_group_external_id
        SET nullIfInactive = NULL
        WHERE sequencing_group_id = :sequencing_group_id;
        """
        await self.connection.execute(
            _query, {'sequencing_group_id': sequencing_group_id, 'author': self.author}
        )
        await self.connection.execute(
            _external_id_query,
            {'sequencing_group_id': sequencing_group_id, 'author': self.author},
        )

    async def get_type_numbers_for_project(self, project) -> dict[str, int]:
        """
        Get number of sequencing groups for each type for a project
        Useful for the web layer
        """
        _query = """
SELECT sg.type, COUNT(*) as n
FROM sequencing_group sg
INNER JOIN sample s ON s.id = sg.sample_id
WHERE s.project = :project AND NOT sg.archived
GROUP BY sg.type
        """
        rows = await self.connection.fetch_all(_query, {'project': project})
        return {r['type']: r['n'] for r in rows}
