from datetime import datetime
from itertools import groupby
from typing import List, Optional, Set, Tuple, Dict

from db.python.connect import DbBase, NotFoundError, to_db_json  # , to_db_json
from db.python.tables.project import ProjectId
from models.enums import AnalysisStatus, AnalysisType
from models.models.analysis import Analysis


class AnalysisTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'analysis'

    async def get_project_ids_for_analysis_ids(
        self, analysis_ids: List[int]
    ) -> Set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = (
            'SELECT project FROM analysis WHERE id in :analysis_ids GROUP BY project'
        )
        rows = await self.connection.fetch_all(_query, {'analysis_ids': analysis_ids})
        return set(r['project'] for r in rows)

    async def insert_analysis(
        self,
        analysis_type: AnalysisType,
        status: AnalysisStatus,
        sample_ids: List[int],
        meta: Dict[str, any] = None,
        output: str = None,
        active: bool = True,
        author: str = None,
        project: ProjectId = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        async with self.connection.transaction():
            kv_pairs = [
                ('type', analysis_type.value),
                ('status', status.value),
                ('meta', to_db_json(meta or {})),
                ('output', output),
                ('author', author or self.author),
                ('project', project or self.project),
                ('active', active if active is not None else True),
            ]

            if author is not None:
                kv_pairs.append(('on_behalf_of', self.author))

            if status == AnalysisStatus.COMPLETED:
                kv_pairs.append(('timestamp_completed', datetime.utcnow()))

            kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
            keys = [k for k, _ in kv_pairs]
            cs_keys = ', '.join(keys)
            cs_id_keys = ', '.join(f':{k}' for k in keys)
            _query = f"""\
INSERT INTO analysis
    ({cs_keys})
VALUES ({cs_id_keys}) RETURNING id;"""

            id_of_new_analysis = await self.connection.fetch_val(
                _query,
                dict(kv_pairs),
            )

            await self.add_samples_to_analysis(id_of_new_analysis, sample_ids)

        return id_of_new_analysis

    async def add_samples_to_analysis(self, analysis_id: int, sample_ids: List[int]):
        """Add samples to an analysis (through the linked table)"""
        _query = (
            'INSERT INTO analysis_sample (analysis_id, sample_id) VALUES (:aid, :sid)'
        )

        values = map(lambda sid: {'aid': analysis_id, 'sid': sid}, sample_ids)
        await self.connection.execute_many(_query, list(values))

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus,
        meta: Dict[str, any] = None,
        active: bool = None,
        output: Optional[str] = None,
        author: Optional[str] = None,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """

        fields = {
            'author': self.author or author,
            'on_behalf_of': self.author,
            'analysis_id': analysis_id,
        }
        setters = ['author = :author', 'on_behalf_of = :on_behalf_of']
        if status:
            setters.append('status = :status')
            fields['status'] = status.value

        if active is not None:
            setters.append('active = :active')
            fields['active'] = active

        if status == AnalysisStatus.COMPLETED:
            fields['timestamp_completed'] = datetime.utcnow()
            setters.append('timestamp_completed = :timestamp_completed')

        if output:
            fields['output'] = output
            setters.append('output = :output')

        if meta is not None and len(meta) > 0:
            fields['meta'] = to_db_json(meta)
            setters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        fields_str = ', '.join(setters)
        _query = f'UPDATE analysis SET {fields_str} WHERE id = :analysis_id'

        await self.connection.execute(_query, fields)

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: AnalysisType,
        meta: Dict[str, any] = None,
    ):
        """Find the most recent completed analysis for some analysis type"""

        values = {'project': project, 'type': analysis_type.value}

        meta_str = ''
        if meta:
            for k, v in meta.items():
                k_replacer = f'meta_{k}'
                meta_str += f" AND json_extract(meta, '$.{k}') = :{k_replacer}"
                if v is None:
                    # mariadb does a bad cast for NULL
                    v = 'null'
                values[k_replacer] = v

        _query = f"""
SELECT a.id as id, a.type as type, a.status as status,
        a.output as output, a_s.sample_id as sample_id,
        a.project as project, a.timestamp_completed as timestamp_completed,
        a.meta as meta
FROM analysis_sample a_s
INNER JOIN analysis a ON a_s.analysis_id = a.id
WHERE a.id = (
    SELECT id FROM analysis
    WHERE active AND type = :type AND project = :project AND status = 'completed' AND timestamp_completed IS NOT NULL{meta_str}
    ORDER BY timestamp_completed DESC
    LIMIT 1
)
"""
        rows = await self.connection.fetch_all(_query, values)
        if len(rows) == 0:
            raise NotFoundError(
                f"Couldn't find any analysis with type {analysis_type.value}"
            )
        a = Analysis.from_db(**dict(rows[0]))
        # .from_db maps 'sample_id' -> sample_ids
        for row in rows[1:]:
            a.sample_ids.append(row['sample_id'])

        return a

    async def get_all_sample_ids_without_analysis_type(
        self, analysis_type: AnalysisType, project: ProjectId
    ):
        """
        Find all the samples in the sample_id list that a
        """
        _query = """
SELECT s.id FROM sample s
WHERE s.project = :project AND
      id NOT IN (
          SELECT a_s.sample_id FROM analysis_sample a_s
          LEFT JOIN analysis a ON a_s.analysis_id = a.id
          WHERE a.type = :analysis_type AND a.active
      )
;"""

        rows = await self.connection.fetch_all(
            _query,
            {'analysis_type': analysis_type.value, 'project': project or self.project},
        )
        return [row[0] for row in rows]

    async def get_incomplete_analyses(self, project: ProjectId) -> List[Analysis]:
        """
        Gets details of analysis with status queued or in-progress
        """
        _query = f"""
SELECT a.id as id, a.type as type, a.status as status,
        a.output as output, a_s.sample_id as sample_id,
        a.project as project, a.meta as meta
FROM analysis_sample a_s
INNER JOIN analysis a ON a_s.analysis_id = a.id
WHERE a.project = :project AND a.active AND (a.status='queued' OR a.status='in-progress')
"""
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        analysis_by_id = {}
        for row in rows:
            aid = row['id']
            if aid not in analysis_by_id:
                analysis_by_id[aid] = Analysis.from_db(**dict(row))
            else:
                analysis_by_id[aid].sample_ids.append(row['sample_id'])

        return list(analysis_by_id.values())

    async def get_latest_complete_analysis_for_samples_by_type(
        self, analysis_type: AnalysisType, sample_ids: List[int]
    ) -> List[Analysis]:
        """Get the latest complete analysis for samples (one per sample)"""
        _query = """
SELECT a.id AS id, a.type as type, a.status as status, a.output as output,
a.project as project, a_s.sample_id as sample_id, a.timestamp_completed as timestamp_completed,
a.meta as meta
FROM analysis a
LEFT JOIN analysis_sample a_s ON a_s.analysis_id = a.id
WHERE
    a.active AND
    a.type = :type AND
    a.timestamp_completed IS NOT NULL AND
    a_s.sample_id in :sample_ids
ORDER BY a.timestamp_completed DESC
        """
        expected_type = (AnalysisType.GVCF, AnalysisType.CRAM, AnalysisType.QC)
        if analysis_type not in expected_type:
            expected_types_str = ', '.join(a.value for a in expected_type)
            raise ValueError(
                f'Received analysis type "{analysis_type.value}", received {expected_types_str}'
            )

        values = {'sample_ids': sample_ids, 'type': analysis_type.value}
        rows = await self.connection.fetch_all(_query, values)
        seen_sample_ids = set()
        analyses: List[Analysis] = []
        for row in rows:
            if row['sample_id'] in seen_sample_ids:
                continue
            seen_sample_ids.add(row['sample_id'])
            analyses.append(Analysis.from_db(**row))

        # reverse after timestamp_completed
        return analyses[::-1]

    async def get_analysis_by_id(self, analysis_id: int) -> Tuple[ProjectId, Analysis]:
        """Get analysis object by analysis_id"""
        _query = """
SELECT a.id as id, a.type as type, a.status as status,
a.output as output, a.project as project, a_s.sample_id as sample_id,
a.timestamp_completed as timestamp_completed, a.meta as meta
FROM analysis a
LEFT JOIN analysis_sample a_s ON a_s.analysis_id = a.id
WHERE a.id = :analysis_id
"""
        rows = await self.connection.fetch_all(_query, {'analysis_id': analysis_id})
        if len(rows) == 0:
            raise NotFoundError(f"Couldn't find analysis with id = {analysis_id}")

        project = rows[0]['project']

        a = Analysis.from_db(**dict(rows[0]))
        for row in rows[1:]:
            a.sample_ids.append(row['sample_id'])

        return project, a

    async def get_sample_cram_path_map_for_seqr(
        self, project: ProjectId
    ) -> List[List[str]]:
        """Get (ext_sample_id, cram_path, internal_id) map"""
        _query = """
SELECT s.external_id, a.output, s.id
FROM analysis a
INNER JOIN analysis_sample a_s ON a_s.analysis_id = a.id
INNER JOIN sample s ON a_s.sample_id = s.id
WHERE
    a.active AND
    a.type = 'cram' AND
    a.status = 'completed' AND
    s.project = :project
ORDER BY a.timestamp_completed DESC
"""

        rows = await self.connection.fetch_all(_query, {'project': project})
        # 1 per analysis
        return [
            list(list(g_rows)[0]) for _, g_rows in groupby(rows, lambda seq: seq['id'])
        ]

    async def get_analysis_runner_log(
        self, project_ids: List[int] = None
    ) -> List[Analysis]:
        """
        Get log for the analysis-runner, useful for checking this history of analysis
        """
        values = {}
        wheres = [
            "status = 'unknown'",
            "json_extract(meta, '$.source') = 'analysis-runner'",
            'active',
        ]
        if project_ids:
            wheres.append('project in :project_ids')
            values['project_ids'] = project_ids

        wheres_str = ' AND '.join(wheres)
        _query = f'SELECT * FROM analysis WHERE {wheres_str}'
        rows = await self.connection.fetch_all(_query, values)
        return [Analysis.from_db(**dict(r)) for r in rows]
