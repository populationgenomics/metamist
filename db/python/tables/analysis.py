from typing import List

from datetime import datetime

# from models.models.analysis import Analysis
from models.enums import AnalysisStatus, AnalysisType

from db.python.connect import DbBase  # , to_db_json


class AnalysisTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'analysis'

    async def insert_analysis(
        self,
        analysis_type: AnalysisType,
        status: AnalysisStatus,
        sample_ids: List[int],
        output=None,
        author=None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        async with self.connection.transaction():

            kv_pairs = [
                ('type', analysis_type.value),
                ('status', status.value),
                ('output', output),
                ('author', author or self.author),
            ]

            if status == AnalysisStatus.COMPLETED:
                kv_pairs.append(('timestamp_completed', datetime.utcnow()))

            kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
            keys = [k for k, _ in kv_pairs]
            cs_keys = ', '.join(keys)
            cs_id_keys = ', '.join(f':{k}' for k in keys)
            _query = f"""\
INSERT INTO analysis
    ({cs_keys})
VALUES ({cs_id_keys});"""

            await self.connection.execute(
                _query,
                dict(kv_pairs),
            )
            id_of_new_analysis = (
                await self.connection.fetch_one('SELECT LAST_INSERT_ID();')
            )[0]

            await self.add_samples_to_analysis(id_of_new_analysis, sample_ids)

        return id_of_new_analysis

    async def add_samples_to_analysis(self, analysis_id: int, sample_ids: List[int]):
        """Add samples to an analysis (through the linked table)"""
        _query = (
            'INSERT INTO analysis_sample (analysis_id, sample_id) VALUES (:aid, :sid)'
        )

        values = map(lambda sid: {'aid': analysis_id, 'sid': sid}, sample_ids)
        await self.connection.execute_many(_query, values)

    async def update_analysis(
        self, analysis_id: int, status: AnalysisStatus, output: str, author: str = None
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """
        fields = {'status': status.value, 'author': self.author or author}
        if status == AnalysisStatus.COMPLETED:
            fields['timestamp_completed'] = datetime.utcnow()
        if output:
            fields['output'] = output

        fields_str = ', '.join(f'{key} = :{key}' for key in fields)

        _query = f'UPDATE analysis SET {fields_str} WHERE id = :analysis_id'

        await self.connection.execute(_query, {**fields, 'analysis_id': analysis_id})

    async def get_all_sample_ids_without_analysis_type(
        self, analysis_type: AnalysisType
    ):
        """
        Find all the samples in the sample_id list that a
        """
        _query = """
SELECT id FROM sample WHERE id NOT IN (
    SELECT a_s.sample_id FROM analysis_sample a_s
    LEFT JOIN analysis a ON a_s.analysis_id = a.id
    WHERE a.type = 'gvcf'
);"""

        rows = await self.connection.fetch_all(
            _query, {'analysis_type': analysis_type.value}
        )
        return [row[0] for row in rows]

    async def get_all_new_gvcfs_since_last_successful_joint_call(self) -> List[int]:
        """
        Get list of analysis ids for new GVCFs since the last successful joint-calling
        """

        _query = """
SELECT a.id
FROM analysis
WHERE type='gvcf'
AND a.timestamp_completed > (
    SELECT timestamp_completed FROM analysis
    WHERE type = 'joint-calling'
    ORDER BY timestamp_completed DESC
    LIMIT 1
)"""
        ids = [r[0] for r in await self.connection.execute(_query)]

        # analysis IDs
        return ids
