from typing import List, Optional, Dict

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.analysis import AnalysisTable

from models.enums import AnalysisStatus, AnalysisType
from models.models.analysis import Analysis
from models.models.sample import sample_id_format


class AnalysisLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.at = AnalysisTable(connection)

    # GETS

    async def get_analysis_by_id(self, analysis_id: int, check_project_id=True):
        """Get analysis by ID"""
        project, analysis = await self.at.get_analysis_by_id(analysis_id)
        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return analysis

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: AnalysisType,
        meta: Dict[str, any] = None,
    ) -> Analysis:
        """Get SINGLE latest complete analysis for some analysis type"""
        return await self.at.get_latest_complete_analysis_for_type(
            project=project, analysis_type=analysis_type, meta=meta
        )

    async def get_latest_complete_analysis_for_samples_and_type(
        self,
        analysis_type: AnalysisType,
        sample_ids: List[int],
        allow_missing=True,
        check_project_ids=True,
    ):
        """Get the latest complete analysis for samples (one per sample)"""

        if check_project_ids:
            project_ids = await SampleTable(
                self.connection
            ).get_project_ids_for_sample_ids(sample_ids)
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        analyses = await self.at.get_latest_complete_analysis_for_samples_by_type(
            analysis_type=analysis_type, sample_ids=sample_ids
        )

        if not allow_missing and len(sample_ids) != len(analyses):
            seen_sample_ids = set(s for a in analyses for s in a.sample_ids)
            missing_sample_ids = set(sample_ids) - seen_sample_ids
            sample_ids_str = ', '.join(sample_id_format(list(missing_sample_ids)))

            raise Exception(
                f'Missing gvcfs for the following sample IDs: {sample_ids_str}'
            )

        return analyses

    async def get_all_sample_ids_without_analysis_type(
        self, project: ProjectId, analysis_type: AnalysisType
    ):
        """
        Find all the samples in the sample_id list that a
        """
        return await self.at.get_all_sample_ids_without_analysis_type(
            analysis_type=analysis_type, project=project
        )

    async def get_incomplete_analyses(self, project: ProjectId) -> List[Analysis]:
        """
        Gets details of analysis with status queued or in-progress
        """
        return await self.at.get_incomplete_analyses(project=project)

    async def get_sample_cram_path_map_for_seqr(
        self, project: ProjectId
    ) -> List[List[str]]:
        """Get (ext_sample_id, cram_path, internal_id) map"""
        return await self.at.get_sample_cram_path_map_for_seqr(project=project)

    # CREATE / UPDATE

    async def insert_analysis(
        self,
        analysis_type: AnalysisType,
        status: AnalysisStatus,
        sample_ids: List[int],
        meta: Dict[str, any],
        output: str = None,
        active: bool = True,
        author: str = None,
        project: ProjectId = None,
    ) -> int:
        """Create a new analysis"""
        return await self.at.insert_analysis(
            analysis_type=analysis_type,
            status=status,
            sample_ids=sample_ids,
            meta=meta,
            output=output,
            active=active,
            author=author,
            project=project,
        )

    async def add_samples_to_analysis(
        self, analysis_id: int, sample_ids: List[int], check_project_id=True
    ):
        """Add samples to an analysis (through the linked table)"""
        if check_project_id:
            project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.at.add_samples_to_analysis(
            analysis_id=analysis_id, sample_ids=sample_ids
        )

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus,
        meta: Dict[str, any] = None,
        output: Optional[str] = None,
        author: Optional[str] = None,
        check_project_id=True,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """
        if check_project_id:
            project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        await self.at.update_analysis(
            analysis_id=analysis_id,
            status=status,
            meta=meta,
            output=output,
            author=author,
        )
