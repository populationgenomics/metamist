from typing import Dict, List

from db.python.connect import NotFoundError
from db.python.layers.base import BaseLayer, Connection
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from models.enums import SampleType
from models.models.sample import Sample, sample_id_format


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.st: SampleTable = SampleTable(connection)

    # GETS
    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> Sample:
        """Get a Sample by its external_id"""
        return await self.st.get_single_by_external_id(
            external_id, project, check_active=check_active
        )

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: List[str],
        project: ProjectId = None,
        allow_missing=False,
    ):
        """Get map of samples {external_id: internal_id}"""
        external_ids_set = set(external_ids)
        sample_id_map = await self.st.get_sample_id_map_by_external_ids(
            external_ids=list(external_ids_set), project=project
        )

        if allow_missing or len(sample_id_map) == len(external_ids_set):
            return sample_id_map

        # we have samples missing from the map, so we'll 404 the whole thing
        missing_sample_ids = external_ids_set - set(sample_id_map.keys())

        raise NotFoundError(
            f"Couldn't find samples with IDs: {', '.join(missing_sample_ids)}"
        )

    async def get_sample_id_map_by_internal_ids(
        self, sample_ids: List[int], check_project_ids=True
    ) -> Dict[int, str]:
        """Get map of external sample id to internal id"""

        sample_ids_set = set(sample_ids)
        # could make a preflight request to self.st.get_project_ids_for_sample_ids
        # but this can do it one request, only one request to the database
        projects, sample_id_map = await self.st.get_sample_id_map_by_internal_ids(
            list(sample_ids_set)
        )

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        if len(sample_id_map) == len(sample_ids):
            return sample_id_map

        # we have samples missing from the map, so we'll 404 the whole thing
        missing_sample_ids = sample_ids_set - set(sample_id_map.keys())
        raise NotFoundError(
            f"Couldn't find samples with IDS: {', '.join(sample_id_format(list(missing_sample_ids)))}"
        )

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> Dict[int, str]:
        """Get sample id map for all samples in project"""
        return await self.st.get_all_sample_id_map_by_internal_ids(project=project)

    async def get_samples_by(
        self,
        sample_ids: List[int] = None,
        meta: Dict[str, any] = None,
        participant_ids: List[int] = None,
        project_ids=None,
        active=True,
        check_project_ids=True,
    ) -> List[Sample]:
        """Get samples by some criteria"""
        if not sample_ids and not project_ids:
            raise ValueError('Must specify one of "project_ids" or "sample_ids"')
        if sample_ids and check_project_ids:
            # project_ids were already checked when transformed to ints,
            # so no else required

            projects = self.st.get_project_ids_for_sample_ids(sample_ids)
            self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        projects, samples = await self.st.get_samples_by(
            sample_ids=sample_ids,
            meta=meta,
            participant_ids=participant_ids,
            project_ids=project_ids,
            active=active,
        )
        if not samples:
            return []

        return samples

    async def get_sample_with_missing_participants_by_internal_id(
        self, project: ProjectId
    ) -> Dict[int, str]:
        """Get samples with missing participants in project"""
        m = await self.st.get_sample_with_missing_participants_by_internal_id(project)
        return dict(m)

    # CREATE / UPDATES
    async def insert_sample(
        self,
        external_id,
        sample_type: SampleType,
        active,
        meta=None,
        participant_id=None,
        author=None,
        project=None,
        check_project_id=True,
    ) -> int:
        """Insert sample into SM database"""
        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                author or self.author, [project], readonly=False
            )

        return await self.st.insert_sample(
            external_id=external_id,
            sample_type=sample_type,
            active=active,
            meta=meta,
            participant_id=participant_id,
            author=author,
            project=project,
        )

    async def update_sample(
        self,
        id_: int,
        meta: Dict = None,
        participant_id: int = None,
        type_: SampleType = None,
        author: str = None,
        active: bool = None,
        check_project_id=True,
    ):
        """Update existing sample in the SM database"""
        if check_project_id:
            projects = await self.st.get_project_ids_for_sample_ids([id_])
            await self.ptable.check_access_to_project_ids(
                user=author or self.author, project_ids=projects, readonly=False
            )

        return await self.st.update_sample(
            id_=id_,
            meta=meta,
            participant_id=participant_id,
            type_=type_,
            author=author,
            active=active,
        )

    async def update_many_participant_ids(
        self, ids: List[int], participant_ids: List[int], check_sample_ids=True
    ) -> bool:
        """
        Update participant IDs for many samples
        Expected len(ids) == len(participant_ids)
        """
        if len(ids) != len(participant_ids):
            raise ValueError(
                f'Number of sampleIDs ({len(ids)}) and ParticipantIds ({len(participant_ids)}) did not match'
            )
        if check_sample_ids:
            project_ids = await self.st.get_project_ids_for_sample_ids(ids)
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        await self.st.update_many_participant_ids(
            ids=ids, participant_ids=participant_ids
        )
        return True
