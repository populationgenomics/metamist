from typing import Dict, List, Any, Optional, Union

from pydantic import BaseModel

from db.python.connect import NotFoundError
from db.python.layers.base import BaseLayer, Connection
from db.python.layers.sequence import SampleSequenceLayer, SequenceUpsert
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable

from models.enums import SampleType
from models.models.sample import (
    Sample,
    sample_id_format_list,
)


class SampleUpsert(BaseModel):
    """Update model for Sample"""

    external_id: Optional[str]
    type: Optional[SampleType] = None
    meta: Optional[Dict] = {}
    participant_id: Optional[int] = None
    active: Optional[bool] = None


class SampleBatchUpsert(SampleUpsert):
    """Update model for sample with sequences list"""

    id: Optional[Union[str, int]]
    sequences: List[SequenceUpsert]


class SampleBatchUpsertBody(BaseModel):
    """Upsert model for batch Samples"""

    samples: List[SampleBatchUpsert]


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.st: SampleTable = SampleTable(connection)
        self.connection = connection

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
        _project = project or self.connection.project
        assert _project
        sample_id_map = await self.st.get_sample_id_map_by_external_ids(
            external_ids=list(external_ids_set), project=_project
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
            f"Couldn't find samples with IDS: {', '.join(sample_id_format_list(list(missing_sample_ids)))}"
        )

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> Dict[int, str]:
        """Get sample id map for all samples in project"""
        return await self.st.get_all_sample_id_map_by_internal_ids(project=project)

    async def get_samples_by(
        self,
        sample_ids: List[int] = None,
        meta: Dict[str, Any] = None,
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
            pjcts = await self.st.get_project_ids_for_sample_ids(sample_ids)
            await self.ptable.check_access_to_project_ids(
                self.author, pjcts, readonly=True
            )

        _, samples = await self.st.get_samples_by(
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
                author or self.author,
                [project or self.connection.project],
                readonly=False,
            )

        return await self.st.insert_sample(
            external_id=external_id,
            sample_type=sample_type,
            active=active,
            meta=meta,
            participant_id=participant_id,
            author=author or self.author,
            project=project or self.connection.project,
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

    async def merge_samples(
        self,
        id_keep: int,
        id_merge: int,
        author=None,
        check_project_id=True,
    ):
        """Merge two samples into one another"""
        if check_project_id:
            projects = await self.st.get_project_ids_for_sample_ids([id_keep, id_merge])
            await self.ptable.check_access_to_project_ids(
                user=author or self.author, project_ids=projects, readonly=False
            )

        return await self.st.merge_samples(
            id_keep=id_keep,
            id_merge=id_merge,
            author=author,
        )

    async def upsert_sample(self, sample: SampleUpsert):
        """Upsert a sample"""
        if not sample.id:
            internal_id = await self.insert_sample(
                external_id=sample.external_id,
                sample_type=sample.type,
                active=True,
                meta=sample.meta,
                participant_id=sample.participant_id,
                check_project_id=False,
            )
            return int(internal_id)

        # Otherwise update
        internal_id = await self.update_sample(
            id_=sample.id,
            meta=sample.meta,
            participant_id=sample.participant_id,
            type_=sample.type,
            active=sample.active,
        )
        return int(internal_id)

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

    async def get_history_of_sample(
        self, id_: int, check_sample_ids: bool = True
    ) -> List[Sample]:
        """Get the full history of a sample"""
        rows = await self.st.get_history_of_sample(id_)

        if check_sample_ids:
            project_ids = set(r.project for r in rows)
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return rows

    async def batch_upsert_samples(self, samples: SampleBatchUpsertBody):
        """Batch upsert a list of samples with sequences"""
        seqt: SampleSequenceLayer = SampleSequenceLayer(self.connection)

        # Create or update samples
        iids = [await self.upsert_sample(s) for s in samples.samples]

        # Upsert all sequences with paired sids
        sequences = zip(iids, [x.sequences for x in samples.samples])
        seqs = [await seqt.upsert_sequences(iid, seqs) for iid, seqs in sequences]

        # Format and return response
        return dict(zip(iids, seqs))
