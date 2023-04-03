from typing import Any

from pydantic import BaseModel

from api.utils import group_by
from db.python.connect import NotFoundError
from db.python.layers.base import BaseLayer, Connection
from db.python.layers.sequence_group import SequencingGroupUpsert, SequenceGroupLayer
from db.python.tables.project import ProjectId, ProjectPermissionsTable
from db.python.tables.sample import SampleTable
from models.enums import SampleType
from models.models.sample import (
    Sample,
    sample_id_format_list,
)


class SampleUpsert(BaseModel):
    """Update model for Sample"""

    id: str | int | None
    external_id: str | None
    type: SampleType | None = None
    meta: dict = {}
    participant_id: int | None = None
    active: bool | None = None
    sequencing_groups: list[SequencingGroupUpsert] = []


class SamplesUpsertBody(BaseModel):
    """Upsert model for batch Samples"""

    samples: list[SampleUpsert]


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.st: SampleTable = SampleTable(connection)
        self.pt = ProjectPermissionsTable(connection.connection)
        self.connection = connection

    # GETS
    async def get_by_id(self, sample_id: int, check_project_id=True) -> Sample:
        """Get sample by internal sample id"""
        project, sample = await self.st.get_single_by_id(sample_id)
        if check_project_id:
            await self.pt.check_access_to_project_ids(
                self.connection.author, [project], readonly=True
            )

        return sample

    async def get_samples_by_analysis_ids(
        self, analysis_ids: list[int], check_project_ids: bool = True
    ) -> dict[int, list[Sample]]:
        """
        Get samples by analysis_ids (map).
        Note: It's not guaranteed that analysis has samples, so some
        analysis_ids may NOT be present in the final map
        """
        projects, analysis_sample_map = await self.st.get_samples_by_analysis_ids(
            analysis_ids
        )

        if not analysis_sample_map:
            return {}

        if check_project_ids:
            await self.pt.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return analysis_sample_map

    async def get_samples_by_participants(
        self, participant_ids: list[int], check_project_ids: bool = True
    ) -> dict[int, list[Sample]]:
        """Get map of samples by participants"""

        projects, samples = await self.st.get_samples_for_participants(participant_ids)

        if not samples:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        grouped_samples = group_by(samples, lambda s: s.participant_id)

        return grouped_samples

    async def get_project_ids_for_sample_ids(self, sample_ids: list[int]) -> set[int]:
        """Return the projects associated with the sample ids"""
        return await self.st.get_project_ids_for_sample_ids(sample_ids)

    async def get_sample_by_id(self, sample_id: int, check_project_id=True) -> Sample:
        """Get sample by ID"""
        project, sample = await self.st.get_single_by_id(sample_id)
        if check_project_id:
            await self.pt.check_access_to_project_ids(
                self.author, [project], readonly=True
            )

        return sample

    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> Sample:
        """Get a Sample by its external_id"""
        return await self.st.get_single_by_external_id(
            external_id, project, check_active=check_active
        )

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: list[str],
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
        self, sample_ids: list[int], check_project_ids=True, allow_missing=False
    ) -> dict[int, str]:
        """Get map of external sample id to internal id"""

        sample_ids_set = set(sample_ids)

        if not sample_ids_set:
            return {}

        # could make a preflight request to self.st.get_project_ids_for_sample_ids
        # but this can do it one request, only one request to the database
        projects, sample_id_map = await self.st.get_sample_id_map_by_internal_ids(
            list(sample_ids_set)
        )

        if not allow_missing and len(sample_id_map) != len(sample_ids):
            # we have samples missing from the map, so we'll 404 the whole thing
            missing_sample_ids = sample_ids_set - set(sample_id_map.keys())
            raise NotFoundError(
                f"Couldn't find samples with IDS: {', '.join(sample_id_format_list(list(missing_sample_ids)))}"
            )

        if not sample_id_map:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> dict[int, str]:
        """Get sample id map for all samples in project"""
        return await self.st.get_all_sample_id_map_by_internal_ids(project=project)

    async def get_samples_by(
        self,
        sample_ids: list[int] = None,
        meta: dict[str, Any] = None,
        participant_ids: list[int] = None,
        project_ids=None,
        active=True,
        check_project_ids=True,
    ) -> list[Sample]:
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
    ) -> dict[int, str]:
        """Get samples with missing participants in project"""
        m = await self.st.get_sample_with_missing_participants_by_internal_id(project)
        return dict(m)

    async def get_samples_create_date(self, sample_ids: list[int]):
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        pjcts = await self.st.get_project_ids_for_sample_ids(sample_ids)
        await self.pt.check_access_to_project_ids(self.author, pjcts, readonly=True)
        return await self.st.get_samples_create_date(sample_ids)

    # CREATE / UPDATES
    async def insert_sample(
        self,
        external_id,
        sample_type: SampleType,
        active=True,
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
        meta: dict = None,
        participant_id: int = None,
        external_id: str = None,
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
            external_id=external_id,
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
            id_=sample.id,  # type: ignore
            meta=sample.meta,
            participant_id=sample.participant_id,
            type_=sample.type,
            active=sample.active,
        )
        return int(internal_id)

    async def update_many_participant_ids(
        self, ids: list[int], participant_ids: list[int], check_sample_ids=True
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
    ) -> list[Sample]:
        """Get the full history of a sample"""
        rows = await self.st.get_history_of_sample(id_)

        if check_sample_ids:
            project_ids = set(r.project for r in rows)
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return rows

    async def batch_upsert_samples(self, samples: list[SampleUpsert]):
        """Batch upsert a list of samples with sequences"""
        seqglayer: SequenceGroupLayer = SequenceGroupLayer(self.connection)

        # Create or update samples
        sids = [await self.upsert_sample(s) for s in samples]

        # Upsert all sequence groups with paired sids, this will
        # also upsert sequences
        sequence_groups = zip(sids, [x.sequencing_groups for x in samples])
        seqs = [
            await seqglayer.upsert_sequence_groups(sid, seqg)
            for sid, seqg in sequence_groups
        ]

        # Format and return response
        return dict(zip(sids, seqs))
