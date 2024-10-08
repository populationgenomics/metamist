import datetime
from typing import Any, NamedTuple

from api.utils import group_by
from db.python.filters import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer, Connection
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.sample import SampleFilter, SampleTable
from db.python.utils import NoOpAenter, NotFoundError
from models.models.assay import AssayUpsertInternal
from models.models.project import (
    FullWriteAccessRoles,
    ProjectId,
    ReadAccessRoles,
)
from models.models.sample import SampleInternal, SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sample_id_format import sample_id_format_list


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.st = SampleTable(connection)
        self.connection = connection

    # GETS
    async def get_by_id(self, sample_id: int) -> SampleInternal:
        """Get sample by internal sample id"""
        project, sample = await self.st.get_sample_by_id(sample_id)

        self.connection.check_access_to_projects_for_ids(
            [project], allowed_roles=ReadAccessRoles
        )

        return sample

    async def query(self, filter_: SampleFilter) -> list[SampleInternal]:
        """Query samples"""
        projects, samples = await self.st.query(filter_)
        if not samples:
            return []

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        return samples

    async def get_samples_by_participants(
        self, participant_ids: list[int]
    ) -> dict[int, list[SampleInternal]]:
        """Get map of samples by participants"""

        projects, samples = await self.st.query(
            SampleFilter(
                participant_id=GenericFilter(in_=participant_ids),
            ),
        )

        if not samples:
            return {}

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        grouped_samples = group_by(samples, lambda s: s.participant_id)

        return grouped_samples

    async def get_project_ids_for_sample_ids(self, sample_ids: list[int]) -> set[int]:
        """Return the projects associated with the sample ids"""
        return await self.st.get_project_ids_for_sample_ids(sample_ids)

    async def get_sample_by_id(self, sample_id: int) -> SampleInternal:
        """Get sample by ID"""
        project, sample = await self.st.get_sample_by_id(sample_id)
        self.connection.check_access_to_projects_for_ids(
            [project], allowed_roles=ReadAccessRoles
        )

        return sample

    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> SampleInternal:
        """Get a Sample by (any of) its external_id(s)"""
        return await self.st.get_single_by_external_id(
            external_id, project, check_active=check_active
        )

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: list[str],
        project: ProjectId = None,
        allow_missing=False,
    ) -> dict[str, int]:
        """Get map of samples {(any) external_id: internal_id}"""
        external_ids_set = set(external_ids)
        _project = project or self.connection.project_id
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

    async def get_internal_to_external_sample_id_map(
        self, sample_ids: list[int], allow_missing=False
    ) -> dict[int, str]:
        """Get map of internal sample id to external id"""

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

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        return sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> dict[int, str]:
        """Get sample id map for all samples in project"""
        return await self.st.get_all_sample_id_map_by_internal_ids(project=project)

    async def get_samples_by(
        self,
        sample_ids: list[int] | None = None,
        meta: dict[str, Any] | None = None,
        participant_ids: list[int] | None = None,
        project_ids=None,
        active=True,
    ) -> list[SampleInternal]:
        """Get samples by some criteria"""
        if not sample_ids and not project_ids:
            raise ValueError('Must specify one of "project_ids" or "sample_ids"')

        if sample_ids:
            # project_ids were already checked when transformed to ints,
            # so no else required
            pjcts = await self.st.get_project_ids_for_sample_ids(sample_ids)
            self.connection.check_access_to_projects_for_ids(
                pjcts, allowed_roles=ReadAccessRoles
            )

        _returned_project_ids, samples = await self.st.query(
            SampleFilter(
                id=GenericFilter(in_=sample_ids),
                meta=meta,
                participant_id=GenericFilter(in_=participant_ids),
                project=GenericFilter(in_=project_ids),
                active=GenericFilter(eq=active) if active is not None else None,
            )
        )
        if not samples:
            return []

        self.connection.check_access_to_projects_for_ids(
            _returned_project_ids, allowed_roles=ReadAccessRoles
        )

        return samples

    async def get_sample_with_missing_participants_by_internal_id(
        self, project: ProjectId
    ) -> list[SampleInternal]:
        """Get samples with missing participants in project"""
        m = await self.st.get_samples_with_missing_participants_by_internal_id(project)
        return m

    async def get_samples_create_date(
        self, sample_ids: list[int]
    ) -> dict[int, datetime.date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        pjcts = await self.st.get_project_ids_for_sample_ids(sample_ids)
        self.connection.check_access_to_projects_for_ids(
            pjcts, allowed_roles=ReadAccessRoles
        )
        return await self.st.get_samples_create_date(sample_ids)

    # CREATE / UPDATES
    async def upsert_sample(
        self,
        sample: SampleUpsertInternal,
        sample_parent_id: int | None = None,
        sample_root_id: int | None = None,
        project: ProjectId | None = None,
        process_sequencing_groups: bool = True,
        process_assays: bool = True,
        open_transaction: bool = True,
    ) -> SampleUpsertInternal:
        """Upsert a sample"""
        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )
        async with with_function():
            alayer = AssayLayer(self.connection)

            for r in self.unwrap_nested_samples([sample]):
                s = r.sample
                sample_parent_id = getattr(r.parent, 'id', sample_parent_id)
                sample_root_id = getattr(r.root, 'id', sample_root_id)

                if not s.id:
                    s.id = await self.st.insert_sample(
                        external_ids=s.external_ids,
                        sample_type=s.type,
                        active=True,
                        meta=s.meta,
                        participant_id=s.participant_id,
                        project=project,
                        sample_parent_id=sample_parent_id,
                        sample_root_id=sample_root_id,
                    )
                else:
                    await self.st.update_sample(
                        id_=s.id,
                        external_ids=s.external_ids,
                        meta=s.meta,
                        participant_id=s.participant_id,
                        type_=s.type,
                        active=s.active,
                        sample_parent_id=sample_parent_id,
                        sample_root_id=sample_root_id,
                    )

                if process_sequencing_groups and sample.sequencing_groups:
                    sglayer = SequencingGroupLayer(self.connection)
                    self.set_sample_ids(s.id, sample.sequencing_groups)
                    await sglayer.upsert_sequencing_groups(sample.sequencing_groups)

                if process_assays and sample.non_sequencing_assays:
                    self.set_sample_ids(s.id, sample.non_sequencing_assays)
                    await alayer.upsert_assays(
                        sample.non_sequencing_assays, open_transaction=False
                    )

        return sample

    def set_sample_ids(
        self,
        sample_id: int,
        internal_objs: list[SequencingGroupUpsertInternal] | list[AssayUpsertInternal],
    ):
        """
        Set the sample id for upserting sequencing group or assay
        These internal upsert models will be children of a SampleUpsertInternal
        but may not have the correct sample_id set. This is to ensure that they do.
        """
        for obj in internal_objs:
            assert hasattr(obj, 'sample_id')
            assert obj.sample_id is None or obj.sample_id == sample_id
            obj.sample_id = sample_id

    async def upsert_samples(
        self,
        samples: list[SampleUpsertInternal],
        open_transaction: bool = True,
        project: ProjectId = None,
    ) -> list[SampleUpsertInternal]:
        """Batch upsert a list of samples with sequences"""
        alayer = AssayLayer(self.connection)
        seqglayer: SequencingGroupLayer = SequencingGroupLayer(self.connection)

        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )

        sids = [s.id for s in samples if s.id]
        if sids:
            pjcts = await self.st.get_project_ids_for_sample_ids(sids)
            self.connection.check_access_to_projects_for_ids(
                pjcts, allowed_roles=ReadAccessRoles
            )

        async with with_function():
            # Create or update samples
            sequencing_groups: list[SequencingGroupUpsertInternal] = []
            assays: list[AssayUpsertInternal] = []
            for sample in samples:
                await self.upsert_sample(
                    sample,
                    project=project,
                    process_sequencing_groups=False,
                    process_assays=False,
                    open_transaction=False,
                )

                # Collect all sequencing_groups and assays
                for seqg in sample.sequencing_groups or []:
                    seqg.sample_id = sample.id
                    sequencing_groups.append(seqg)

                for assay in sample.non_sequencing_assays or []:
                    assay.sample_id = sample.id
                    assays.append(assay)

            # Upsert all sequencing_groups (in turn relevant assays)
            await seqglayer.upsert_sequencing_groups(sequencing_groups)
            await alayer.upsert_assays(assays, open_transaction=False)

        return samples

    class UnwrappedSample(NamedTuple):
        """
        When we unwrap the nested samples, we store the root and parent to insert later
        """

        root: SampleUpsertInternal | None
        parent: SampleUpsertInternal | None
        sample: SampleUpsertInternal

    class SampleUnwrapMaxDepthError(Exception):
        """Error for when we exceed the user-set max-depth"""

    @staticmethod
    def unwrap_nested_samples(
        samples: list[SampleUpsertInternal], max_depth=10
    ) -> list[UnwrappedSample]:
        """
        We only insert one by one, so we don't need to do anything too crazy, just pull
        out the insert order, keeping reference to the root, and parent.

        Just keep a soft limit on the depth, as we don't want to go too deep.
        """

        retval: list[SampleLayer.UnwrappedSample] = []
        seen_samples = set()
        stack: list[
            tuple[
                SampleUpsertInternal | None,
                SampleUpsertInternal | None,
                SampleUpsertInternal | None,
                int,
            ]
        ] = [(None, None, sample, 0) for sample in samples]

        while stack:
            root, parent, sample, depth = stack.pop(0)
            if depth > max_depth:
                raise SampleLayer.SampleUnwrapMaxDepthError(
                    f'Exceeded max depth of {max_depth} for nested samples. '
                    f'Parents: {parent}'
                )

            if id(sample) in seen_samples:
                raise ValueError(f'Sample sample was seen in the list ({sample})')
            seen_samples.add(id(sample))

            retval.append(
                SampleLayer.UnwrappedSample(root=root, parent=parent, sample=sample)
            )

            if sample.nested_samples:
                for nested_sample in sample.nested_samples:
                    stack.append((root or sample, sample, nested_sample, depth + 1))

        return retval

    async def merge_samples(
        self,
        id_keep: int,
        id_merge: int,
    ):
        """Merge two samples into one another"""
        projects = await self.st.get_project_ids_for_sample_ids([id_keep, id_merge])
        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=FullWriteAccessRoles
        )

        return await self.st.merge_samples(
            id_keep=id_keep,
            id_merge=id_merge,
        )

    async def update_many_participant_ids(
        self, ids: list[int], participant_ids: list[int]
    ) -> bool:
        """
        Update participant IDs for many samples
        Expected len(ids) == len(participant_ids)
        """
        if len(ids) != len(participant_ids):
            raise ValueError(
                f'Number of sampleIDs ({len(ids)}) and ParticipantIds ({len(participant_ids)}) did not match'
            )

        projects = await self.st.get_project_ids_for_sample_ids(ids)
        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=FullWriteAccessRoles
        )

        await self.st.update_many_participant_ids(
            ids=ids, participant_ids=participant_ids
        )
        return True

    async def get_history_of_sample(self, id_: int) -> list[SampleInternal]:
        """Get the full history of a sample"""
        rows = await self.st.get_history_of_sample(id_)

        projects = set(r.project for r in rows)
        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=FullWriteAccessRoles
        )

        return rows
