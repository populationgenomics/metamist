import datetime
from typing import Any, NamedTuple

from api.utils import group_by
from db.python.filters import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer, Connection
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sample import SampleFilter, SampleTable
from db.python.utils import NoOpAenter, NotFoundError
from models.models.project import ProjectId
from models.models.sample import SampleInternal, SampleUpsertInternal
from models.utils.sample_id_format import sample_id_format_list


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.st: SampleTable = SampleTable(connection)
        self.pt = ProjectPermissionsTable(connection)
        self.connection = connection

    # GETS
    async def get_by_id(self, sample_id: int, check_project_id=True) -> SampleInternal:
        """Get sample by internal sample id"""
        project, sample = await self.st.get_sample_by_id(sample_id)
        if check_project_id:
            await self.pt.check_access_to_project_ids(
                self.connection.author, [project], readonly=True
            )

        return sample

    async def query(
        self, filter_: SampleFilter, check_project_ids: bool = True
    ) -> list[SampleInternal]:
        """Query samples"""
        projects, samples = await self.st.query(filter_)
        if not samples:
            return samples

        if check_project_ids:
            await self.pt.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return samples

    async def get_samples_by_participants(
        self, participant_ids: list[int], check_project_ids: bool = True
    ) -> dict[int, list[SampleInternal]]:
        """Get map of samples by participants"""

        projects, samples = await self.st.query(
            SampleFilter(
                participant_id=GenericFilter(in_=participant_ids),
            ),
        )

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

    async def get_sample_by_id(
        self, sample_id: int, check_project_id=True
    ) -> SampleInternal:
        """Get sample by ID"""
        project, sample = await self.st.get_sample_by_id(sample_id)
        if check_project_id:
            await self.pt.check_access_to_project_ids(
                self.author, [project], readonly=True
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

    async def get_internal_to_external_sample_id_map(
        self, sample_ids: list[int], check_project_ids=True, allow_missing=False
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
        sample_ids: list[int] | None = None,
        meta: dict[str, Any] | None = None,
        participant_ids: list[int] | None = None,
        project_ids=None,
        active=True,
        check_project_ids=True,
    ) -> list[SampleInternal]:
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

        if not project_ids and check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, _returned_project_ids, readonly=True
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
        await self.pt.check_access_to_project_ids(self.author, pjcts, readonly=True)
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
        # safely ignore nested samples here
        async with with_function():
            if not sample.id:
                sample.id = await self.st.insert_sample(
                    external_ids=sample.external_ids,
                    sample_type=sample.type,
                    active=True,
                    meta=sample.meta,
                    participant_id=sample.participant_id,
                    project=project,
                    sample_parent_id=sample_parent_id,
                    sample_root_id=sample_root_id,
                )
            else:
                # Otherwise update
                await self.st.update_sample(
                    id_=sample.id,  # type: ignore
                    external_ids=sample.external_ids,
                    meta=sample.meta,
                    participant_id=sample.participant_id,
                    type_=sample.type,
                    active=sample.active,
                    # think about these more carefully
                    # sample_parent_id=sample_parent_id,
                    # sample_root_id=sample_root_id,
                )

            if sample.sequencing_groups:
                sglayer = SequencingGroupLayer(self.connection)
                for seqg in sample.sequencing_groups:
                    seqg.sample_id = sample.id

                if process_sequencing_groups:
                    await sglayer.upsert_sequencing_groups(sample.sequencing_groups)

            if sample.non_sequencing_assays:
                alayer = AssayLayer(self.connection)
                for assay in sample.non_sequencing_assays:
                    assay.sample_id = sample.id
                if process_assays:
                    await alayer.upsert_assays(
                        sample.non_sequencing_assays, open_transaction=False
                    )

        return sample

    async def upsert_samples(
        self,
        samples: list[SampleUpsertInternal],
        open_transaction: bool = True,
        project: ProjectId = None,
        check_project_id=True,
    ) -> list[SampleUpsertInternal]:
        """Batch upsert a list of samples with sequences"""
        seqglayer: SequencingGroupLayer = SequencingGroupLayer(self.connection)

        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )

        if check_project_id:
            sids = [s.id for s in samples if s.id]
            if sids:
                pjcts = await self.st.get_project_ids_for_sample_ids(sids)
                await self.ptable.check_access_to_project_ids(
                    self.author, pjcts, readonly=False
                )

        async with with_function():
            # Create or update samples
            for r in self.unwrap_nested_samples(samples):
                await self.upsert_sample(
                    r.sample,
                    sample_root_id=r.root.id if r.root else None,
                    sample_parent_id=r.parent.id if r.parent else None,
                    project=project,
                    process_sequencing_groups=False,
                    process_assays=False,
                    open_transaction=False,
                )

            # Upsert all sequencing_groups (in turn relevant assays)
            sequencing_groups = [
                seqg for sample in samples for seqg in (sample.sequencing_groups or [])
            ]
            if sequencing_groups:
                await seqglayer.upsert_sequencing_groups(sequencing_groups)

            assays = [
                assay
                for sample in samples
                for assay in (sample.non_sequencing_assays or [])
            ]
            if assays:
                alayer = AssayLayer(self.connection)
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

        NB: Opting for a non-recursive approach here, as I'm a bit afraid of recursive
            Python after a weird Hail Batch thing, and sounded like a nightmare to debug
        """

        retval: list[SampleLayer.UnwrappedSample] = []

        seen_samples = {id(s) for s in samples}

        rounds: list[
            list[
                tuple[
                    SampleUpsertInternal | None,
                    SampleUpsertInternal | None,
                    list[SampleUpsertInternal],
                ]
            ]
        ] = [[(None, None, samples)]]

        round_idx = 0
        while round_idx < len(rounds):
            prev_round = rounds[round_idx]
            new_round = []
            round_idx += 1
            for root, parent, nested_samples in prev_round:

                for sample in nested_samples:
                    retval.append(
                        SampleLayer.UnwrappedSample(
                            root=root, parent=parent, sample=sample
                        )
                    )
                    if not sample.nested_samples:
                        continue

                    # do the seen check
                    for s in sample.nested_samples:
                        if id(s) in seen_samples:
                            raise ValueError(
                                f'Sample sample was seen in the list ({s})'
                            )
                        seen_samples.add(id(s))
                    new_round.append((root or sample, sample, sample.nested_samples))

            if new_round:
                if round_idx >= max_depth:
                    parents = ', '.join(str(s) for _, s, _ in new_round)
                    raise SampleLayer.SampleUnwrapMaxDepthError(
                        f'Exceeded max depth of {max_depth} for nested samples. '
                        f'Parents: {parents}'
                    )
                rounds.append(new_round)

        return retval

    async def merge_samples(
        self,
        id_keep: int,
        id_merge: int,
        check_project_id=True,
    ):
        """Merge two samples into one another"""
        if check_project_id:
            projects = await self.st.get_project_ids_for_sample_ids([id_keep, id_merge])
            await self.ptable.check_access_to_project_ids(
                user=self.author, project_ids=projects, readonly=False
            )

        return await self.st.merge_samples(
            id_keep=id_keep,
            id_merge=id_merge,
        )

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
    ) -> list[SampleInternal]:
        """Get the full history of a sample"""
        rows = await self.st.get_history_of_sample(id_)

        if check_sample_ids:
            project_ids = set(r.project for r in rows)
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return rows
