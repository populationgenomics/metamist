from datetime import date

from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer
from db.python.tables.assay import AssayFilter, AssayTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from db.python.utils import NoOpAenter, NotFoundError
from models.models.project import FullWriteAccessRoles, ProjectId, ReadAccessRoles
from models.models.sequencing_group import (
    SequencingGroupInternal,
    SequencingGroupInternalId,
    SequencingGroupUpsertInternal,
)
from models.utils.sequencing_group_id_format import sequencing_group_id_format


class SequencingGroupLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqgt: SequencingGroupTable = SequencingGroupTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    async def get_sequencing_group_by_id(
        self, 
        sequencing_group_id: int, 
        active_only=True
    ) -> SequencingGroupInternal:
        """
        Get sequencing group by internal ID
        """
        groups = await self.get_sequencing_groups_by_ids([sequencing_group_id], active_only)

        return groups[0]

    async def get_sequencing_groups_by_ids(
        self, 
        sequencing_group_ids: list[int], 
        active_only=True
    ) -> list[SequencingGroupInternal]:
        """
        Get sequence groups by internal IDs
        """
        if not sequencing_group_ids:
            return []

        projects, groups = await self.seqgt.get_sequencing_groups_by_ids(
            sequencing_group_ids,
            active_only
        )

        if not groups:
            return []

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        if len(groups) != len(sequencing_group_ids):
            missing_ids = set(sequencing_group_ids) - set(sg.id for sg in groups)

            raise NotFoundError(
                f'Missing sequencing groups with IDs: {", ".join(map(sequencing_group_id_format, missing_ids))}'
            )

        return groups

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, list[SequencingGroupInternal]]:
        """
        Get sequencing groups by analysis IDs
        """
        if not analysis_ids:
            return {}

        projects, groups = await self.seqgt.get_sequencing_groups_by_analysis_ids(
            analysis_ids
        )

        if not groups:
            return groups

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        return groups

    async def query(
        self,
        filter_: SequencingGroupFilter,
    ) -> list[SequencingGroupInternal]:
        """
        Query sequencing groups
        """
        projects, sequencing_groups = await self.seqgt.query(filter_)
        if not sequencing_groups:
            return []

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        return sequencing_groups

    async def get_samples_create_date_from_sgs(
        self, sequencing_group_ids: list[int]
    ) -> dict[SequencingGroupInternalId, date]:
        """
        Get a map of {internal_sg_id: sample_date_created}
        for a list of sequencing_groups
        """
        return await self.seqgt.get_samples_create_date_from_sgs(sequencing_group_ids)

    async def get_all_sequencing_group_ids_by_sample_ids_by_type(
        self,
    ) -> dict[int, dict[str, list[int]]]:
        """
        Get all sequencing group IDs by sample IDs by type
        """
        return await self.seqgt.get_all_sequencing_group_ids_by_sample_ids_by_type()

    async def get_type_numbers_for_project(self, project: ProjectId) -> dict[str, int]:
        """Get sequencing type numbers (of groups) for a project"""
        return await self.seqgt.get_type_numbers_for_project(project)

    # region CREATE / MUTATE

    async def recreate_sequencing_group_with_new_assays(
        self,
        sequencing_group_id: int,
        assays: list[int],
        meta: dict,
        open_transaction=True,
    ) -> int:
        """
        Change the list of assays in a sequence group:
            - this first archives the existing group,
            - and returns a new sequence group.
        """
        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )

        seqgroup = await self.get_sequencing_group_by_id(sequencing_group_id)
        async with with_function():
            await self.archive_sequencing_group(seqgroup.id)

            return await self.seqgt.create_sequencing_group(
                sample_id=seqgroup.sample_id,
                type_=seqgroup.type,
                technology=seqgroup.technology,
                platform=seqgroup.platform,
                meta={**seqgroup.meta, **meta},
                assay_ids=assays,
                open_transaction=False,
            )

    async def archive_sequencing_group(self, sequencing_group_id: int):
        """
        Archive a single sequencing group,
        see `archive_sequencing_groups` for more details

        """
        return await self.archive_sequencing_groups([sequencing_group_id])

    async def archive_sequencing_groups(self, sequencing_group_ids: list[int]):
        """
        Archive multiple sequencing groups. Generally sequencing groups are archived
        via the upsert_sample method when assays are updated. There are some
        circumstances however where it is necessary to directly archive sequencing
        groups. For example we may be provided with a new set of assays with new sample
        ids, in this case the old sequencing groups will not be automatically archived.

        This method should be used with care as it may be necessary to also deactivate
        analyses and/or samples manually at the same time.
        """

        projects, _groups = await self.seqgt.get_sequencing_groups_by_ids(
            sequencing_group_ids
        )
        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=FullWriteAccessRoles
        )

        return await self.seqgt.archive_sequencing_groups(sequencing_group_ids)

    async def upsert_sequencing_groups(
        self, sequencing_groups: list[SequencingGroupUpsertInternal]
    ):
        """Upsert a list of sequence groups"""
        if not isinstance(sequencing_groups, list):
            raise ValueError('Sequencing groups is not a list')
        # first determine if any groups have different sequences
        assay_layer = AssayLayer(self.connection)
        assays = []
        for sg in sequencing_groups:
            for assay in sg.assays or []:
                assay.sample_id = sg.sample_id
                assays.append(assay)
        if assays:
            if not all(a.sample_id for a in assays):
                raise ValueError(
                    'Upserting sequencing-groups with assays requires a sample_id to be set for every sequencing-group'
                )

            await assay_layer.upsert_assays(assays)

        to_insert = [sg for sg in sequencing_groups if not sg.id]
        to_update = []
        to_replace: list[SequencingGroupUpsertInternal] = []

        existing_sgs = [sg for sg in sequencing_groups if sg.id]
        if existing_sgs:
            seq_group_ids = [sg.id for sg in existing_sgs if sg.id]
            sequence_to_group = await self.seqgt.get_assay_ids_by_sequencing_group_ids(
                seq_group_ids
            )

            for sg in existing_sgs:
                if not sg.assays:
                    # treat it as an update
                    to_update.append(sg)
                    continue

                # if we need to insert any assays, then the group will have to change
                if any(not assay.id for assay in sg.assays):
                    to_replace.append(sg)
                    continue

                existing_sequences = set(sequence_to_group.get(int(sg.id), []))
                new_assay_ids = set(sq.id for sq in sg.assays)
                if new_assay_ids == existing_sequences:
                    to_update.append(sg)
                else:
                    to_replace.append(sg)

        # You can't write to the same connections multiple times in parallel,
        # but we're inside a transaction, so it's not actually committing anything
        # so should be quick to "write" in serial
        for sg in to_insert:
            assay_ids = [a.id for a in sg.assays] if sg.assays else []
            sg.id = await self.seqgt.create_sequencing_group(
                sample_id=sg.sample_id,
                type_=sg.type,
                technology=sg.technology,
                platform=sg.platform,
                meta=sg.meta,
                assay_ids=assay_ids,
            )

        for sg in to_update:
            await self.seqgt.update_sequencing_group(
                int(sg.id), meta=sg.meta, platform=sg.platform
            )

        for sg in to_replace:
            await self.recreate_sequencing_group_with_new_assays(
                sequencing_group_id=int(sg.id),
                assays=[s.id for s in sg.assays],
                meta=sg.meta,
            )

        return sequencing_groups

    # endregion
