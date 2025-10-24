from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta

from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer
from db.python.tables.assay import AssayFilter, AssayTable, NoOpAenter
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from db.python.utils import NotFoundError
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
        self, sequencing_group_id: int
    ) -> SequencingGroupInternal:
        """
        Get sequencing group by internal ID
        """
        groups = await self.get_sequencing_groups_by_ids([sequencing_group_id])

        return groups[0]

    async def get_sequencing_groups_by_ids(
        self, sequencing_group_ids: list[int]
    ) -> list[SequencingGroupInternal]:
        """
        Get sequence groups by internal IDs
        """
        if not sequencing_group_ids:
            return []

        projects, groups = await self.seqgt.get_sequencing_groups_by_ids(
            sequencing_group_ids
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

    async def get_sequencing_groups_create_date(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, date]:
        """
        Get a map of {internal_sample_id: date_created} for list of sequencing_groups
        """
        if len(sequencing_group_ids) == 0:
            return {}

        return await self.seqgt.get_sequencing_groups_create_date(sequencing_group_ids)

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

    async def get_participant_ids_sequencing_group_ids_for_sequencing_type(
        self, sequencing_type: str
    ) -> dict[int, list[int]]:
        """
        Get list of partiicpant IDs for a specific sequence type,
        useful for synchronisation seqr projects
        """
        (
            projects,
            pids,
        ) = await self.seqgt.get_participant_ids_and_sequencing_group_ids_for_sequencing_type(
            sequencing_type
        )
        if not pids:
            return {}

        self.connection.check_access_to_projects_for_ids(
            projects, allowed_roles=ReadAccessRoles
        )

        return pids

    async def get_type_numbers_for_project(self, project: ProjectId) -> dict[str, int]:
        """Get sequencing type numbers (of groups) for a project"""
        return await self.seqgt.get_type_numbers_for_project(project)
    
    async def get_type_numbers_for_project_history(self, project_id: ProjectId) -> dict[ProjectId, dict[date, dict[str, int]]]:
        """Returns a record of the number of each sequencing group type for the given projects over time."""
        # Retrieve the raw data from the Sequencing Group/Sample tables.
        rows = await self.seqgt.get_type_numbers_history(project_id)

        # Organise the data by project into a dictionary.
        project_history: dict[date, dict[str, int]] = {}
        for row in rows:
            # Extract values from the table row.
            month_created = date.fromisoformat(row['date_created']).replace(day=1)
            type = row['type']
            num_sg = row['num_sg']
            
            # Organise the date's data into a dictionary, based on sample type.
            if month_created not in project_history:
                project_history[month_created] = {}

            project_history[month_created][type] = num_sg

        # We want the total number of each sg type over time, so performing this summing and
        # fill in the months between data points.
        todays_month = date.today().replace(day=1)
        iteration_month = min(project_history.keys()) # The month currently being filled in.
        type_totals: dict[str, int] = defaultdict(lambda: 0)

        # Start from the earliest recorded month and work towards the current month.
        while iteration_month <= todays_month:
            iteration_counts = project_history.get(iteration_month, {})
            # If there's any recorded Sequencing Group counts for this iteration's month, 
            # add them to the cumulative count.
            for type, count in iteration_counts.items():
                type_totals[type] += count

            iteration_counts.update(type_totals)
            project_history[iteration_month] = iteration_counts

            iteration_month += relativedelta(months=1)

        return project_history

    # region CREATE / MUTATE

    async def create_sequencing_group_from_assays(
        self, assay_ids: list[int], meta: dict
    ) -> SequencingGroupInternal:
        """
        Create a sequencing group from a list of assays,
        return an exception if they're not of the same type
        """
        if not assay_ids:
            raise ValueError('Requires assays to create SequencingGroup')

        # let's check the sequences first
        slayer = AssayTable(self.connection)
        projects, assays = await slayer.query(
            AssayFilter(id=GenericFilter(in_=assay_ids))
        )

        if len(assay_ids) != len(assays):
            missing_seq_ids = set(assay_ids) - set(s.id for s in assays)
            raise NotFoundError(f'Some assays were not found: {missing_seq_ids}')

        if not projects:
            raise ValueError('Sequences were not attached to any project')

        assay_types = set(a.type for a in assays)
        if not len(assay_types) == 1 and 'sequencing' in assay_types:
            raise ValueError(
                f'Assays must be all of type "sequencing", got: {assay_types}'
            )

        sample_ids = set(s.sample_id for s in assays)
        sequencing_types = set(s.meta.get('sequencing_type') for s in assays)
        sequencing_technologies = set(
            s.meta.get('sequencing_technology') for s in assays
        )
        sequencing_platforms = set(s.meta.get('sequencing_platform') for s in assays)

        attributes_to_check = {
            'type': sequencing_types,
            'meta.technology': sequencing_technologies,
            'meta.platform': sequencing_platforms,
            'meta.sample_id': sample_ids,
        }

        for attribute, values in attributes_to_check.items():
            if len(values) > 1:
                raise ValueError(
                    f'Cannot create sequencing group from sequences with different {attribute!r}: {values!r}'
                )
            first_value = next(iter(values))
            if first_value is None:
                raise ValueError(
                    f'Cannot create sequencing group from sequences with missing {attribute!r}'
                )

        sequencing_group_id = await self.seqgt.create_sequencing_group(
            sample_id=next(iter(sample_ids)),
            type_=next(iter(sequencing_types)),
            technology=next(iter(sequencing_technologies)),
            platform=next(iter(sequencing_platforms)),
            assay_ids=assay_ids,
            meta=meta,
        )
        return SequencingGroupInternal(
            id=sequencing_group_id,
            type=next(iter(sequencing_types)),
            technology=next(iter(sequencing_technologies)),
            platform=next(iter(sequencing_platforms)),
            sample_id=next(iter(sample_ids)),
            meta=meta,
            assays=assays,
        )

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
        slayer = AssayLayer(self.connection)
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

            await slayer.upsert_assays(assays, open_transaction=False)

        to_insert = [sg for sg in sequencing_groups if not sg.id]
        to_update = []
        to_replace: list[SequencingGroupUpsertInternal] = []

        sequencing_groups_that_exist = [sg for sg in sequencing_groups if sg.id]
        if sequencing_groups_that_exist:
            seq_group_ids = [sg.id for sg in sequencing_groups_that_exist if sg.id]
            sequence_to_group = await self.seqgt.get_assay_ids_by_sequencing_group_ids(
                seq_group_ids
            )

            for sg in sequencing_groups_that_exist:
                if not sg.assays:
                    # treat it as an update
                    to_update.append(sg)
                    continue

                # if we need to insert any assays, then the group will have to change
                if any(not sq.id for sq in sg.assays):
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
                open_transaction=False,
            )

        for sg in to_update:
            await self.seqgt.update_sequencing_group(
                int(sg.id), meta=sg.meta, platform=sg.platform
            )

        for sg in to_replace:
            await self.recreate_sequencing_group_with_new_assays(
                sequencing_group_id=int(sg.id),
                assays=[s.id for s in sg.assays],
                open_transaction=False,
                meta=sg.meta,
            )

        return sequencing_groups

    # endregion
