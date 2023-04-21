from datetime import date

from db.python.connect import Connection, NotFoundError
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer
from db.python.tables.assay import AssayTable, NoOpAenter
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupTable
from db.python.utils import ProjectId
from models.models.sequencing_group import (
    SequencingGroupUpsertInternal,
    SequencingGroupInternal,
)
from models.utils.sequencing_group_id_format import sequencing_group_id_format


class SequencingGroupLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqgt: SequencingGroupTable = SequencingGroupTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    async def get_sequencing_group_by_id(
        self, sequencing_group_id: int, check_project_id: bool = True
    ) -> SequencingGroupInternal:
        """
        Get sequence group by internal ID
        """
        groups = await self.get_sequencing_groups_by_ids(
            [sequencing_group_id], check_project_ids=check_project_id
        )

        return groups[0]

    async def get_sequencing_groups_by_ids(
        self, sequencing_group_ids: list[int], check_project_ids: bool = True
    ) -> list[SequencingGroupInternal]:
        """
        Get sequence groups by internal IDs
        """
        if not sequencing_group_ids:
            return []

        projects, groups = await self.seqgt.get_sequencing_groups_by_ids(
            sequencing_group_ids
        )

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        if len(groups) != len(sequencing_group_ids):
            missing_ids = set(sequencing_group_ids) - set(sg.id for sg in groups)

            raise NotFoundError(
                f'Missing sequence groups with IDs: {", ".join(map(sequencing_group_id_format, missing_ids))}'
            )

        return groups

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int], check_project_ids: bool = True
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

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return groups

    async def query(
        self,
        project_ids: list[ProjectId] = None,
        sample_ids: list[int] = None,
        sequencing_group_ids: list[int] = None,
        types: list[str] = None,
        technologies: list[str] = None,
        platforms: list[str] = None,
        active_only: bool = True,
        check_project_ids: bool = True,
    ) -> list[SequencingGroupInternal]:
        """
        Query sequencing groups
        """
        projects, sequencing_groups = await self.seqgt.query(
            project_ids=project_ids,
            sample_ids=sample_ids,
            sequencing_group_ids=sequencing_group_ids,
            types=types,
            technologies=technologies,
            platforms=platforms,
            active_only=active_only,
        )
        if not sequencing_groups:
            return []

        if check_project_ids and not project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return sequencing_groups

    async def get_sequencing_groups_create_date(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sequencing_group_ids) == 0:
            return {}

        return await self.seqgt.get_sequencing_groups_create_date(sequencing_group_ids)

    async def get_all_sequencing_group_ids_by_sample_ids_by_type(
        self,
    ) -> dict[int, dict[str, list[int]]]:
        """
        Get all sequencing group IDs by sample IDs by type
        """
        return await self.seqgt.get_all_sequencing_group_ids_by_sample_ids_by_type()

    async def get_participant_ids_sequencing_group_ids_for_sequencing_type(
        self, sequencing_type: str, check_project_ids: bool = True
    ) -> dict[int, list[int]]:
        """
        Get list of partiicpant IDs for a specific sequence type,
        useful for synchronisation seqr projects
        """
        (
            projects,
            pids,
        ) = await self.seqgt.get_participant_ids_and_sequence_group_ids_for_sequencing_type(
            sequencing_type
        )
        if not pids:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return pids

    async def get_type_numbers_for_project(self, project: ProjectId) -> dict[str, int]:
        """Get sequencing type numbers (of groups) for a project"""
        return await self.seqgt.get_type_numbers_for_project(project)

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
        projects, assays = await slayer.get_assays_by(assay_ids=assay_ids)

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
            sequence_ids=assay_ids,
            meta=meta,
        )
        return SequencingGroupInternal(
            id=sequencing_group_id,
            type=next(iter(sequencing_types)),
            technology=next(iter(sequencing_technologies)),
            platform=next(iter(sequencing_platforms)),
            sequence_ids=assay_ids,
            sample_id=next(iter(sample_ids)),
            meta=meta,
            assays=assays,
        )

    async def modify_sequences_in_group(
        self,
        sequencing_group_id: int,
        sequences: list[int],
        meta: dict,
        open_transaction=True,
    ):
        """
        Change the list of sequences in a sequence group, this first
        archives the existing group, and returns a new sequence group.
        """
        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )

        seqgroup = await self.get_sequencing_group_by_id(sequencing_group_id)
        async with with_function:
            await self.archive_sequencing_group(seqgroup.id)

            await self.seqgt.create_sequencing_group(
                sample_id=seqgroup.sample_id,
                type_=seqgroup.type,
                technology=seqgroup.technology,
                platform=seqgroup.platform,
                meta={**seqgroup.meta, **meta},
                sequence_ids=sequences,
                author=self.author,
                open_transaction=False,
            )

    async def archive_sequencing_group(self, sequencing_group_id: int):
        """
        Archive sequence group, should you be able to do this?
        What are the consequences:
        - should all relevant single-sample analysis entries be archived
        - why are they being archived?
        """
        return await self.archive_sequencing_group(sequencing_group_id)

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

        await slayer.upsert_assays(assays, open_transaction=False)

        to_insert = [sg for sg in sequencing_groups if not sg.id]
        to_update = []
        to_replace: list[SequencingGroupUpsertInternal] = []

        sequencing_groups_that_exist = [sg for sg in sequencing_groups if sg.id]
        if sequencing_groups_that_exist:
            seq_group_ids = [sg.id for sg in sequencing_groups_that_exist if sg.id]
            # TODO: Fix the cast from sequencing_group_id to integers correctly
            seq_group_ids = list(map(int, seq_group_ids))
            sequence_to_group = await self.seqgt.get_assay_ids_by_sequencing_group_ids(
                seq_group_ids
            )

            for sg in sequencing_groups_that_exist:
                # if we need to insert any sequences, then the group will have to change
                if any(not sq.id for sq in sg.assays):
                    to_replace.append(sg)
                    continue

                existing_sequences = set(sequence_to_group.get(int(sg.id), []))
                new_sequences = set(sq.id for sq in sg.assays)
                if new_sequences == existing_sequences:
                    to_update.append(sg)
                else:
                    to_replace.append(sg)

        # You can't write to the same connections multiple times in parallel,
        # but we're inside a transaction so it's not actually committing anything
        # so should be quick to "write" in serial
        for sg in to_insert:
            assay_ids = [a.id for a in sg.assays]
            sg.id = await self.seqgt.create_sequencing_group(
                sample_id=sg.sample_id,
                type_=sg.type,
                technology=sg.technology,
                platform=sg.platform,
                meta=sg.meta,
                sequence_ids=assay_ids,
                open_transaction=False,
            )

        for sg in to_update:
            await self.seqgt.update_sequencing_group(int(sg.id), sg.meta, sg.platform)

        for sg in to_replace:
            await self.modify_sequences_in_group(
                sequencing_group_id=int(sg.id),
                sequences=[s.id for s in sg.assays],
                open_transaction=False,
                meta=sg.meta,
            )

        return sequencing_groups

    # endregion
