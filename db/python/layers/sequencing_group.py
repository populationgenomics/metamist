import asyncio
from typing import Awaitable

from db.python.connect import Connection, NotFoundError
from db.python.layers.base import BaseLayer
from db.python.layers.assay import AssayLayer
from db.python.tables.sample import SampleTable
from db.python.tables.assay import AssayTable, NoOpAenter
from db.python.tables.sequencing_group import SequencingGroupTable
from models.models.sequencing_group import SequencingGroupUpsertInternal


class SequencingGroupLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqgt: SequencingGroupTable = SequencingGroupTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    async def get_sequencing_group_by_id(
        self, sequencing_group_id: int, check_project_id: bool = True
    ) -> dict:
        """
        Get sequence group by internal ID
        """
        groups = await self.get_sequencing_groups_by_ids(
            [sequencing_group_id], check_project_ids=check_project_id
        )

        return groups[0]

    async def get_sequencing_groups_by_ids(
        self, sequencing_group_ids: list[int], check_project_ids: bool = True
    ):
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
            missing_ids = set(sequencing_group_ids) - set(sg['id'] for sg in groups)

            raise NotFoundError(
                f'Missing sequence groups with IDs: {", ".join(map(str, missing_ids))}'
            )

        return groups

    # region CREATE / MUTATE

    async def create_sequencing_group_from_sequences(
        self, sequence_ids: list[int], meta: dict
    ):
        """
        Create a sequence group from a list of sequences,
        return an exception if they're not of the same type
        """
        if not sequence_ids:
            raise ValueError('Requires sequences to group sequence group')

        # let's check the sequences first
        slayer = AssayTable(self.connection)
        projects, sequences = await slayer.get_assays_by(assay_ids=sequence_ids)

        if len(sequence_ids) != len(sequences):
            missing_seq_ids = set(sequence_ids) - set(s.id for s in sequences)
            raise NotFoundError(f'Some sequences were not found: {missing_seq_ids}')

        if not projects:
            raise ValueError('Sequences were not attached to any project')

        matching_attributes = ['sample_id', 'type', 'technology', 'platform']

        for attribute in matching_attributes:
            distinct_attributes = set(str(getattr(s, attribute)) for s in sequences)
            if len(distinct_attributes) != 1:
                raise ValueError(
                    f'Incorrect number of {attribute}\'s from provided sequences - '
                    f'expected 1, got {len(distinct_attributes)}: {distinct_attributes}'
                )

        raise ValueError(
            'Have another think, because we do not store the type of sequencing group on the assay anymore'
        )
        # seq0 = sequences[0]
        # sequence = await self.seqgt.create_sequencing_group(
        #     sample_id=int(seq0.sample_id),
        #     type_=seq0.type,
        #     technology=seq0.technology,
        #     platform=seq0.platform,
        #     sequence_ids=sequence_ids,
        #     meta=meta,
        # )
        # return sequence

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
            await self.archive_sequencing_group(seqgroup['id'])

            await self.seqgt.create_sequencing_group(
                sample_id=seqgroup['sample_id'],
                type_=seqgroup['type_'],
                technology=seqgroup['technology'],
                platform=seqgroup['platform'],
                meta={**seqgroup['meta'], **meta},
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
            sequence_to_group = (
                await self.seqgt.get_sequence_ids_by_sequencing_group_ids(seq_group_ids)
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

        promises: list[Awaitable] = []

        async def insert(sg: SequencingGroupUpsertInternal):
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

        promises.extend(map(insert, to_insert))

        for sg in to_update:
            promises.append(
                self.seqgt.update_sequencing_group(int(sg.id), sg.meta, sg.platform)
            )

        for sg in to_replace:
            promises.append(
                self.modify_sequences_in_group(
                    sequencing_group_id=int(sg.id),
                    sequences=[s.id for s in sg.assays],
                    open_transaction=False,
                    meta=sg.meta,
                )
            )

        await asyncio.gather(*promises)

        return sequencing_groups

    # endregion
