import asyncio

from pydantic import BaseModel

from db.python.connect import Connection, NotFoundError
from db.python.layers.base import BaseLayer
from db.python.layers.sequence import SampleSequenceLayer, SequenceUpsert
from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable, NoOpAenter
from db.python.tables.sequence_group import SequenceGroupTable
from models.enums import SequenceType, SequenceTechnology


class SequenceGroupUpsert(BaseModel):
    id: str | None
    type: SequenceType
    technology: SequenceTechnology
    platform: str  # uppercase
    meta: dict[str, str]

    sequences: list[SequenceUpsert]


class SequenceGroupLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqgt: SequenceGroupTable = SequenceGroupTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    async def get_sequence_group_by_id(
        self, sequence_group_id: int, check_project_id: bool = True
    ) -> dict:
        """
        Get sequence group by internal ID
        """
        groups = await self.get_sequence_groups_by_ids(
            [sequence_group_id], check_project_ids=check_project_id
        )

        return groups[0]

    async def get_sequence_groups_by_ids(
        self, sequence_group_ids: list[int], check_project_ids: bool = True
    ):
        """
        Get sequence groups by internal IDs
        """
        if not sequence_group_ids:
            return []

        projects, groups = await self.seqgt.get_sequence_groups_by_ids(
            sequence_group_ids
        )

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(self.author, projects)

        if len(groups) != len(sequence_group_ids):
            missing_ids = set(sequence_group_ids) - set(sg['id'] for sg in groups)

            raise NotFoundError(
                f'Missing sequence groups with IDs: {", ".join(map(str, missing_ids))}'
            )

        return groups

    # region CREATE / MUTATE

    async def create_sequence_group_from_sequences(self, sequence_ids: list[int], meta: dict):
        """
        Create a sequence group from a list of sequences,
        return an exception if they're not of the same type
        """
        if not sequence_ids:
            raise ValueError('Requires sequences to group sequence group')

        # let's check the sequences first
        slayer = SampleSequencingTable(self.connection)
        projects, sequences = await slayer.get_sequences_by(sequence_ids=sequence_ids)

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

        seq0 = sequences[0]
        sequence = await self.seqgt.create_sequence_group(
            sample_id=seq0.sample_id,
            type_=seq0.type,
            technology=seq0.technology,
            platform=seq0.platform,
            sequence_ids=sequence_ids,
            meta=meta,
        )
        return sequence

    async def modify_sequences_in_group(
        self, sequence_group_id: int, sequences: list[int], meta: dict, open_transaction=True
    ):
        """
        Change the list of sequences in a sequence group, this first
        archives the existing group, and returns a new sequence group.
        """
        with_function = self.connection.connection.transaction if open_transaction else NoOpAenter

        seqgroup = await self.get_sequence_group_by_id(sequence_group_id)
        async with with_function:
            await self.archive_sequence_group(seqgroup['id'])

            await self.seqgt.create_sequence_group(
                sample_id=seqgroup['sample_id'],
                type_=seqgroup['type_'],
                technology=seqgroup['technology'],
                platform=seqgroup['platform'],
                meta={**seqgroup['meta'], **meta},
                sequence_ids=sequences,
                author=self.author,
                open_transaction=False,
            )

    async def archive_sequence_group(self, sequence_group_id: int):
        """
        Archive sequence group, should you be able to do this?
        What are the consequences:
        - should all relevant single-sample analysis entries be archived
        - why are they being archived?
        """
        return await self.archive_sequence_group(sequence_group_id)

    async def upsert_sequence_groups(self, sample_id: int, sequence_groups: list[SequenceGroupUpsert]):
        # first determine if any groups have different sequences
        slayer = SampleSequenceLayer(self.connection)
        await asyncio.gather(*[slayer.upsert_sequences(sample_id, sg.sequences) for sg in sequence_groups])

        to_insert = [sg for sg in sequence_groups if not sg.id]
        to_update = []
        to_replace = []

        sequence_groups_that_exist = [sg for sg in sequence_groups if sg.id]
        if sequence_groups_that_exist:
            seq_group_ids = [sg.id for sg in sequence_groups_that_exist if sg.id]
            # TODO: Fix the cast from sequence_group_id to integers correctly
            seq_group_ids = list(map(int, seq_group_ids))
            sequence_to_group = await self.seqgt.get_sequence_ids_by_sequence_group_ids(seq_group_ids)

            for sg in sequence_groups_that_exist:
                # if we need to insert any sequences, then the group will have to change
                if any(not sq.id for sq in sg.sequences):
                    to_replace.append(sg)
                    continue

                existing_sequences = set(sequence_to_group.get(int(sg.id), []))
                new_sequences = set(sq.id for sq in sg.sequences)
                if new_sequences == existing_sequences:
                    to_update.append(sg)
                else:
                    to_replace.append(sg)

        promises = []

        async def insert(sg):
            sequence_ids = [s.id for s in sg.sequences]
            sg.id = await self.seqgt.create_sequence_group(
                sample_id=sample_id,
                type_=sg.type,
                technology=sg.technology,
                platform=sg.platform,
                meta=sg.meta,
                sequence_ids=sequence_ids,
            )

        promises.extend(map(insert, to_insert))

        for sg in to_update:
            promises.append(self.seqgt.update_sequence_group(int(sg.id), sg.meta, sg.platform))

        for sg in to_replace:
            promises.append(self.modify_sequences_in_group(
                sequence_group_id=int(sg.id),
                sequences=[s.id for s in sg.sequences],
                open_transaction=False,
                meta=sg.meta,
            ))

    # endregion
