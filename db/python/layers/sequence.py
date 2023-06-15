# pylint: disable=too-many-arguments
from typing import Dict, List, Optional, Any

from pydantic import BaseModel

from db.python.layers.base import BaseLayer, Connection
from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable
from models.enums import SequenceStatus, SequenceType, SequenceTechnology
from models.models.sequence import SampleSequencing


class SequenceUpdateModel(BaseModel):
    """Update analysis model"""

    external_ids: dict[str, str] | None = None
    sample_id: int | None = None
    status: SequenceStatus | None = None
    technology: SequenceTechnology | None = None
    meta: Dict | None = None
    type: SequenceType | None = None


class SequenceUpsert(SequenceUpdateModel):
    """Update model for Sequence with internal id"""

    id: Optional[int]


class SampleSequenceLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqt: SampleSequencingTable = SampleSequencingTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    # GET
    async def get_sequence_by_id(
        self, sequence_id: int, check_project_id=True
    ) -> SampleSequencing:
        """Get sequence by sequence ID"""
        project, sequence = await self.seqt.get_sequence_by_id(sequence_id)

        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return sequence

    async def get_sequence_by_external_id(
        self, external_sequence_id: str, project: int = None
    ):
        """
        Get sequence from a project ID, you must specify explicitly, or have a
        project specified on the connection for this method to work.
        """

        sequence = await self.seqt.get_sequence_by_external_id(
            external_sequence_id, project=project
        )
        return sequence

    async def get_sequence_ids_for_sample_id(
        self, sample_id: int, check_project_id=True
    ) -> Dict[str, list[int]]:
        """Get all sequence IDs for a sample id, returned as a map with key being sequence type"""
        (
            projects,
            sequence_id_map,
        ) = await self.seqt.get_sequence_ids_for_sample_id(sample_id)

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids=projects, readonly=True
            )

        return sequence_id_map

    async def get_sequences_for_sample_ids(
        self,
        sample_ids: List[int],
        check_project_ids=True,
    ) -> List[SampleSequencing]:
        """
        Get ALL active sequence objects for a list of internal sample IDs
        """
        projects, sequences = await self.seqt.get_sequences_by(sample_ids=sample_ids)

        if (
            check_project_ids and sequences
        ):  # Only do this check if a sample actually has associated sequences
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return sequences

    async def get_sequence_ids_for_sample_ids_by_type(
        self, sample_ids: List[int], check_project_ids=True
    ) -> dict[int, dict[SequenceType, list[int]]]:
        """
        Get the IDs of all sequences for a sample, keyed by the internal sample ID,
        then by the sequence type
        """
        (
            project_ids,
            sample_sequence_map,
        ) = await self.seqt.get_sequence_ids_for_sample_ids_by_type(
            sample_ids=sample_ids
        )

        if not sample_sequence_map:
            return sample_sequence_map

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return sample_sequence_map

    async def get_sequences_by(
        self,
        sample_ids: List[int] = None,
        sequence_ids: List[int] = None,
        external_sequence_ids: list[str] = None,
        seq_meta: Dict[str, Any] = None,
        sample_meta: Dict[str, Any] = None,
        project_ids=None,
        types: List[SequenceType] = None,
        statuses: List[SequenceStatus] = None,
        technologies: list[SequenceTechnology] = None,
        active=True,
    ):
        """Get sequences by some criteria"""
        if not sample_ids and not sequence_ids and not project_ids:
            raise ValueError(
                'Must specify one of "project_ids", "sample_ids" or "sequence_ids"'
            )

        projs, seqs = await self.seqt.get_sequences_by(
            sample_ids=sample_ids,
            seq_meta=seq_meta,
            sample_meta=sample_meta,
            sequence_ids=sequence_ids,
            external_sequence_ids=external_sequence_ids,
            project_ids=project_ids,
            active=active,
            types=types,
            statuses=statuses,
            technologies=technologies,
        )

        if not project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projs, readonly=True
            )

        return seqs

    async def get_pids_sids_for_sequence_type(
        self, sequence_type: SequenceType, check_project_ids: bool = True
    ) -> dict[int, list[int]]:
        """
        Get list of partiicpant IDs for a specific sequence type,
        useful for synchronisation seqr projects
        """
        (
            projects,
            pids,
        ) = await self.seqt.get_participant_ids_and_sample_ids_for_sequence_type(
            sequence_type
        )
        if not pids:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return pids

    # region INSERTS

    async def insert_many_sequencing(
        self, sequencing: List[SampleSequencing], author=None, check_project_ids=True
    ) -> list[int]:
        """Insert many sequencing, returning no IDs"""
        if check_project_ids:
            sample_ids = set(int(s.sample_id) for s in sequencing)
            st = SampleTable(self.connection)
            project_ids = await st.get_project_ids_for_sample_ids(list(sample_ids))
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.insert_many_sequencing(
            sequencing=sequencing, author=author
        )

    async def insert_sequencing(
        self,
        sample_id: int,
        external_ids: Optional[dict[str, str]],
        sequence_type: SequenceType,
        status: SequenceStatus,
        technology: SequenceTechnology | None,
        sequence_meta: Dict[str, Any] = None,
        author=None,
        check_project_id=True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """
        st = SampleTable(self.connection)
        project_ids = await st.get_project_ids_for_sample_ids([sample_id])
        project_id = next(iter(project_ids))

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.insert_sequencing(
            sample_id=sample_id,
            external_ids=external_ids,
            sequence_type=sequence_type,
            technology=technology,
            status=status,
            sequence_meta=sequence_meta,
            author=author,
            project=project_id,
        )

    # endregion INSERTS

    # region UPDATES

    async def update_sequence(
        self,
        sequence_id: int,
        external_ids: dict[str, str] | None = None,
        status: SequenceStatus | None = None,
        technology: SequenceTechnology | None = None,
        sequence_type: SequenceType | None = None,
        meta: dict | None = None,
        author: str | None = None,
        check_project_id=True,
    ):
        """Update a sequence"""
        project_ids = await self.seqt.get_projects_by_sequence_ids([sequence_id])
        project_id = next(iter(project_ids))

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.update_sequence(
            sequencing_id=sequence_id,
            external_ids=external_ids,
            status=status,
            meta=meta,
            technology=technology,
            sequence_type=sequence_type,
            author=author,
            project=project_id,
        )

    async def update_status(
        self, sequencing_id, status: SequenceStatus, author=None, check_project_id=True
    ):
        """Update status of a sequence"""
        if check_project_id:
            project_ids = await self.seqt.get_projects_by_sequence_ids([sequencing_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.update_status(
            sequencing_id=sequencing_id,
            status=status,
            author=author,
        )

    # endregion UPDATES

    # region UPSERTS

    async def upsert_sequence(self, iid: int, sequence: SequenceUpsert):
        """Upsert a single sequence to the given sample_id (sid)"""
        sequence.sample_id = iid
        if not sequence.id:
            return await self.insert_sequencing(
                sample_id=sequence.sample_id,
                sequence_type=sequence.type,
                sequence_meta=sequence.meta,
                status=sequence.status,
                technology=sequence.technology,
                external_ids=sequence.external_ids,
            )

        # Otherwise update
        await self.update_sequence(
            sequence.id, status=sequence.status, meta=sequence.meta
        )
        return sequence.id

    async def upsert_sequences(self, iid: int, sequences: List[SequenceUpsert]):
        """Upsert multiple sequences to the given sample (sid)"""
        upserts = [await self.upsert_sequence(iid, s) for s in sequences]
        return upserts

    # endregion UPSERTS
