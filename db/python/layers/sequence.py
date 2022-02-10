import asyncio

from typing import Dict, List, Optional, Any

from pydantic import BaseModel

from db.python.layers.base import BaseLayer, Connection
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable
from models.enums import SequenceStatus, SequenceType
from models.models.sample import sample_id_transform_to_raw
from models.models.sequence import SampleSequencing


class SequenceUpdateModel(BaseModel):
    """Update analysis model"""

    sample_id: Optional[str] = None
    status: Optional[SequenceStatus] = None
    meta: Optional[Dict] = None
    type: Optional[SequenceType] = None


class SequenceUpsert(SequenceUpdateModel):
    """Update model for Sequence with internal id"""

    id: Optional[int]


class SampleSequenceLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqt: SampleSequencingTable = SampleSequencingTable(connection)

    # GET
    async def get_sequence_by_id(
        self, sequence_id: int, check_project_id=False
    ) -> SampleSequencing:
        """Get sequence by sequence ID"""
        project, sequence = await self.seqt.get_sequence_by_id(sequence_id)

        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return sequence

    async def get_latest_sequence_id_for_sample_id(
        self, sample_id: int, check_project_id=True
    ) -> int:
        """Get latest added sequence ID from internal sample_id"""
        project, sequence_id = await self.seqt.get_latest_sequence_id_for_sample_id(
            sample_id
        )

        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return sequence_id

    async def get_sequences_for_sample_ids(
        self,
        sample_ids: List[int],
        get_latest_sequence_only=True,
        check_project_ids=True,
    ) -> List[SampleSequencing]:
        """
        Get the latest sequence objects for a list of internal sample IDs
        """
        projects, sequences = await self.seqt.get_latest_sequence_for_sample_ids(
            sample_ids, get_latest_sequence_only=get_latest_sequence_only
        )

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return sequences

    async def get_latest_sequence_ids_for_sample_ids(
        self, sample_ids: List[int], check_project_ids=True
    ) -> Dict[int, int]:
        """
        Get the IDs of the latest sequence for a sample, keyed by the internal sample ID
        """
        (
            project_ids,
            sample_sequence_map,
        ) = await self.seqt.get_latest_sequence_ids_for_sample_ids(
            sample_ids=sample_ids
        )

        if not sample_sequence_map:
            return sample_sequence_map

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return sample_sequence_map

    async def get_latest_sequence_id_for_external_sample_id(
        self, project: ProjectId, external_sample_id
    ):
        """Get latest added sequence ID from external sample_id"""
        return await self.get_latest_sequence_id_for_external_sample_id(
            project=project, external_sample_id=external_sample_id
        )

    # INSERTS

    async def insert_many_sequencing(
        self, sequencing: List[SampleSequencing], author=None, check_project_ids=True
    ) -> None:
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
        sample_id,
        sequence_type: SequenceType,
        status: SequenceStatus,
        sequence_meta: Dict[str, Any] = None,
        author=None,
        check_project_id=True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """
        if check_project_id:
            st = SampleTable(self.connection)
            project_ids = await st.get_project_ids_for_sample_ids([sample_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.insert_sequencing(
            sample_id=sample_id,
            sequence_type=sequence_type,
            status=status,
            sequence_meta=sequence_meta,
            author=author,
        )

    # UPDATES

    async def update_sequence(
        self,
        sequence_id,
        status: Optional[SequenceStatus] = None,
        meta: Optional[Dict] = None,
        author=None,
        check_project_id=True,
    ):
        """Update a sequence"""
        if check_project_id:
            project_ids = await self.seqt.get_projects_by_sequence_ids([sequence_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.seqt.update_sequence(
            sequence_id=sequence_id,
            status=status,
            meta=meta,
            author=author,
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

    async def update_sequencing_status_from_internal_sample_id(
        self, sample_id: int, status: SequenceStatus
    ):
        """Update the sequencing status from the internal sample id"""
        # check project ID in first one
        seq_id = self.get_latest_sequence_id_for_sample_id(sample_id)
        return self.update_status(seq_id, status, check_project_id=False)

    async def update_sequencing_status_from_external_sample_id(
        self, project: ProjectId, external_sample_id: str, status: SequenceStatus
    ):
        """
        Update the sequencing status from the external sample id,
        by first looking up the internal sample id.
        """
        # project ID check done here
        seq_id = await self.get_latest_sequence_id_for_external_sample_id(
            project=project, external_sample_id=external_sample_id
        )
        return await self.update_status(seq_id, status, check_project_id=False)

    # UPSERT
    async def upsert_sequence(self, sid: str, sequence: SequenceUpsert):
        """Upsert a single sequence to the given sample_id (sid)"""
        sequence.sample_id = sid
        if not sequence.id:
            return await self.insert_sequencing(
                sample_id=sample_id_transform_to_raw(sequence.sample_id),
                sequence_type=sequence.type,
                sequence_meta=sequence.meta,
                status=sequence.status,
            )

        # Otherwise update
        await self.update_sequence(
            sequence.id, status=sequence.status, meta=sequence.meta
        )
        return sequence.id

    async def upsert_sequences(self, sid: str, sequences: List[SequenceUpsert]):
        """Upsert multiple sequences to the given sample (sid)"""
        upserts = [self.upsert_sequence(sid, s) for s in sequences]
        return await asyncio.gather(*upserts)
