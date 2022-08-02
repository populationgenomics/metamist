from typing import Dict, List, Optional, Any

from pydantic import BaseModel

from db.python.layers.base import BaseLayer, Connection
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable
from models.enums import SequenceStatus, SequenceType
from models.models.sequence import SampleSequencing


class SequenceUpdateModel(BaseModel):
    """Update analysis model"""

    sample_id: Optional[int] = None
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
        self.sampt: SampleTable = SampleTable(connection)

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

    async def get_latest_sequence_id_from_sample_id_and_type(
        self, sample_id: int, stype: SequenceType, check_project_id=True
    ) -> int:
        """Get latest added sequence ID from internal sample_id"""
        (
            project,
            sequence_id,
        ) = await self.seqt.get_latest_sequence_id_from_sample_id_and_type(
            sample_id, stype
        )

        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return sequence_id

    async def get_all_sequence_ids_for_sample_id(
        self, sample_id: int, check_project_id=True
    ) -> Dict[str, int]:
        """Get all sequence IDs for a sample id, returned as a map with key being sequence type"""
        (
            projects,
            sequence_id_map,
        ) = await self.seqt.get_all_sequence_ids_for_sample_id(sample_id)

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids=projects, readonly=True
            )

        return sequence_id_map

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

    async def get_sequence_ids_from_sample_ids(
        self, sample_ids: List[int], check_project_ids=True
    ):
        """
        Get the IDs of of all sequences for a sample, keyed by the internal sample ID
        """
        (
            project_ids,
            sample_sequence_map,
        ) = await self.seqt.get_sequence_ids_from_sample_ids(sample_ids=sample_ids)

        if not sample_sequence_map:
            return sample_sequence_map

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return sample_sequence_map

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
        sample_id: int,
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
    async def upsert_sequence(self, iid: int, sequence: SequenceUpsert):
        """Upsert a single sequence to the given sample_id (sid)"""
        sequence.sample_id = iid
        if not sequence.id:
            return await self.insert_sequencing(
                sample_id=sequence.sample_id,
                sequence_type=sequence.type,
                sequence_meta=sequence.meta,
                status=sequence.status,
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

    async def update_sequence_from_sample_and_type(
        self,
        sample_id: int,
        sequence_type: SequenceType,
        status: SequenceStatus,
        meta: dict,
    ):
        """Update a sequence from the sample_id and sequence_type"""

        # Get sequence_id from sample_id and batch_id
        sequence_id = await self.get_latest_sequence_id_from_sample_id_and_type(
            sample_id, sequence_type
        )

        _ = await self.update_sequence(sequence_id, status=status, meta=meta)

        return sequence_id

    async def upsert_sequence_from_external_id_and_type(
        self,
        external_sample_id,
        sequence_type,
        status,
        meta,
        sample_type,
    ):
        """Update a sequence from the external_id and sequence_type"""

        # Convert the external_id to an internal sample_id
        sample_ids = await self.sampt.get_sample_id_map_by_external_ids(
            [external_sample_id], project=None
        )

        internal_sample_id: int = None
        if not sample_ids:
            # If the sample doesn't exist, create it
            internal_sample_id = await self.sampt.insert_sample(
                external_id=external_sample_id,
                sample_type=sample_type,
                active=True,
                author=self.author,
                project=self.connection.project,
            )

            # Create the sequence also
            await self.seqt.insert_sequencing(
                sample_id=internal_sample_id,
                sequence_type=sequence_type,
                status=status,
                sequence_meta=meta,
                author=self.author,
            )

        if not internal_sample_id:
            internal_sample_id = sample_ids[external_sample_id]

        # Get sequence_id from sample_id and batch_id
        return await self.update_sequence_from_sample_and_type(
            internal_sample_id, sequence_type, status, meta
        )

    async def get_sequences_by(
        self,
        sample_ids: List[int] = None,
        sequence_ids: List[int] = None,
        seq_meta: Dict[str, Any] = None,
        project_ids=None,
        types: List[str] = None,
        statuses: List[str] = None,
        active=True,
        latest: bool = False,
    ):
        """Get sequences by some criteria"""
        if not sample_ids and not sequence_ids and not project_ids:
            raise ValueError(
                'Must specify one of "project_ids", "sample_ids" or "sequence_ids"'
            )

        output = await self.seqt.get_sequences_by(
            sample_ids=sample_ids,
            seq_meta=seq_meta,
            sequence_ids=sequence_ids,
            project_ids=project_ids,
            active=active,
            types=types,
            statuses=statuses,
            latest=latest,
        )

        return output
