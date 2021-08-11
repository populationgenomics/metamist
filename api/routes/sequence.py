from typing import Optional, Dict, List

from fastapi import APIRouter, Query
from pydantic import BaseModel

from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.tables.sequencing import (
    SequenceType,
    SequenceStatus,
    SampleSequencingTable,
)


from api.utils.db import get_db_connection, Connection

router = APIRouter(prefix='/sequence', tags=['sequence'])


class NewSequence(BaseModel):
    """Model for creating new sequence"""

    sample_id: str
    status: SequenceStatus
    meta: Dict
    type: SequenceType


class SequenceUpdateModel(BaseModel):
    """Update analysis model"""

    status: Optional[SequenceStatus] = None
    meta: Optional[Dict] = None


@router.put('/', operation_id='createNewSequence')
async def create_sequence(
    sequence: NewSequence, connection: Connection = get_db_connection
):
    """Create new sequence, attached to a sample"""
    seq_table = SampleSequencingTable(connection)
    return await seq_table.insert_sequencing(
        sample_id=sample_id_transform_to_raw(sequence.sample_id),
        sequence_type=sequence.type,
        sequence_meta=sequence.meta,
        status=sequence.status,
    )


@router.patch('/{sequence_id}', operation_id='updateSequence')
async def update_sequence(
    sequence_id: int,
    sequence: SequenceUpdateModel,
    connection: Connection = get_db_connection,
):
    """Get sample by external ID"""
    sequence_layer = SampleSequencingTable(connection)
    _ = await sequence_layer.update_sequence(
        sequence_id, status=sequence.status, meta=sequence.meta
    )

    return {'success': True}


@router.get('/{sequence_id}/details', operation_id='getSequenceByID')
async def get_sequence(sequence_id: int, connection: Connection = get_db_connection):
    """Get sequence by sequence ID"""
    sequence_layer = SampleSequencingTable(connection)
    resp = await sequence_layer.get_sequence_by_id(sequence_id)
    resp.sample_id = sample_id_format(resp.sample_id)
    return resp


@router.get('/', operation_id='getSequencesByIds')
async def get_sequences_by_internal_sample_ids(
    sample_ids: List[str] = Query(None), connection: Connection = get_db_connection
):
    """Get a list of sequence objects by their internal CPG sample IDs"""
    sequence_layer = SampleSequencingTable(connection)
    unwrapped_sample_ids: List[int] = sample_id_transform_to_raw(sample_ids)
    sequences = await sequence_layer.get_sequences_by_sample_ids(unwrapped_sample_ids)

    for seq in sequences:
        seq.sample_id = sample_id_format(seq.sample_id)

    return sequences


@router.get(
    '/latest-from-sample-id/{sample_id}', operation_id='getSequenceIdFromSampleId'
)
async def get_sequence_id_from_sample_id(
    sample_id: str, connection: Connection = get_db_connection
) -> int:
    """Get sample by internal ID"""
    sequence_layer = SampleSequencingTable(connection)
    sample_id_raw = sample_id_transform_to_raw(sample_id)
    sequence_id = await sequence_layer.get_latest_sequence_id_by_sample_id(
        sample_id_raw
    )

    return sequence_id


@router.post('/latest-from-sample-ids', operation_id='getSequenceIdsFromSampleIds')
async def get_sequence_ids_from_sample_ids(
    sample_ids: List[str], connection: Connection = get_db_connection
) -> Dict[str, int]:
    """Get sequence ids from internal sample ids"""
    sequence_layer = SampleSequencingTable(connection)
    sample_ids_raw = sample_id_transform_to_raw(sample_ids)
    sequence_id_map = await sequence_layer.get_latest_sequence_ids_by_sample_ids(
        sample_ids_raw
    )
    return {sample_id_format(k): v for k, v in sequence_id_map.items()}
