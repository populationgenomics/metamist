from typing import Optional, Dict

from fastapi import APIRouter
from pydantic import BaseModel

from models.models.sample import sample_id_transform_to_raw
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


@router.get(
    '/latest-from-sample-id/{sample_id}', operation_id='getSequenceIdFromSampleId'
)
async def get_sequence_id_from_sample_id(
    sample_id: str, connection: Connection = get_db_connection
):
    """Get sample by external ID"""
    sequence_layer = SampleSequencingTable(connection)
    sample_id_raw = sample_id_transform_to_raw(sample_id)
    sequence_id = await sequence_layer.get_latest_sequence_id_by_sample_id(
        sample_id_raw
    )

    return sequence_id
