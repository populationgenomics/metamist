from typing import Dict, List

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils.db import (
    Connection,
    get_projectless_db_connection,
)
from db.python.layers.sequence import (
    SequenceType,
    SequenceStatus,
    SampleSequenceLayer,
    SequenceUpdateModel,
)
from models.models.sample import (
    sample_id_transform_to_raw,
    sample_id_transform_to_raw_list,
    sample_id_format,
)

router = APIRouter(prefix='/sequence', tags=['sequence'])


class Sequence(BaseModel):
    """Model for creating new sequence"""

    status: SequenceStatus
    meta: Dict
    type: SequenceType


class NewSequence(Sequence):
    """Model for creating new sequence"""

    sample_id: str


@router.put('/', operation_id='createNewSequence')
async def create_sequence(
    sequence: NewSequence, connection: Connection = get_projectless_db_connection
):
    """Create new sequence, attached to a sample"""
    seq_table = SampleSequenceLayer(connection)
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
    connection: Connection = get_projectless_db_connection,
):
    """Update sequence by ID"""
    sequence_layer = SampleSequenceLayer(connection)
    _ = await sequence_layer.update_sequence(
        sequence_id, status=sequence.status, meta=sequence.meta
    )

    return sequence_id


@router.get('/{sequence_id}/details', operation_id='getSequenceByID')
async def get_sequence(
    sequence_id: int, connection: Connection = get_projectless_db_connection
):
    """Get sequence by sequence ID"""
    sequence_layer = SampleSequenceLayer(connection)
    resp = await sequence_layer.get_sequence_by_id(sequence_id, check_project_id=True)
    resp.sample_id = sample_id_format(resp.sample_id)  # type: ignore[arg-type]
    return resp


@router.post('/', operation_id='getSequencesBySampleIds')
async def get_sequences_by_internal_sample_ids(
    sample_ids: List[str],
    get_latest_sequence_only: bool = True,
    connection: Connection = get_projectless_db_connection,
):
    """Get a list of sequence objects by their internal CPG sample IDs"""
    sequence_layer = SampleSequenceLayer(connection)
    unwrapped_sample_ids: List[int] = sample_id_transform_to_raw_list(sample_ids)
    sequences = await sequence_layer.get_sequences_for_sample_ids(
        unwrapped_sample_ids, get_latest_sequence_only=get_latest_sequence_only
    )

    for seq in sequences:
        seq.sample_id = sample_id_format(int(seq.sample_id))

    return sequences


@router.get(
    '/latest-from-sample-id/{sample_id}', operation_id='getSequenceIdFromSampleId'
)
async def get_sequence_id_from_sample_id(
    sample_id: str, connection: Connection = get_projectless_db_connection
) -> int:
    """Get sequence by internal Sample ID"""
    sequence_layer = SampleSequenceLayer(connection)
    sample_id_raw = sample_id_transform_to_raw(sample_id)
    sequence_id = await sequence_layer.get_latest_sequence_id_for_sample_id(
        sample_id_raw
    )

    return sequence_id


@router.post('/latest-ids-from-sample-ids', operation_id='getSequenceIdsFromSampleIds')
async def get_sequence_ids_from_sample_ids(
    sample_ids: List[str], connection: Connection = get_projectless_db_connection
) -> Dict[str, int]:
    """Get sequence ids from internal sample ids"""
    sequence_layer = SampleSequenceLayer(connection)
    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids)
    sequence_id_map = await sequence_layer.get_latest_sequence_ids_for_sample_ids(
        sample_ids_raw
    )
    return {sample_id_format(k): v for k, v in sequence_id_map.items()}
