# pylint: disable=too-many-arguments
from typing import Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils import get_project_readonly_connection
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
from db.python.tables.project import ProjectPermissionsTable
from models.enums import SequenceTechnology
from models.models.sample import (
    sample_id_format,
    sample_id_transform_to_raw,
    sample_id_transform_to_raw_list,
)

# from models.models.sequence import SampleSequencing


router = APIRouter(prefix='/sequence', tags=['sequence'])


class Sequence(BaseModel):
    """Model for creating new sequence"""

    external_ids: dict[str, str]
    status: SequenceStatus
    meta: Dict
    type: SequenceType
    technology: SequenceTechnology


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
        external_ids=sequence.external_ids,
        sample_id=sample_id_transform_to_raw(sequence.sample_id),
        sequence_type=sequence.type,
        technology=sequence.technology,
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
        sequence_id,
        external_ids=sequence.external_ids,
        sequence_type=sequence.type,
        status=sequence.status,
        meta=sequence.meta,
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


@router.get(
    '/{project}/external_id/{external_id}', operation_id='getSequenceByExternalId'
)
async def get_sequence_by_external_id(
    external_id: str, connection=get_project_readonly_connection
):
    """Get a sequence by ONE of its external identifiers"""
    sequence_layer = SampleSequenceLayer(connection)
    resp = await sequence_layer.get_sequence_by_external_id(
        external_sequence_id=external_id
    )
    resp.sample_id = sample_id_format(resp.sample_id)  # type: ignore[arg-type]
    return resp


@router.post('/criteria', operation_id='getSequencesByCriteria')
async def get_sequences_by_criteria(
    sample_ids: List[str] = None,
    sequence_ids: List[int] = None,
    external_sequence_ids: List[str] = None,
    seq_meta: Dict = None,
    sample_meta: Dict = None,
    projects: List[str] = None,
    active: bool = True,
    types: List[str] = None,
    statuses: List[str] = None,
    technologies: List[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """Get sequences by some criteria"""
    sequence_layer = SampleSequenceLayer(connection)
    pt = ProjectPermissionsTable(connection.connection)

    pids: Optional[List[int]] = None
    if projects:
        pids = await pt.get_project_ids_from_names_and_user(
            connection.author, projects, readonly=True
        )

    unwrapped_sample_ids: Optional[List[int]] = None
    if sample_ids:
        unwrapped_sample_ids = sample_id_transform_to_raw_list(sample_ids)

    result = await sequence_layer.get_sequences_by(
        sample_ids=unwrapped_sample_ids,
        sequence_ids=sequence_ids,
        external_sequence_ids=external_sequence_ids,
        seq_meta=seq_meta,
        sample_meta=sample_meta,
        project_ids=pids,
        active=active,
        types=[SequenceType(s) for s in types] if types else None,
        statuses=[SequenceStatus(s) for s in statuses] if statuses else None,
        technologies=[SequenceTechnology(s) for s in technologies]
        if technologies
        else None,
    )

    for sequence in result:
        sequence.sample_id = sample_id_format(sequence.sample_id)

    return result


@router.post(
    '/',
    # response_model=List[SampleSequencing],
    operation_id='getSequencesBySampleIds',
)
async def get_sequences_by_internal_sample_ids(
    sample_ids: List[str],
    connection: Connection = get_projectless_db_connection,
):
    """Get a list of sequence objects by their internal CPG sample IDs"""
    sequence_layer = SampleSequenceLayer(connection)
    unwrapped_sample_ids: List[int] = sample_id_transform_to_raw_list(sample_ids)
    sequences = await sequence_layer.get_sequences_for_sample_ids(unwrapped_sample_ids)

    for seq in sequences:
        seq.sample_id = sample_id_format(int(seq.sample_id))

    return sequences


@router.get(
    '/ids-for-sample',
    operation_id='getSequencesForSampleIdByType',
)
async def get_sequence_ids_for_sample_id(
    sample_id: str,
    connection: Connection = get_projectless_db_connection,
) -> Dict[str, list[int]]:
    """Get all sequences for internal Sample ID"""
    sequence_layer = SampleSequenceLayer(connection)
    sample_id_raw = sample_id_transform_to_raw(sample_id)
    sequence_ids_map = await sequence_layer.get_sequence_ids_for_sample_id(
        sample_id_raw
    )
    return sequence_ids_map


@router.post(
    '/ids-for-samples',
    operation_id='getSequenceIdsForSampleIdsByType',
)
async def get_sequence_ids_for_sample_ids_by_type_by_type(
    sample_ids: List[str],
    connection: Connection = get_projectless_db_connection,
) -> Dict[str, Dict[SequenceType, list[int]]]:
    """Get all sequences by internal Sample IDs list"""
    sequence_layer = SampleSequenceLayer(connection)
    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids)
    sequence_id_map = await sequence_layer.get_sequence_ids_for_sample_ids_by_type(
        sample_ids_raw
    )
    return {sample_id_format(k): v for k, v in sequence_id_map.items()}
