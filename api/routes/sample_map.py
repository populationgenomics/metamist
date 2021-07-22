from typing import List, Dict

from fastapi import APIRouter

from db.python.layers.sample_map import SampleMapLayer

from api.utils.db import get_author
from models.models.sample import sample_id_transform_to_raw, sample_id_format

router = APIRouter(prefix='/sample-map', tags=['sample-map'])


@router.post(
    '/', response_model=Dict[str, str], operation_id='getProjectIndependentSampleMap'
)
async def get_sample_map(
    internal_sample_ids: List[str], author: str = get_author
) -> Dict[str, str]:
    """Creates a new sample, and returns the internal sample ID"""
    sml = SampleMapLayer(author=author)
    raw_internal_ids = sample_id_transform_to_raw(internal_sample_ids)
    m = await sml.get_internal_id_map(internal_ids=raw_internal_ids)
    return {sample_id_format(k): v for k, v in m.items()}
