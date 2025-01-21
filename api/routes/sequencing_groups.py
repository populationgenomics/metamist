from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils.db import (
    Connection,
    get_project_db_connection,
    get_projectless_db_connection,
)
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.project import FullWriteAccessRoles, ReadAccessRoles
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (  # Sample,
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw,
)

router = APIRouter(prefix='/sequencing-group', tags=['sequencing-group'])

# region CREATES


class SequencingGroupMetaUpdateModel(BaseModel):
    """Update sequencing group model"""

    meta: dict[str, Any] | None = None


@router.get('{sequencing_group_id}', operation_id='getSequencingGroup')
async def get_sequencing_group(
    sequencing_group_id: str, connection: Connection = get_projectless_db_connection
) -> str:
    """Creates a new sample, and returns the internal sample ID"""
    st = SequencingGroupLayer(connection)
    sg = await st.get_sequencing_group_by_id(
        sequencing_group_id_transform_to_raw(sequencing_group_id)
    )
    return sg.to_external()


@router.get('/project/{project}', operation_id='getAllSequencingGroupIdsBySampleByType')
async def get_all_sequencing_group_ids_by_sample_by_type(
    connection: Connection = get_project_db_connection(ReadAccessRoles),
) -> dict[str, dict[str, list[str]]]:
    """Creates a new sample, and returns the internal sample ID"""
    st = SequencingGroupLayer(connection)
    sg = await st.get_all_sequencing_group_ids_by_sample_ids_by_type()
    return {
        sample_id_format(sid): {
            sg_type: sequencing_group_id_format_list(sg_ids)
            for sg_type, sg_ids in sg_type_to_sg_ids.items()
        }
        for sid, sg_type_to_sg_ids in sg.items()
    }


@router.patch('/project/{sequencing_group_id}', operation_id='updateSequencingGroup')
async def update_sequencing_group(
    sequencing_group_id: str,
    sequencing_group: SequencingGroupMetaUpdateModel,
    connection: Connection = get_project_db_connection(FullWriteAccessRoles),
) -> bool:
    """Update the meta fields of a sequencing group"""
    st = SequencingGroupLayer(connection)
    await st.upsert_sequencing_groups(
        [
            SequencingGroupUpsertInternal(
                id=sequencing_group_id_transform_to_raw(sequencing_group_id),
                meta=sequencing_group.meta,
            )
        ]
    )
    return True


@router.patch('/archive', operation_id='archiveSequencingGroups')
async def archive_sequencing_groups(
    sequencing_group_ids: list[str],
    connection: Connection = get_projectless_db_connection,
) -> bool:
    """Update the meta fields of a sequencing group"""
    sl = SequencingGroupLayer(connection)
    await sl.archive_sequencing_groups(
        [sequencing_group_id_transform_to_raw(sgid) for sgid in sequencing_group_ids]
    )
    return True


# endregion
