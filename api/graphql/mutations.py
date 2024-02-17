
import strawberry
from typing import Any

from pydantic import BaseModel

from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
    get_projectless_db_connection,
)

from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sequencing_group_id_format import (  # Sample,
    sequencing_group_id_transform_to_raw,
)


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def update_sequencing_group(
        self,
        sequencing_group_id: str,
        sequencing_group: strawberry.scalars.JSON,
        info: strawberry.types.Info,
    ) -> bool:
        """Update the meta fields of a sequencing group"""
        connection = info.context['connection']
        st = SequencingGroupLayer(connection)
        await st.upsert_sequencing_groups(
            [
                SequencingGroupUpsertInternal(
                    id=sequencing_group_id_transform_to_raw(sequencing_group_id),
                    meta=sequencing_group,
                )
            ]
        )
        return True
