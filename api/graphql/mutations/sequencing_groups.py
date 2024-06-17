from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from typing_extensions import Annotated

from api.graphql.types import CustomJSON
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw,
)

if TYPE_CHECKING:
    from ..schema import GraphQLSequencingGroup


@strawberry.type
class SequencingGroupsMutations:
    """Sequencing group mutations"""

    @strawberry.mutation
    async def get_sequencing_group(
        self,
        sequencing_group_id: str,
        info: Info,
    ) -> Annotated['GraphQLSequencingGroup', strawberry.lazy('..schema')]:
        """Gets a sequencing group by ID"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        st = SequencingGroupLayer(connection)
        sg = await st.get_sequencing_group_by_id(
            sequencing_group_id_transform_to_raw(sequencing_group_id)
        )
        #  pylint: disable=no-member
        return Annotated[
            'GraphQLSequencingGroup', strawberry.lazy('..schema')
        ].from_internal(  # type: ignore [attr-defined]
            sg
        )

    @strawberry.mutation
    async def get_all_sequencing_group_ids_by_sample_by_type(
        self,
        info: Info,
    ) -> CustomJSON:
        """Gets all sequencing group IDs by sample by type"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        st = SequencingGroupLayer(connection)
        sg = await st.get_all_sequencing_group_ids_by_sample_ids_by_type()
        return CustomJSON(
            {
                sample_id_format(sid): CustomJSON(
                    {
                        sg_type: sequencing_group_id_format_list(sg_ids)
                        for sg_type, sg_ids in sg_type_to_sg_ids.items()
                    }
                )
                for sid, sg_type_to_sg_ids in sg.items()
            }
        )

    @strawberry.mutation
    async def update_sequencing_group(
        self,
        sequencing_group_id: str,
        sequencing_group: strawberry.scalars.JSON,
        info: Info,
    ) -> bool:
        """Update the meta fields of a sequencing group"""
        # TODO: Reconfigure connection permissions as per `routes``
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
