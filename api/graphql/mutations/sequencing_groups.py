import strawberry
from strawberry.types import Info

from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


@strawberry.type
class SequencingGroupsMutations:
    """Sequencing group mutations"""

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
