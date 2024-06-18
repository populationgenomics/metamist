import strawberry
from strawberry.types import Info

from api.graphql.types import AssayUpsertInput, AssayUpsertType
from db.python.layers.assay import AssayLayer
from models.models.assay import AssayUpsertInternal


@strawberry.type
class AssayMutations:
    """Assay mutations"""

    @strawberry.mutation
    async def create_assay(
        self, assay: AssayUpsertInput, info: Info
    ) -> AssayUpsertType:
        """Create new assay, attached to a sample"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        seq_table = AssayLayer(connection)
        return AssayUpsertType.from_upsert_internal(
            await seq_table.upsert_assay(
                AssayUpsertInternal.from_dict(strawberry.asdict(assay))
            )
        )

    @strawberry.mutation
    async def update_assay(
        self,
        assay: AssayUpsertInput,
        info: Info,
    ) -> int:
        """Update assay for ID"""
        # TODO: Reconfigure connection permissions as per `routes`

        if not assay.id:
            raise ValueError('Assay must have an ID to update')
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        await assay_layer.upsert_assay(
            AssayUpsertInternal.from_dict(strawberry.asdict(assay))
        )

        return assay.id
