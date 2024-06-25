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
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        return AssayUpsertType.from_upsert_internal(
            await assay_layer.upsert_assay(
                AssayUpsertInternal.from_db(strawberry.asdict(assay))
            )
        )

    @strawberry.mutation
    async def update_assay(
        self,
        assay: AssayUpsertInput,
        info: Info,
    ) -> int:
        """Update assay for ID"""
        if not assay.id:
            raise ValueError('Assay must have an ID to update')
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        await assay_layer.upsert_assay(
            AssayUpsertInternal.from_db(strawberry.asdict(assay))
        )

        return assay.id
