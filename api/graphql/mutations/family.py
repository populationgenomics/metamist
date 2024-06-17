import strawberry
from strawberry.types import Info

from api.graphql.types import CustomJSON, FamilyUpdateInput
from db.python.layers.family import FamilyLayer


@strawberry.type
class FamilyMutations:
    """Family mutations"""

    @strawberry.mutation
    async def update_family(
        self,
        family: FamilyUpdateInput,
        info: Info,
    ) -> CustomJSON:
        """Update information for a single family"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        family_layer = FamilyLayer(connection)
        return CustomJSON(
            {
                'success': await family_layer.update_family(
                    id_=family.id,
                    external_id=family.external_id,
                    description=family.description,
                    coded_phenotype=family.coded_phenotype,
                )
            }
        )
