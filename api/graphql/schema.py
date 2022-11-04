# pylint: disable=redefined-builtin
import strawberry
from strawberry.types import Info

from api.utils import get_projectless_db_connection
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer
from models.enums import SampleType
from models.models.sequence import SampleSequencing as SampleSequencingType
from models.models.sample import sample_id_transform_to_raw


async def get_context(connection=get_projectless_db_connection):
    """Connection context"""
    return {'connection': connection}


@strawberry.type
class SampleSequencing:
    """GQL Sequence Model"""

    id: int
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON


@strawberry.type
class Sample:
    """GQL Sample Model"""

    id: str
    external_id: str
    participant_id: int
    active: bool
    meta: strawberry.scalars.JSON
    type: SampleType
    author: str | None

    @strawberry.field
    async def sequences(self, info: Info, root) -> list[SampleSequencingType]:
        """Get related sequences"""
        connection = info.context['connection']
        seqlayer = SampleSequenceLayer(connection)

        return await seqlayer.get_sequences_for_sample_ids(
            [sample_id_transform_to_raw(root.id)]
        )


@strawberry.type
class Query:
    """GQL Query Model"""

    @strawberry.field
    async def sample(self, info: Info, id: str) -> Sample:
        """Get sample"""
        connection = info.context['connection']
        slayer = SampleLayer(connection)
        return await slayer.get_sample_by_id(sample_id_transform_to_raw(id))


schema = strawberry.Schema(Query)
